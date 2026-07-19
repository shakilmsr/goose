//! The SwarmOS kernel: an event-driven interpreter of the planned topology.
//!
//! Port of SwarmOS `engine/kernel.py`'s `_event_loop`/`_dispatch`. The kernel is
//! domain-agnostic: it decomposes the prompt, plans an event graph, then walks it —
//! consuming events from the [`EventBus`], counting `join_after` barriers, and
//! dispatching each triggered node to an [`AgentSpawner`] (the bridge to real Goose
//! subagent sessions). Subtask outputs travel through the [`ScratchPad`], and a
//! dependent receives only its direct prerequisites' outputs — never whole
//! transcripts — which is what keeps multi-legged runs free of token bloat.
//!
//! v1 scope (same as SwarmOS): siblings dispatch sequentially, in event order.
//! Correct independence and joins are the goal here, not wall-clock parallelism.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use crate::swarm::decompose::{sink_ids, Decomposer, SubTask};
use crate::swarm::roster::{AgentConfig, Roster};
use crate::swarm::topology::{
    planner_for, select_topology, TopologyType, WorkflowNode, ENTRY_EVENT,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub name: String,
    pub payload: serde_json::Value,
}

/// Run-scoped shared working memory. Subtask outputs land here and are read back
/// as dependency context, so data passes between ephemeral agents without ever
/// re-entering the orchestrating model's context window.
#[derive(Clone, Default)]
pub struct ScratchPad {
    memory: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl ScratchPad {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.memory.read().await.get(key).cloned()
    }

    pub async fn set(&self, key: String, value: serde_json::Value) {
        self.memory.write().await.insert(key, value);
    }

    pub async fn snapshot(&self) -> HashMap<String, serde_json::Value> {
        self.memory.read().await.clone()
    }
}

pub struct EventBus {
    pub sender: mpsc::UnboundedSender<Event>,
    pub receiver: mpsc::UnboundedReceiver<Event>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self { sender, receiver }
    }

    pub fn publish(&self, name: impl Into<String>, payload: serde_json::Value) {
        let _ = self.sender.send(Event {
            name: name.into(),
            payload,
        });
    }
}

/// What one subtask run produced: the envelope output plus any file artifacts
/// the subagent wrote into its per-subtask workspace directory.
#[derive(Debug, Clone, Default)]
pub struct SubtaskResult {
    pub output: String,
    pub artifacts: Vec<String>,
}

/// The bridge from the kernel to actual Goose agent execution: run one subtask as
/// an ephemeral agent and return its final output and captured artifacts.
#[async_trait::async_trait]
pub trait AgentSpawner: Send + Sync {
    async fn run_subtask(
        &self,
        subtask: &SubTask,
        agent: &AgentConfig,
        dependency_context: &str,
    ) -> Result<SubtaskResult>;

    /// Usable token budget for one subtask's prompt material (instruction +
    /// content + dependency context). `None` means unbounded. The kernel enforces
    /// this at dispatch so a subagent's context is never silently overflowed into
    /// mid-task compaction.
    fn prompt_token_budget(&self) -> Option<usize> {
        None
    }
}

const TRUNCATION_MARKER: &str =
    "\n…[dependency context truncated: exceeded the subtask context budget]";

fn artifacts_key(subtask_id: &str) -> String {
    format!("{subtask_id}/artifacts")
}

/// Cap dependency context so instruction + content + context stays within the
/// budget, measured with Goose's real tokenizer. Truncation is tail-first (the
/// earliest prerequisites' outputs survive intact) and always explicitly marked,
/// so the subagent knows its inputs are incomplete rather than hallucinating
/// around a silent gap.
pub async fn fit_dependency_context(subtask: &SubTask, context: String, budget: usize) -> String {
    if context.is_empty() {
        return context;
    }
    let counter = match crate::token_counter::create_token_counter().await {
        Ok(counter) => counter,
        Err(e) => {
            tracing::warn!("token counter unavailable, dependency context not budgeted: {e}");
            return context;
        }
    };
    let base = counter.count_tokens(&subtask.instruction) + counter.count_tokens(&subtask.content);
    let remaining = budget.saturating_sub(base);
    let context_tokens = counter.count_tokens(&context);
    if context_tokens <= remaining {
        return context;
    }
    tracing::warn!(
        subtask = %subtask.id,
        context_tokens,
        remaining,
        budget,
        "dependency context exceeds the subtask budget; truncating"
    );
    if remaining < 64 {
        return TRUNCATION_MARKER.trim_start().to_string();
    }
    let mut cut = (context.len().saturating_mul(remaining) / context_tokens).min(context.len());
    while cut > 0 && !context.is_char_boundary(cut) {
        cut -= 1;
    }
    let kept = context.get(..cut).unwrap_or_default();
    format!("{kept}{TRUNCATION_MARKER}")
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeFailure {
    pub subtask_id: String,
    pub attempts: usize,
    pub error: String,
}

#[derive(Debug, Serialize)]
pub struct SwarmReport {
    pub topology: TopologyType,
    pub subtasks: Vec<SubTask>,
    pub outputs: HashMap<String, String>,
    pub artifacts: HashMap<String, Vec<String>>,
    pub failures: Vec<NodeFailure>,
    pub dispatches: usize,
    pub final_output: String,
    /// Set when a circuit breaker stopped the run before the graph drained.
    pub halted: Option<String>,
}

pub struct Kernel {
    pub event_bus: EventBus,
    pub scratch_pad: ScratchPad,
    spawner: Arc<dyn AgentSpawner>,
    roster: Roster,
    max_retries: usize,
    subtask_timeout: Option<std::time::Duration>,
    max_run_duration: Option<std::time::Duration>,
}

impl Kernel {
    pub fn new(spawner: Arc<dyn AgentSpawner>) -> Self {
        Self::with_roster(spawner, Roster::new())
    }

    pub fn with_roster(spawner: Arc<dyn AgentSpawner>, roster: Roster) -> Self {
        Self {
            event_bus: EventBus::new(),
            scratch_pad: ScratchPad::new(),
            spawner,
            roster,
            max_retries: 2,
            subtask_timeout: None,
            max_run_duration: None,
        }
    }

    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Wall-clock cap per spawner attempt; a timed-out attempt counts as a
    /// retryable failure (SwarmOS `KILL_AND_RESTART_NODE`, bounded by
    /// `max_retries`).
    pub fn with_subtask_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.subtask_timeout = Some(timeout);
        self
    }

    /// Wall-clock circuit breaker for the whole run: once exceeded, no further
    /// nodes dispatch and the report records why (SwarmOS
    /// `max_workflow_duration_seconds`).
    pub fn with_max_run_duration(mut self, max: std::time::Duration) -> Self {
        self.max_run_duration = Some(max);
        self
    }

    /// Decompose the task, plan the topology, and walk the event graph to
    /// completion. Because dispatch is sequential and every event is published
    /// inline, a drained bus means the run is over — no timeouts needed.
    pub async fn run(&mut self, task: &str, decomposer: &dyn Decomposer) -> Result<SwarmReport> {
        let subtasks = decomposer.decompose(task).await?;
        if subtasks.is_empty() {
            return Err(anyhow!("decomposer produced no subtasks"));
        }
        let topology = select_topology(&subtasks);
        let nodes = planner_for(topology).plan(&subtasks)?;
        self.roster.extend_from_subtasks(&subtasks);

        let subtask_by_id: HashMap<&str, &SubTask> =
            subtasks.iter().map(|s| (s.id.as_str(), s)).collect();
        let mut trigger_map: HashMap<&str, Vec<usize>> = HashMap::new();
        for (idx, node) in nodes.iter().enumerate() {
            for evt in &node.trigger_events {
                trigger_map.entry(evt.as_str()).or_default().push(idx);
            }
        }

        let mut join_counts: HashMap<usize, usize> = HashMap::new();
        let mut dispatched: HashMap<usize, bool> = HashMap::new();
        let mut failures: Vec<NodeFailure> = Vec::new();
        let mut dispatches = 0usize;
        let mut halted: Option<String> = None;
        let run_start = tokio::time::Instant::now();

        self.event_bus
            .publish(ENTRY_EVENT, serde_json::json!({ "prompt": task }));

        'run: while let Ok(event) = self.event_bus.receiver.try_recv() {
            let Some(triggered) = trigger_map.get(event.name.as_str()) else {
                continue; // terminal event — no downstream node
            };
            for &idx in triggered {
                let node = &nodes[idx];
                if node.join_after > 1 {
                    let count = join_counts.entry(idx).or_insert(0);
                    *count += 1;
                    if *count < node.join_after {
                        tracing::debug!(
                            node = %node.id,
                            "join barrier waiting: {}/{}",
                            count,
                            node.join_after
                        );
                        continue;
                    }
                }
                if std::mem::replace(dispatched.entry(idx).or_insert(false), true) {
                    continue;
                }
                if let Some(max) = self.max_run_duration {
                    if run_start.elapsed() > max {
                        let reason = format!(
                            "run duration breaker tripped after {:.0?} (limit {:.0?})",
                            run_start.elapsed(),
                            max
                        );
                        tracing::warn!("{reason}");
                        halted = Some(reason);
                        break 'run;
                    }
                }
                dispatches += 1;
                let subtask = subtask_by_id
                    .get(node.subtask_id.as_str())
                    .copied()
                    .ok_or_else(|| anyhow!("node '{}' references unknown subtask", node.id))?;
                self.dispatch(node, subtask, &mut failures).await;
            }
        }

        let mut outputs: HashMap<String, String> = HashMap::new();
        let mut artifacts: HashMap<String, Vec<String>> = HashMap::new();
        for st in &subtasks {
            if let Some(serde_json::Value::String(out)) = self.scratch_pad.get(&st.id).await {
                outputs.insert(st.id.clone(), out);
            }
            if let Some(value) = self.scratch_pad.get(&artifacts_key(&st.id)).await {
                if let Ok(paths) = serde_json::from_value::<Vec<String>>(value) {
                    if !paths.is_empty() {
                        artifacts.insert(st.id.clone(), paths);
                    }
                }
            }
        }
        let final_output = self.collect_final_output(&subtasks, &outputs);

        Ok(SwarmReport {
            topology,
            subtasks,
            outputs,
            artifacts,
            failures,
            dispatches,
            final_output,
            halted,
        })
    }

    async fn dispatch(
        &self,
        node: &WorkflowNode,
        subtask: &SubTask,
        failures: &mut Vec<NodeFailure>,
    ) {
        let agent = self.roster.resolve(subtask);
        let mut dependency_context = self.gather_dependency_context(subtask).await;
        if let Some(budget) = self.spawner.prompt_token_budget() {
            dependency_context = fit_dependency_context(subtask, dependency_context, budget).await;
        }
        let mut last_error = String::new();

        for attempt in 0..=self.max_retries {
            tracing::info!(
                node = %node.id,
                subtask = %subtask.id,
                role = %agent.role,
                attempt = attempt + 1,
                "swarm dispatch"
            );
            let attempt_result = {
                let fut = self
                    .spawner
                    .run_subtask(subtask, &agent, &dependency_context);
                match self.subtask_timeout {
                    Some(limit) => match tokio::time::timeout(limit, fut).await {
                        Ok(result) => result,
                        Err(_) => Err(anyhow!("subtask timed out after {:.0?}", limit)),
                    },
                    None => fut.await,
                }
            };
            match attempt_result {
                Ok(result) => {
                    self.scratch_pad
                        .set(subtask.id.clone(), serde_json::Value::String(result.output))
                        .await;
                    if !result.artifacts.is_empty() {
                        self.scratch_pad
                            .set(
                                artifacts_key(&subtask.id),
                                serde_json::json!(result.artifacts),
                            )
                            .await;
                    }
                    self.event_bus.publish(
                        node.publishes_on_success.clone(),
                        serde_json::json!({ "subtask_id": subtask.id }),
                    );
                    return;
                }
                Err(e) => {
                    last_error = e.to_string();
                    tracing::warn!(
                        node = %node.id,
                        subtask = %subtask.id,
                        "subtask attempt {} failed: {}",
                        attempt + 1,
                        last_error
                    );
                }
            }
        }

        failures.push(NodeFailure {
            subtask_id: subtask.id.clone(),
            attempts: self.max_retries + 1,
            error: last_error,
        });
        self.event_bus.publish(
            node.publishes_on_failure.clone(),
            serde_json::json!({ "subtask_id": subtask.id }),
        );
    }

    async fn gather_dependency_context(&self, subtask: &SubTask) -> String {
        let mut sections = Vec::new();
        for dep in &subtask.dependencies {
            if let Some(serde_json::Value::String(out)) = self.scratch_pad.get(dep).await {
                sections.push(format!("### Output of {dep}\n{out}"));
            }
            if let Some(value) = self.scratch_pad.get(&artifacts_key(dep)).await {
                if let Ok(paths) = serde_json::from_value::<Vec<String>>(value) {
                    if !paths.is_empty() {
                        sections.push(format!(
                            "### Artifact files from {dep} (read them from disk as needed)\n{}",
                            paths
                                .iter()
                                .map(|p| format!("- {p}"))
                                .collect::<Vec<_>>()
                                .join("\n")
                        ));
                    }
                }
            }
        }
        sections.join("\n\n")
    }

    fn collect_final_output(
        &self,
        subtasks: &[SubTask],
        outputs: &HashMap<String, String>,
    ) -> String {
        let sinks = sink_ids(subtasks);
        let produced: Vec<(&String, &String)> = sinks
            .iter()
            .filter_map(|id| outputs.get(id).map(|out| (id, out)))
            .collect();
        match produced.as_slice() {
            [] => String::new(),
            [(_, only)] => (*only).clone(),
            many => many
                .iter()
                .map(|(id, out)| format!("## {id}\n{out}"))
                .collect::<Vec<_>>()
                .join("\n\n"),
        }
    }
}
