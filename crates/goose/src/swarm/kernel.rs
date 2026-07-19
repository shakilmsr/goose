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
//! Independent siblings dispatch concurrently, bounded by
//! [`Kernel::with_max_concurrency`] (default 1 — sequential, in event order —
//! so parallelism is an explicit opt-in). Each in-flight node publishes its
//! success or failure event before its task completes, so the loop can always
//! re-drain the bus after a join and the run is over exactly when the bus is
//! drained, nothing is ready, and nothing is in flight.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;

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
    /// content + dependency context). `None` means unbounded. Per-subtask because
    /// tier/model routing can give subtasks different context windows. The kernel
    /// enforces this at dispatch so a subagent's context is never silently
    /// overflowed into mid-task compaction.
    fn prompt_token_budget(&self, _subtask: &SubTask) -> Option<usize> {
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
    max_concurrency: usize,
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
            max_concurrency: 1,
            subtask_timeout: None,
            max_run_duration: None,
        }
    }

    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// How many independent subtasks may run at once. 1 (the default) preserves
    /// strict sequential event-order dispatch; higher values let siblings whose
    /// dependencies are all satisfied run in parallel. Join semantics are
    /// unaffected — a join still fires exactly once, after all its
    /// prerequisites' completion events.
    pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency.max(1);
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
    /// completion. Every in-flight node publishes its terminal event before its
    /// task finishes, so after each completion the bus is re-drained; the run is
    /// over when the bus is empty, nothing is ready, and nothing is in flight —
    /// no timeouts needed.
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
        let mut dispatched = vec![false; nodes.len()];
        let mut ready: VecDeque<usize> = VecDeque::new();
        let mut in_flight: JoinSet<Option<NodeFailure>> = JoinSet::new();
        let mut failures: Vec<NodeFailure> = Vec::new();
        let mut dispatches = 0usize;
        let mut halted: Option<String> = None;
        let run_start = tokio::time::Instant::now();

        self.event_bus
            .publish(ENTRY_EVENT, serde_json::json!({ "prompt": task }));

        loop {
            while let Ok(event) = self.event_bus.receiver.try_recv() {
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
                    if std::mem::replace(&mut dispatched[idx], true) {
                        continue;
                    }
                    ready.push_back(idx);
                }
            }

            while halted.is_none() && in_flight.len() < self.max_concurrency {
                let Some(idx) = ready.pop_front() else {
                    break;
                };
                if let Some(max) = self.max_run_duration {
                    if run_start.elapsed() > max {
                        let reason = format!(
                            "run duration breaker tripped after {:.0?} (limit {:.0?})",
                            run_start.elapsed(),
                            max
                        );
                        tracing::warn!("{reason}");
                        halted = Some(reason);
                        ready.clear();
                        break;
                    }
                }
                dispatches += 1;
                let node = &nodes[idx];
                let subtask = subtask_by_id
                    .get(node.subtask_id.as_str())
                    .copied()
                    .ok_or_else(|| anyhow!("node '{}' references unknown subtask", node.id))?;
                in_flight.spawn(
                    NodeDispatch {
                        spawner: Arc::clone(&self.spawner),
                        scratch_pad: self.scratch_pad.clone(),
                        events: self.event_bus.sender.clone(),
                        max_retries: self.max_retries,
                        subtask_timeout: self.subtask_timeout,
                        node: node.clone(),
                        subtask: subtask.clone(),
                        agent: self.roster.resolve(subtask),
                    }
                    .run(),
                );
            }

            if in_flight.is_empty() {
                break;
            }
            if let Some(joined) = in_flight.join_next().await {
                match joined {
                    Ok(Some(failure)) => failures.push(failure),
                    Ok(None) => {}
                    Err(e) => tracing::error!("swarm dispatch task aborted: {e}"),
                }
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

/// One node's dispatch, owned so it can run as a spawned task alongside its
/// siblings. Publishes the node's success or failure event before completing —
/// the kernel's termination invariant depends on that ordering.
struct NodeDispatch {
    spawner: Arc<dyn AgentSpawner>,
    scratch_pad: ScratchPad,
    events: mpsc::UnboundedSender<Event>,
    max_retries: usize,
    subtask_timeout: Option<std::time::Duration>,
    node: WorkflowNode,
    subtask: SubTask,
    agent: AgentConfig,
}

impl NodeDispatch {
    async fn run(self) -> Option<NodeFailure> {
        let mut dependency_context =
            gather_dependency_context(&self.scratch_pad, &self.subtask).await;
        if let Some(budget) = self.spawner.prompt_token_budget(&self.subtask) {
            dependency_context =
                fit_dependency_context(&self.subtask, dependency_context, budget).await;
        }
        let mut last_error = String::new();

        for attempt in 0..=self.max_retries {
            tracing::info!(
                node = %self.node.id,
                subtask = %self.subtask.id,
                role = %self.agent.role,
                attempt = attempt + 1,
                "swarm dispatch"
            );
            let attempt_result = {
                let fut = self
                    .spawner
                    .run_subtask(&self.subtask, &self.agent, &dependency_context);
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
                        .set(
                            self.subtask.id.clone(),
                            serde_json::Value::String(result.output),
                        )
                        .await;
                    if !result.artifacts.is_empty() {
                        self.scratch_pad
                            .set(
                                artifacts_key(&self.subtask.id),
                                serde_json::json!(result.artifacts),
                            )
                            .await;
                    }
                    self.publish(&self.node.publishes_on_success);
                    return None;
                }
                Err(e) => {
                    last_error = e.to_string();
                    tracing::warn!(
                        node = %self.node.id,
                        subtask = %self.subtask.id,
                        "subtask attempt {} failed: {}",
                        attempt + 1,
                        last_error
                    );
                }
            }
        }

        self.publish(&self.node.publishes_on_failure);
        Some(NodeFailure {
            subtask_id: self.subtask.id.clone(),
            attempts: self.max_retries + 1,
            error: last_error,
        })
    }

    fn publish(&self, event_name: &str) {
        let _ = self.events.send(Event {
            name: event_name.to_string(),
            payload: serde_json::json!({ "subtask_id": self.subtask.id }),
        });
    }
}

async fn gather_dependency_context(scratch_pad: &ScratchPad, subtask: &SubTask) -> String {
    let mut sections = Vec::new();
    for dep in &subtask.dependencies {
        if let Some(serde_json::Value::String(out)) = scratch_pad.get(dep).await {
            sections.push(format!("### Output of {dep}\n{out}"));
        }
        if let Some(value) = scratch_pad.get(&artifacts_key(dep)).await {
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
