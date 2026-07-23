//! Topology: the subtask graph compiled into an event graph.
//!
//! Port of SwarmOS `engine/topology.py` (shape auto-selection) and
//! `engine/dag_topology.py` (the general dependency-graph executor). In SwarmOS
//! the topology IS the event graph: a planner emits [`WorkflowNode`]s whose
//! trigger/publish events the kernel walks — it does not execute anything itself.
//! Pipeline and fan-out-join are legible fast-paths; the DAG planner is the
//! lossless catch-all that represents any acyclic shape a decomposition produces.

use crate::swarm::decompose::SubTask;
use anyhow::{anyhow, Result};
use std::collections::HashSet;

pub const ENTRY_EVENT: &str = "SWARM_START";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyType {
    Pipeline,
    FanOutJoin,
    Dag,
    ReviewLoop,
    VerifyLoop,
}

impl serde::Serialize for TopologyType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl TopologyType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TopologyType::Pipeline => "pipeline",
            TopologyType::FanOutJoin => "fan_out_join",
            TopologyType::Dag => "dag",
            TopologyType::ReviewLoop => "review_loop",
            TopologyType::VerifyLoop => "verify_loop",
        }
    }
}

/// Loop wiring carried by a critic node. The kernel routes on the critic's verdict
/// rather than publishing `publishes_on_success` blindly: it emits
/// `worker_done_event` (the worker's canonical completion, which downstream depends
/// on) when the loop converges, or `revision_event` (re-running the worker) while
/// under `max_revisions`.
#[derive(Debug, Clone)]
pub struct CriticWiring {
    pub worker_subtask_id: String,
    pub worker_done_event: String,
    pub revision_event: String,
    pub max_revisions: usize,
}

/// One node of the event graph. `trigger_events` fire it, `join_after > 1` makes
/// it a join barrier (dispatch only after that many trigger occurrences), and on
/// completion the kernel publishes the success or failure event. A node with
/// `critic_of` set is a review-loop critic: the kernel routes on its verdict
/// instead of publishing `publishes_on_success` unconditionally.
#[derive(Debug, Clone)]
pub struct WorkflowNode {
    pub id: String,
    pub subtask_id: String,
    pub trigger_events: Vec<String>,
    pub join_after: usize,
    pub publishes_on_success: String,
    pub publishes_on_failure: String,
    pub critic_of: Option<CriticWiring>,
}

fn safe_id(id: &str) -> String {
    id.chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect()
}

fn done_event(subtask_id: &str) -> String {
    format!("SUBTASK_{}_DONE", safe_id(subtask_id))
}

fn blocked_event(subtask_id: &str) -> String {
    format!("SUBTASK_{}_BLOCKED", safe_id(subtask_id))
}

/// Intermediate event a review-loop worker publishes on success — consumed only by
/// its critic, so downstream (which waits on the canonical done event) does not
/// fire until the critic accepts.
fn review_draft_event(worker_id: &str) -> String {
    format!("RL_DRAFT_{}", safe_id(worker_id))
}

/// Loop-back event the kernel publishes to re-run a review-loop worker.
fn revision_event(worker_id: &str) -> String {
    format!("RL_REVISION_{}", safe_id(worker_id))
}

/// True only for the clean "N independent roots -> exactly one sink" shape:
/// every non-join subtask is a root, the single join depends on all of them, and
/// nothing depends on the join. Anything richer must go through the DAG planner
/// so real dependency edges are never silently discarded.
fn is_clean_fan_out_join(subtasks: &[SubTask]) -> bool {
    let joins: Vec<&SubTask> = subtasks
        .iter()
        .filter(|s| s.dependencies.len() >= 2)
        .collect();
    let [join] = joins.as_slice() else {
        return false;
    };
    let other_ids: HashSet<&str> = subtasks
        .iter()
        .filter(|s| s.id != join.id)
        .map(|s| s.id.as_str())
        .collect();
    let join_deps: HashSet<&str> = join.dependencies.iter().map(String::as_str).collect();
    if join_deps != other_ids {
        return false;
    }
    if subtasks
        .iter()
        .any(|s| s.id != join.id && !s.dependencies.is_empty())
    {
        return false;
    }
    !subtasks
        .iter()
        .any(|s| s.dependencies.iter().any(|d| d == &join.id))
}

/// True only for a strict chain in slice order: the first subtask is a root and
/// each subsequent one depends exactly on its predecessor. A looser test (all
/// dependency counts <= 1) would also match independent sets and stars — shapes
/// whose siblings can run in parallel and must not be serialized by the
/// pipeline planner, which would additionally chain unrelated failures.
fn is_chain(subtasks: &[SubTask]) -> bool {
    subtasks.iter().enumerate().all(|(i, st)| match i {
        0 => st.dependencies.is_empty(),
        _ => st.dependencies.len() == 1 && st.dependencies[0] == subtasks[i - 1].id,
    })
}

/// Infer the topology from the actual dependency graph — the most authoritative
/// signal available (port of `_shape_from_decomposition`).
pub fn select_topology(subtasks: &[SubTask]) -> TopologyType {
    if subtasks.iter().any(|s| s.verify.is_some()) {
        return TopologyType::VerifyLoop;
    }
    // A declared review loop is a cycle the dependency graph cannot express, so it
    // is never inferred — its presence is the signal. It routes through the
    // review-aware DAG planner, which also handles any surrounding acyclic edges.
    if subtasks.iter().any(SubTask::is_critic) {
        return TopologyType::ReviewLoop;
    }
    if subtasks.len() < 2 {
        return TopologyType::Pipeline;
    }
    if is_clean_fan_out_join(subtasks) {
        return TopologyType::FanOutJoin;
    }
    if is_chain(subtasks) {
        return TopologyType::Pipeline;
    }
    TopologyType::Dag
}

pub trait TopologyExecutor: Send + Sync {
    fn plan(&self, subtasks: &[SubTask]) -> Result<Vec<WorkflowNode>>;
}

/// Strict sequential chain in slice order. Selection only routes here when the
/// graph really is a chain (or a single subtask), so the implied stage edges
/// coincide with the declared dependencies.
pub struct PipelineExecutor;

impl TopologyExecutor for PipelineExecutor {
    fn plan(&self, subtasks: &[SubTask]) -> Result<Vec<WorkflowNode>> {
        if subtasks.is_empty() {
            return Err(anyhow!("cannot plan an empty subtask list"));
        }
        Ok(subtasks
            .iter()
            .enumerate()
            .map(|(i, st)| WorkflowNode {
                id: format!("stage_{}", i + 1),
                subtask_id: st.id.clone(),
                trigger_events: vec![if i == 0 {
                    ENTRY_EVENT.to_string()
                } else {
                    done_event(&subtasks[i - 1].id)
                }],
                join_after: 0,
                publishes_on_success: done_event(&st.id),
                publishes_on_failure: blocked_event(&st.id),
                critic_of: None,
            })
            .collect())
    }
}

/// General dependency-graph planner: one node per subtask wired 1:1 from its own
/// `dependencies`. Roots fire on the entry event; a subtask with N dependencies
/// joins on its N predecessors' distinct completion events (`join_after: N`).
/// Also serves fan-out-join — that shape is just a DAG whose join has N root
/// dependencies. Assumes the graph is acyclic (guaranteed by the decomposer's
/// validation), but fails loudly on a dependency naming no known subtask.
pub fn expand_verify_subtasks(subtasks: &[SubTask]) -> Vec<SubTask> {
    let mut expanded = Vec::with_capacity(subtasks.len());
    for st in subtasks {
        expanded.push(st.clone());
        if let Some(spec) = &st.verify {
            if st.reviews.is_none() {
                let verifier_id = format!("{}_verifier", st.id);
                let mut verifier = SubTask::new(
                    verifier_id,
                    format!("Run verification command: {}", spec.command),
                );
                verifier.reviews = Some(st.id.clone());
                verifier.dependencies = vec![st.id.clone()];
                verifier.max_revisions = spec.max_revisions;
                verifier.verify = Some(spec.clone());
                expanded.push(verifier);
            }
        }
    }
    expanded
}

pub struct DagExecutor;

impl TopologyExecutor for DagExecutor {
    fn plan(&self, subtasks: &[SubTask]) -> Result<Vec<WorkflowNode>> {
        if subtasks.is_empty() {
            return Err(anyhow!("cannot plan an empty subtask list"));
        }
        let subtasks = expand_verify_subtasks(subtasks);
        let known: HashSet<&str> = subtasks.iter().map(|s| s.id.as_str()).collect();
        let mut nodes: Vec<WorkflowNode> = subtasks
            .iter()
            .map(|st| {
                for dep in &st.dependencies {
                    if !known.contains(dep.as_str()) {
                        return Err(anyhow!(
                            "subtask '{}' depends on unknown subtask '{}'",
                            st.id,
                            dep
                        ));
                    }
                }
                Ok(WorkflowNode {
                    id: format!("node_{}", safe_id(&st.id)),
                    subtask_id: st.id.clone(),
                    trigger_events: if st.dependencies.is_empty() {
                        vec![ENTRY_EVENT.to_string()]
                    } else {
                        st.dependencies.iter().map(|d| done_event(d)).collect()
                    },
                    join_after: if st.dependencies.len() > 1 {
                        st.dependencies.len()
                    } else {
                        0
                    },
                    publishes_on_success: done_event(&st.id),
                    publishes_on_failure: blocked_event(&st.id),
                    critic_of: None,
                })
            })
            .collect::<Result<_>>()?;
        wire_review_loops(&mut nodes, &subtasks)?;
        Ok(nodes)
    }
}

/// Rewire the plain 1:1 graph for each declared review loop. A critic C
/// (`reviews = W`) gates its worker's canonical completion: W is switched to
/// publish an intermediate draft event (consumed only by C) and to re-trigger on
/// the loop-back event; C triggers on the draft, and its [`CriticWiring`] lets the
/// kernel publish W's canonical done on accept or the loop-back on revise. The
/// parser guarantees each worker has at most one critic, so the last critic in
/// slice order wins if this is ever called on an unvalidated plan.
fn wire_review_loops(nodes: &mut [WorkflowNode], subtasks: &[SubTask]) -> Result<()> {
    let node_index: std::collections::HashMap<String, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.subtask_id.clone(), i))
        .collect();
    for st in subtasks {
        let Some(worker_id) = &st.reviews else {
            continue;
        };
        let worker_idx = *node_index
            .get(worker_id.as_str())
            .ok_or_else(|| anyhow!("critic '{}' reviews unknown subtask '{}'", st.id, worker_id))?;
        let critic_idx = node_index[st.id.as_str()];

        let draft = review_draft_event(worker_id);
        let revision = revision_event(worker_id);

        let worker = &mut nodes[worker_idx];
        worker.publishes_on_success = draft.clone();
        if !worker.trigger_events.contains(&revision) {
            worker.trigger_events.push(revision.clone());
        }

        let critic = &mut nodes[critic_idx];
        critic.trigger_events = vec![draft];
        critic.join_after = 0;
        critic.critic_of = Some(CriticWiring {
            worker_subtask_id: worker_id.clone(),
            worker_done_event: done_event(worker_id),
            revision_event: revision,
            max_revisions: st.max_revisions,
        });
    }
    Ok(())
}

pub fn planner_for(topology: TopologyType) -> Box<dyn TopologyExecutor> {
    match topology {
        TopologyType::Pipeline => Box::new(PipelineExecutor),
        TopologyType::FanOutJoin
        | TopologyType::Dag
        | TopologyType::ReviewLoop
        | TopologyType::VerifyLoop => Box::new(DagExecutor),
    }
}
