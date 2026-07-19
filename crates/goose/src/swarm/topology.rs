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
        }
    }
}

/// One node of the event graph. `trigger_events` fire it, `join_after > 1` makes
/// it a join barrier (dispatch only after that many trigger occurrences), and on
/// completion the kernel publishes the success or failure event.
#[derive(Debug, Clone)]
pub struct WorkflowNode {
    pub id: String,
    pub subtask_id: String,
    pub trigger_events: Vec<String>,
    pub join_after: usize,
    pub publishes_on_success: String,
    pub publishes_on_failure: String,
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
pub struct DagExecutor;

impl TopologyExecutor for DagExecutor {
    fn plan(&self, subtasks: &[SubTask]) -> Result<Vec<WorkflowNode>> {
        if subtasks.is_empty() {
            return Err(anyhow!("cannot plan an empty subtask list"));
        }
        let known: HashSet<&str> = subtasks.iter().map(|s| s.id.as_str()).collect();
        subtasks
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
                })
            })
            .collect()
    }
}

pub fn planner_for(topology: TopologyType) -> Box<dyn TopologyExecutor> {
    match topology {
        TopologyType::Pipeline => Box::new(PipelineExecutor),
        TopologyType::FanOutJoin | TopologyType::Dag => Box::new(DagExecutor),
    }
}
