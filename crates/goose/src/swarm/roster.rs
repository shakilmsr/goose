//! Roster: subtask -> agent configuration.
//!
//! Port of SwarmOS `engine/dag_topology.py::build_dag_roster` — one individually
//! framed agent per subtask, the subtask's own instruction baked into its system
//! prompt. Bridges SwarmOS roles to Goose subagent instantiation: `model: None`
//! inherits the parent session's model; a registered role or per-subtask entry
//! overrides the derived default.

use crate::swarm::decompose::SubTask;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub role: String,
    pub model: Option<String>,
    pub system_prompt: String,
}

impl AgentConfig {
    /// Derived default framing for a subtask, mirroring `build_dag_roster`'s
    /// base_prompt: own the exact unit of work, build only on the prerequisite
    /// outputs provided, and never invent facts absent from them.
    pub fn derived_for(subtask: &SubTask) -> Self {
        if subtask.is_critic() {
            return Self::critic_for(subtask);
        }
        let role = if subtask.role.is_empty() {
            "worker".to_string()
        } else {
            subtask.role.clone()
        };
        let system_prompt = format!(
            "You are the {role} agent in a multi-agent swarm, responsible for exactly \
             this unit of work: {instruction}\n\n\
             Build on the outputs of your prerequisite subtasks when they are provided. \
             Do not introduce facts, names, or quantities that appear in neither your \
             own instructions nor those outputs. Produce a complete, self-contained \
             result: your final message is consumed verbatim by downstream agents.",
            role = role,
            instruction = subtask.instruction,
        );
        Self {
            role,
            model: subtask.model.clone(),
            system_prompt,
        }
    }

    /// Framing for a review-loop critic: judge the worker's output against the
    /// requirements and return an accept/revise verdict — never rewrite the work.
    fn critic_for(subtask: &SubTask) -> Self {
        let role = if subtask.role.is_empty() {
            "critic".to_string()
        } else {
            subtask.role.clone()
        };
        let system_prompt = format!(
            "You are the {role} agent in a multi-agent swarm: the critic in a review \
             loop. Your job is to evaluate the worker's output against the requirements \
             of this review: {instruction}\n\n\
             The worker's draft is provided as your prerequisite output. Judge it — do \
             NOT rewrite or redo the work yourself. Return 'accept' only if the output \
             genuinely meets the requirements; otherwise return 'revise' with specific, \
             actionable feedback the worker can act on. The worker will re-run seeing \
             only your feedback, so make it concrete and self-contained.",
            role = role,
            instruction = subtask.instruction,
        );
        Self {
            role,
            model: subtask.model.clone(),
            system_prompt,
        }
    }
}

#[derive(Default)]
pub struct Roster {
    agents: HashMap<String, AgentConfig>,
}

impl Roster {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a config under a key: either a subtask id (exact assignment) or a
    /// role name (shared by every subtask carrying that role).
    pub fn register(&mut self, key: impl Into<String>, config: AgentConfig) {
        self.agents.insert(key.into(), config);
    }

    /// Resolve a subtask to an agent config: exact subtask-id entry, then role
    /// entry, then a derived default. Always yields a runnable config so a swarm
    /// never stalls on roster gaps.
    pub fn resolve(&self, subtask: &SubTask) -> AgentConfig {
        self.agents
            .get(&subtask.id)
            .or_else(|| self.agents.get(&subtask.role))
            .cloned()
            .unwrap_or_else(|| AgentConfig::derived_for(subtask))
    }

    pub fn is_empty(&self) -> bool {
        self.agents.is_empty()
    }

    /// Pre-register a derived agent per subtask, keyed by subtask id. Existing
    /// entries win — an operator-supplied assignment is authoritative.
    pub fn extend_from_subtasks(&mut self, subtasks: &[SubTask]) {
        for st in subtasks {
            self.agents
                .entry(st.id.clone())
                .or_insert_with(|| AgentConfig::derived_for(st));
        }
    }
}
