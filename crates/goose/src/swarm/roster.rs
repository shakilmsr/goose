use anyhow::Result;
use std::collections::HashMap;

/// Represents the configuration required to instantiate a Goose Agent for a subtask
#[derive(Clone)]
pub struct AgentConfig {
    pub model: String,
    pub system_prompt: String,
}

pub struct Roster {
    agents: HashMap<String, AgentConfig>,
}

impl Default for Roster {
    fn default() -> Self {
        Self::new()
    }
}

impl Roster {
    pub fn new() -> Self {
        Self {
            agents: HashMap::new(),
        }
    }

    pub fn register(&mut self, role: String, config: AgentConfig) {
        self.agents.insert(role, config);
    }

    pub fn get_agent_for_task(&self, role: &str) -> Result<AgentConfig> {
        self.agents
            .get(role)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Role '{}' not found in roster", role))
    }
}
