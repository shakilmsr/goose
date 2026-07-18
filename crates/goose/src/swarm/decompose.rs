use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubTask {
    pub id: String,
    pub description: String,
    pub dependencies: Vec<String>,
}

#[async_trait::async_trait]
pub trait Decomposer {
    async fn decompose(&self, task: &str) -> Result<Vec<SubTask>>;
}

pub struct StructuralDecomposer;

#[async_trait::async_trait]
impl Decomposer for StructuralDecomposer {
    async fn decompose(&self, _task: &str) -> Result<Vec<SubTask>> {
        // Scaffold implementation for splitting text based on limits
        Ok(vec![])
    }
}

pub struct SemanticDecomposer;

#[async_trait::async_trait]
impl Decomposer for SemanticDecomposer {
    async fn decompose(&self, _task: &str) -> Result<Vec<SubTask>> {
        // Scaffold implementation for using an LLM to generate subtasks
        Ok(vec![])
    }
}
