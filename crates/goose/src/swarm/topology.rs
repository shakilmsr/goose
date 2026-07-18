use crate::swarm::decompose::SubTask;
use anyhow::Result;

pub enum TopologyType {
    Pipeline,
    FanOutJoin,
    DAG,
}

pub trait TopologyExecutor {
    fn execute(&self, subtasks: Vec<SubTask>) -> Result<()>;
}

pub struct PipelineExecutor;

impl TopologyExecutor for PipelineExecutor {
    fn execute(&self, _subtasks: Vec<SubTask>) -> Result<()> {
        // Scaffold: execute tasks strictly sequentially
        Ok(())
    }
}

pub struct DagExecutor;

impl TopologyExecutor for DagExecutor {
    fn execute(&self, _subtasks: Vec<SubTask>) -> Result<()> {
        // Scaffold: execute tasks according to their dependency graph
        Ok(())
    }
}
