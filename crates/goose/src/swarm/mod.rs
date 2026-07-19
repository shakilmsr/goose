//! SwarmOS orchestration engine: prompt -> semantic decomposition -> topology
//! selection -> event-driven execution of ephemeral Goose subagents, with data
//! passed through a run-scoped scratch pad instead of the parent context window.

pub mod decompose;
pub mod envelope;
pub mod kernel;
pub mod roster;
pub mod topology;

pub use decompose::{Decomposer, SemanticDecomposer, StructuralDecomposer, SubTask};
pub use kernel::{AgentSpawner, EventBus, Kernel, ScratchPad, SwarmReport};
pub use roster::{AgentConfig as SwarmAgentConfig, Roster};
pub use topology::{select_topology, TopologyType, WorkflowNode};
