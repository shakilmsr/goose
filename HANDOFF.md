# Project Handoff: Goose + SwarmOS Integration

## 1. Project Context & Objectives
The goal of this initiative is to integrate the advanced event-driven orchestration, task decomposition, and topology mapping capabilities of **SwarmOS** natively into the **Goose** Rust framework. 

Currently, Goose handles subagents via a simple flat 1-to-1 polling loop (the LLM explicitly writes a tool call to `delegate`, a subagent spins up, and the LLM explicitly writes a tool call to `load` to read the entire markdown output). We are migrating this to SwarmOS's model, where tasks are semantically decomposed into subtasks, routed via an async `EventBus`, and data is passed securely between ephemeral agents via a `ScratchPad`.

## 2. Completed Work (What has been done)

### A. Architectural Abstraction & Mapping
Fully analyzed and mapped both existing frameworks:
- **Goose Framework**: Documented the `<turn_context>` (in `moim.rs`), the main execution loop (in `agent.rs`), and the background task delegation loop (in `summon.rs` and `subagent_handler.rs`).
- **SwarmOS Architecture**: Documented the structural/semantic decomposer, multi-legged topologies (Pipelines, DAGs), ephemeral roster assignments, and the central Kernel (EventBus & ScratchPad).

*Note: High-level architectural write-ups and Mermaid sequence/flowchart diagrams for these mappings exist in the agent conversation artifacts.*

### B. Rust Scaffolding (`crates/goose/src/swarm/`)
We have created the foundational Rust structures to port SwarmOS into Goose. The following module scaffold was created:
- **`mod.rs`**: Exports the SwarmOS engine. Registered in `crates/goose/src/lib.rs`.
- **`kernel.rs`**: 
  - Defined the `EventBus` using `tokio::sync::mpsc` for async event-driven triggers.
  - Defined the `ScratchPad` memory using `Arc<RwLock<HashMap<String, serde_json::Value>>>` to prevent token bloat during multi-legged tasks.
  - Defined the orchestrator `Kernel` struct.
- **`decompose.rs`**: 
  - Defined the `SubTask` data model.
  - Defined the `Decomposer` async trait with stubs for `StructuralDecomposer` and `SemanticDecomposer`.
- **`topology.rs`**: 
  - Defined `TopologyType` (Pipeline, FanOutJoin, DAG).
  - Defined the `TopologyExecutor` trait with stubs for `PipelineExecutor` and `DagExecutor`.
- **`roster.rs`**: 
  - Defined the `Roster`, which holds `AgentConfig` structs. This bridges SwarmOS roles to actual Goose LLM provider instantiations.

## 3. Next Steps (Where future agents should pick up)

Future work should focus on implementing the logic within the `crates/goose/src/swarm/` module and wiring it into Goose's existing systems. Do not start over or duplicate the abstractions.

**Immediate Action Items:**
1. **Implement `Decomposer` Logic**: Fill out the `SemanticDecomposer` (in `decompose.rs`) to actually call the LLM to generate `SubTask` arrays from a user prompt.
2. **Implement Topologies**: Implement the `PipelineExecutor` and `DagExecutor` (in `topology.rs`) to traverse the generated subtasks and push execution events to the `EventBus`.
3. **Wire the Event Loop**: Write the `Kernel::run` loop (in `kernel.rs`) to listen to the `EventBus`, match tasks against the `Roster`, and spin up active Goose agent sessions.
4. **Integration**: Create a new tool (e.g., `swarm_execute`) alongside Goose's existing `delegate` tool (in `crates/goose/src/agents/platform_extensions/summon.rs`), or modify `agent.rs` so that complex tasks are routed directly to the SwarmOS `Kernel` instead of standard tool execution.

## 4. Crucial Developer Environment Notes
- **Hermit Environment**: The workspace uses Hermit for environment management. Bare commands like `cargo fmt` or `cargo build` will fail natively in Windows PowerShell. Future automated agents must run these via `bash` using `source bin/activate-hermit` (if WSL/GitBash is available), or simply write code and leave the compilation/build tasks to the user's Hermit terminal.
- **Goose Structure**: 
  - Main Agent Logic: `crates/goose/src/agents/agent.rs`
  - Subagent Runner: `crates/goose/src/agents/subagent_handler.rs`
  - SwarmOS Engine: `crates/goose/src/swarm/`
