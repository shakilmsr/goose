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

## 3. Implemented (2026-07-18)

All four action items from the original handoff are now implemented in `crates/goose/src/swarm/` and wired into summon:

1. **`decompose.rs`** — `SemanticDecomposer` calls the session's own `Provider` (`complete()`) with a JSON-planning system prompt and parses the plan via `parse_subtask_plan`, which validates rather than trusts it (non-empty instructions, index/id dependency normalization with order-preserving dedup, cycle rejection — port of SwarmOS `_from_seam`/`_has_cycle`). Any invalid plan falls back to `StructuralDecomposer` (explicit list items fan out + synthesis subtask, else a single subtask — never invents structure).
2. **`topology.rs`** — `select_topology` infers the shape from the actual dependency graph (port of `_shape_from_decomposition` + `_clean_fan_out_join`): clean N-roots→one-join = FanOutJoin, chain = Pipeline, anything richer = Dag. Planners compile subtasks into `WorkflowNode` event graphs (trigger events, `join_after` barriers, success/failure events); `DagExecutor` is the lossless catch-all and also serves FanOutJoin.
3. **`kernel.rs`** — `Kernel::run` walks the planned event graph: consumes the `EventBus`, counts `join_after` barriers (a join fires exactly once), dispatches each node to an `AgentSpawner` with bounded per-node retries, and passes dependency outputs through the `ScratchPad` (a dependent sees only its direct prerequisites' outputs — no token bloat). Returns a `SwarmReport` (topology, per-subtask outputs, failures, final sink output). v1 dispatches sequentially, same as SwarmOS.
4. **`summon.rs` integration** — a `swarm_execute` tool alongside `delegate`: resolves provider/model/extensions once from the parent session, runs the `SemanticDecomposer`, and executes each subtask as a real ephemeral Goose subagent session via `run_subagent_task` (`SummonAgentSpawner`). Subagent sessions cannot spawn nested swarms.

Added same day:

5. **Token budgeting (real tokenizer, no chars/4 heuristic)** — `decompose::usable_budget` = 40% of `ModelConfig::context_limit()` (SwarmOS `context_budget()`); the `SemanticDecomposer` injects the budget into the planner prompt and warns on oversized chunks via `crate::token_counter` (o200k). The hard guarantee is dispatch-time: `kernel::fit_dependency_context` caps instruction+content+dependency-context against `AgentSpawner::prompt_token_budget()`, truncating tail-first with an explicit marker so a subagent never silently overflows into mid-task compaction.
6. **Tier-1 structured output envelope** — every subtask recipe carries `swarm::envelope::envelope_schema()` (`{status: success|failed, output, notes}`) via `recipe.response`, enforced by Goose's schema-validated `final_output` tool. The spawner interprets the envelope: `status=failed` becomes a kernel-visible failure (drives retries/failure events); non-envelope text degrades to success-with-raw-text. Fixes the gap where any returned prose counted as success.

7. **Tier-2 task-specific output schemas** — the decomposer may attach an `output_schema` (JSON Schema) to any subtask when downstream steps need machine-readable data. Validate-don't-trust: a model-authored schema is accepted only if it is a non-empty object that compiles under `jsonschema::validator_for` (confirmed to reject malformed schemas); an invalid schema is dropped with a warning while the plan survives (the subtask falls back to the plain Tier-1 envelope — a bad schema can't hang the graph the way a cycle could). An accepted schema nests as the envelope's `output` property (`envelope_schema_for`), so the success/failed signal survives and `output` becomes typed JSON; structured outputs flow downstream as compact JSON text.

8. **`split_to_fit` port** — `decompose::split_to_fit` splits text to fit a token budget (paragraphs → sentences → hard char wrap, greedy packing), measured with the real tokenizer. `structural_decompose_with_budget` uses it in the semantic decomposer's fallback path: oversized item content (or an oversized itemless task) splits into fitting subtasks + a synthesis sink whose instruction references only the task's head. LLM-planned chunks are deliberately warned rather than split — slicing a semantic plan's content would silently change what its author asked each worker to do.
9. **Workspace artifact capture + soft write lanes** — each swarm run gets `<working_dir>/.goose-swarm/run-<ts>/` with a per-subtask artifact directory. Subagents are told to write file deliverables there; after each subtask the spawner collects the files into `SubtaskResult.artifacts`, the kernel stores them in the ScratchPad, downstream subtasks see "Artifact files from <dep>" path listings in their dependency context (they read them from disk — the sandbox/script hand-off path), and `SwarmReport.artifacts` lists everything. The decomposer may also declare `workspace_writes` lanes per subtask (sanitized: relative, no traversal) which are injected into the subagent prompt as advisory write lanes — prompt-level, not filesystem-enforced.
10. **Per-subtask extension scoping** — the decomposer is told the session's available extension names and may emit `required_extensions` per subtask (unknown names dropped at parse). The spawner filters the inherited extensions accordingly, failing open to full inheritance when the filter matches nothing real.
11. **Circuit breakers** — `Kernel::with_subtask_timeout` (wall-clock cap per spawner attempt; a timeout counts as a retryable failure, SwarmOS `KILL_AND_RESTART_NODE`) and `Kernel::with_max_run_duration` (whole-run breaker; when tripped, no further nodes dispatch and `SwarmReport.halted` records why). Exposed on `swarm_execute` as `subtask_timeout_secs` / `max_duration_secs`; both off by default.

Tests: `crates/goose/tests/swarm_test.rs` (25, covering all of the above with a fake spawner; breaker tests use real short durations — tokio `start_paused` needs the `test-util` feature goose doesn't enable).

**Remaining / future work (ask the user before starting):**
- Concurrent dispatch of independent siblings (kernel is sequential by design, matching SwarmOS v1).
- Per-subtask model/tier routing: `roster::AgentConfig.model` exists but needs a tier→model mapping decision (config surface) before wiring into the spawner.
- Review-loop topology with a critic-verdict gate (SwarmOS `review_loop` + `_last_critic_passed`).
- Hard (filesystem-enforced) write lanes — current lanes are advisory prompt guidance; true enforcement needs an executor seam or FS sandboxing Goose doesn't have today.
- Concurrent dispatch of independent siblings (kernel is sequential by design in v1).
- Per-subtask model/tier routing: `roster::AgentConfig.model` exists but the spawner currently inherits the parent session's model for every subtask.
- Cost/duration circuit breakers (SwarmOS `_check_global_breakers`) and a `review_loop` topology with a critic-verdict gate.
- Surfacing swarm progress to the UI beyond tool notifications.

## 4. Crucial Developer Environment Notes
- **Hermit Environment**: The workspace uses Hermit for environment management. Bare commands like `cargo fmt` or `cargo build` will fail natively in Windows PowerShell. Future automated agents must run these via `bash` using `source bin/activate-hermit` (if WSL/GitBash is available), or simply write code and leave the compilation/build tasks to the user's Hermit terminal.
- **Goose Structure**: 
  - Main Agent Logic: `crates/goose/src/agents/agent.rs`
  - Subagent Runner: `crates/goose/src/agents/subagent_handler.rs`
  - SwarmOS Engine: `crates/goose/src/swarm/`
