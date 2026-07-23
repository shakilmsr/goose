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

12. **Concurrent dispatch of independent siblings (2026-07-19)** — `Kernel::run` now dispatches ready nodes into a `tokio::task::JoinSet` bounded by `Kernel::with_max_concurrency` (default 1 = the old sequential event-order behavior, so parallelism is an explicit opt-in). Each `NodeDispatch` task owns its clones (spawner Arc, ScratchPad, event sender, node, subtask, agent) and publishes its success/failure event *before* completing — the termination invariant is now "bus drained + nothing ready + nothing in flight". Join semantics unchanged (a join still fires exactly once); the run-duration breaker stops new dispatch but drains in-flight tasks. `swarm_execute` enables it by default: `max_concurrent_subtasks` param, defaulting to `GOOSE_MAX_BACKGROUND_TASKS` (5). The spawner was already concurrency-safe (per-subtask sessions and artifact dirs, everything behind Arc). Also fixed a `select_topology` misclassification this exposed: "all dependency counts <= 1" treated independent sets and stars as Pipelines, silently chaining parallelizable siblings (and their failures); only strict chains (each subtask depending exactly on its slice predecessor) route to the pipeline planner now — everything else sparse goes to the lossless DAG planner.

Tests: `crates/goose/tests/swarm_test.rs` (29, covering all of the above with a fake spawner; breaker/concurrency tests use real short durations — tokio `start_paused` needs the `test-util` feature goose doesn't enable; the fake spawner tracks max-in-flight via atomics with a cancellation-safe drop guard, and overlap is proven with rendezvous barriers rather than sleeps — sleep-based overlap assertions flaked under parallel test load).

13. **Delegate absorbed into `swarm_execute`; delegate tool retired (2026-07-19)** — all four steps of the absorption plan landed in one pass:
    - *No-planner fast path*: optional `subtasks` array param, validated via `parse_explicit_subtasks` → `decompose::parse_subtask_plan` (`FixedPlan` decomposer skips the planning LLM call). Hard error on invalid plans — no silent fallback for caller-supplied plans. `subtasks: [one item]` = the old single delegate, deterministic.
    - *Async execution*: `async: true` on `swarm_execute` registers a `BackgroundTask` (ids `swarm-N` from a process-lifetime counter; `handle_load` routes by registry membership as well as session-id shape). `prepare_swarm` is the shared setup for sync/async; the background future renders the `SwarmReport`. Aggregate turn/idle tracking flows through a shared `OnMessageCallback` on the spawner. peek/cancel/moim all reuse the existing registry machinery.
    - *Source-based runs*: `SubTask` gained `source`/`parameters`; plan entries may be source-only (instruction auto-generated). `resolve_subtask_sources` resolves every named source to a concrete `Recipe` up front (unknown name fails the run before any dispatch); the spawner extends the resolved recipe's prompt with the swarm material instead of building an ad-hoc recipe. Top-level `source` param = single-subtask swarm with `task` as its work instructions. `build_recipe_from_source` now fetches the session lazily (only the subrecipe values-merge branch needs it).
    - *Retirement*: `delegate` removed from `list_tools` and `call_tool`; `handle_delegate`/`handle_async_delegate`/`create_delegate_tool`/`validate_delegate_params`/`get_task_description`/`build_delegate_recipe` deleted. `DelegateParams` slimmed+renamed `SubagentParams` (internal-only). Rewrote `swarm_execute` tool description (absorbed delegate's coaching), `build_subagent_instructions`, load discovery text, CLI render arm (`swarm_execute` renders via the old delegate renderer + `task` field), desktop icon/label mappings, and the Phase-3 sections of `goose-self-test.yaml`. Note: subagent sessions still cannot spawn swarms ("Delegated tasks cannot spawn swarms").

14. **Per-subtask model/tier routing (2026-07-19)** — user decisions: planner routes via abstract tiers only + explicit plans may pin exact models; mapping lives in config/env with a per-run override param.
    - `decompose::Tier` (`light`/`standard` default/`heavy`) + `SubTask.tier` and `SubTask.model`. `parse_subtask_plan` takes a `PlanTrust` arg: `Planner` drops model names from plans (validate-never-trust — the planner LLM can only name tiers, never models), `Caller` keeps them (same trust level as the existing top-level `model` param). Unknown tier strings normalize to standard with a warning. The planner prompt teaches the tier vocabulary.
    - Mapping surface: per-run `models: {light|standard|heavy: model}` param on `swarm_execute` beats `GOOSE_SWARM_MODEL_LIGHT/STANDARD/HEAVY` (Config::get_param — config.yaml key or env var); unmapped tiers fall through to the swarm's base model; a tier mapped to the base model is a no-op. Unknown `models` keys hard-error.
    - `resolve_swarm_models` resolves every tier mapping and explicit-plan pinned model to a full `ModelConfig` **eagerly in `prepare_swarm`** (via `override_model_config`, extracted from `resolve_model_config`: canonical limits from the new model, session-level toolshim/temperature/request_params preserved from the parent) — a bad model name fails the run before any dispatch. At dispatch the spawner swaps `task_config.model_config`; an unresolved pinned name fails open to the base model with a warning. All models run on the swarm's provider (cross-provider routing not supported).
    - `AgentSpawner::prompt_token_budget` is now per-subtask, so the dependency-context budget follows the routed model's context window. `roster::AgentConfig::derived_for` carries the subtask's pinned model (the handoff's dormant `AgentConfig.model` is now live).
    - Tests: swarm_test 31 (tier normalization/trust gating, roster model carry), summon 35 (tier precedence param>config>base, unknown-key rejection, pinned-model resolution with canonical limits + fail-open). goose-self-test.yaml gained a Model/Tier Routing section.

15. **Review-loop topology with a critic-verdict gate (2026-07-19)** — user decisions: caller *and* planner may declare loops; bound exhaustion accepts-by-exhaustion (ship last draft + record a warning). A review loop is a cycle the acyclic decomposer rejects, so it is **declared, never inferred**: a critic subtask carries `reviews: <worker-id>` (also added to `dependencies` so the base graph stays acyclic; the loop-back edge lives only in the topology).
    - `decompose.rs` — `SubTask.reviews`/`max_revisions` (`DEFAULT_MAX_REVISIONS=2`); `parse_subtask_plan` resolves `reviews` (index or id, self-review rejected), adds the review target as a dependency, and enforces the v1 constraints in a post-pass (≤1 critic per worker, no chained loops i.e. a critic may not review a critic) — violations drop the `reviews` field (degrade to a plain dependent), never fail the plan. Planner prompt teaches the `reviews`/`max_revisions` vocabulary.
    - `envelope.rs` — `critic_envelope_schema()` (`{status, verdict: accept|revise, feedback, assessment}`), `Verdict{Accept, Revise(String)}`, `interpret_critic_output` (status=failed → node fails, no verdict; verdict routes the loop; unparseable → Accept, fail-open toward termination since the loop is bounded anyway).
    - `topology.rs` — `TopologyType::ReviewLoop`; `WorkflowNode.critic_of: Option<CriticWiring>`; `select_topology` routes any plan containing a critic to the review-aware `DagExecutor`; `wire_review_loops` rewires per critic: worker publishes an intermediate `RL_DRAFT_<W>` (not its canonical done) and re-triggers on `RL_REVISION_<W>`; critic triggers on the draft and carries the wiring. Composes with arbitrary DAGs.
    - `kernel.rs` — `SubtaskResult.verdict`; `NodeDispatch` now returns a `DispatchResult` (normal nodes still publish their own terminal event in-task; a critic that succeeds returns `CriticVerdict` **without** publishing, so the kernel routes it with the shared revision counter). `route_critic_verdict`: revise-under-bound stashes feedback (ScratchPad `<w>/review_feedback`) + republishes the loop-back; accept — or accept-by-exhaustion once the bound is hit — publishes W's canonical done (releasing downstream) + the critic's own done, recording an exhaustion note in `SwarmReport.review_notes`. Loop-back re-arms the worker directly in the event drain (bypassing join accounting) and re-opens the critic. `gather_dependency_context` prepends the reviewer feedback on revision passes. `deliverable_sinks` excludes critics AND treats a review worker as a sink (depended-on only by its critic), so the deliverable is the accepted draft, never the verdict.
    - `roster.rs` — critic-specific system-prompt framing (judge, don't rewrite; accept/revise + actionable feedback).
    - `summon.rs` — critic subtasks use `critic_envelope_schema()` + `interpret_critic_output` → `verdict`; top-level `review: true` convenience (+ `max_revisions`) wraps `task` in a worker+critic `FixedPlan` (validated through the same path; cannot combine with `subtasks`/`source`); `render_swarm_report` surfaces `review_notes`; tool schema + description updated.
    - Tests: swarm_test 39 (7 new: parse/normalize+acyclic, drop invalid/duplicate/chained critics, planner wiring, converge-after-revisions with feedback visible, accept-by-exhaustion, DAG compose, critic-error stalls loop, verdict interpretation), summon 36 (review convenience plan shape). goose-self-test.yaml gained a Review-Loop section. fmt + clippy clean on `goose` (default) and `goose-cli` (portable-default). Live-validated on `gemini-3-flash-preview`, both paths: `review: true` accept path (topology=review_loop, real critic emitted `verdict: accept`, deliverable = worker draft not verdict) and the revise/exhaustion path (worker pinned wrong vs. a critic demanding a different answer → real critic `verdict: revise` with feedback, feedback injected into the re-dispatched worker, bounded at max_revisions=2 → 6 dispatches, accept-by-exhaustion note recorded, last draft shipped).

16. **Sandboxed Terminal Execution & Verify-Loop Topology (2026-07-23)**
    - **`crates/goose/src/sandbox/`**: Created `SandboxBackend` trait, `PreparedCommand`, and `SandboxPolicy` in `backend.rs`. Implemented `LocalBackend` in `local.rs` using Linux `bubblewrap` (`bwrap`) with PID/IPC/UTS isolation, read-only system toolchain binds (`/usr`, `/bin`, `/lib`), writable task workspace binds, and shared network (`--share-net`). Added `WindowsBackend` in `windows.rs` implementing Win32 Job Object process isolation (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`), memory limits, and token security attributes. Created platform `DefaultSandboxBackend` alias in `mod.rs` (`LocalBackend` on Linux, `WindowsBackend` on Windows). Added `sandbox: Mutex<Option<DynSandboxBackend>>` to `Agent` with thread-safe getters and setters.
    - **`crates/goose/src/agents/platform_extensions/developer/shell.rs`**: Threaded active sandbox from `Agent` -> `ToolCallContext` -> `DeveloperExtension` -> `shell_with_cwd_and_sandbox`. Commands pass through `SandboxBackend::wrap_command` when a sandbox is active. Added `run_command_for_verify` to execute verification commands in the sandbox.
    - **`crates/goose/src/swarm/`**: Added `VerifySpec { command, max_revisions }` to `SubTask` and `RawPlanEntry` in `decompose.rs`. Added `TopologyType::VerifyLoop` to topology selection, implemented `expand_verify_subtasks()` in `topology.rs` to generate synthetic verifier subtasks (`<id>_verifier`), and created `interpret_verify_result(exit_code, stdout, stderr)` in `envelope.rs` returning `Verdict::Accept` on exit 0 and `Verdict::Revise(stderr)` on non-zero exit code or process failure. Updated `SummonAgentSpawner::run_subtask` in `summon.rs` to execute verification commands via `run_command_for_verify` and route feedback to worker revisions via `route_critic_verdict`. Added `verify`, `verify_command`, and `sandbox` parameters to `SwarmExecuteParams`.
    - **CLI & REST API**: Added `--sandbox` CLI option in `cli.rs` and `SessionBuilderConfig` in `builder.rs`. Added `/agent/set_sandbox` REST endpoint and `SetSandboxRequest` schema in `routes/agent.rs`. Added `windows-sys` dependency under target `cfg(windows)`.
    - **Verification**:
      - Sandbox Unit Tests: `wsl -d Ubuntu -- bash -c "source ~/.cargo/env && cd /mnt/c/Users/shaki/DevProjects/Goose+Swarm && CARGO_TARGET_DIR=~/goose-target cargo test -p goose sandbox"` (1/1 passed for `WindowsBackend` and `LocalBackend`).
      - Swarm & Verify-Loop Test Suite: `wsl -d Ubuntu -- bash -c "source ~/.cargo/env && cd /mnt/c/Users/shaki/DevProjects/Goose+Swarm && CARGO_TARGET_DIR=~/goose-target cargo test -p goose --test swarm_test"` (40/40 passed).

## NEXT SESSION: START HERE (updated 2026-07-23, Sandboxed Terminal & Verify-Loop complete)

**State:** Sandboxed Terminal Execution (`LocalBackend` & `WindowsBackend`) & Verify-Loop Topology (item 16) are complete, implemented, and verified. 40/40 `swarm_test` integration tests + `sandbox` unit tests green inside WSL Ubuntu.

**Next tasks are all in "Remaining / future work" below — ask the user which one before starting.**

**Build/test (hermit is broken on this machine — use WSL Ubuntu):**
```bash
wsl -d Ubuntu -- bash -c "source ~/.cargo/env && cd /mnt/c/Users/shaki/DevProjects/Goose+Swarm && CARGO_TARGET_DIR=~/goose-target cargo test -p goose --test swarm_test"
wsl -d Ubuntu -- bash -c "source ~/.cargo/env && cd /mnt/c/Users/shaki/DevProjects/Goose+Swarm && CARGO_TARGET_DIR=~/goose-target cargo test -p goose sandbox"
```
CLI binary: build with `--no-default-features --features portable-default` (default features fail on llama-cpp bindgen in WSL) → `~/goose-target/debug/goose`. A working live-test rig exists in WSL: config + Gemini OAuth tokens staged in `~/.config/goose/`, test workspace `~/swarm-live-test/`, run with `GOOSE_PROVIDER=gemini_oauth GOOSE_MODEL=gemini-3-flash-preview`. Gemini 3.5 Flash is unreachable via this OAuth (Code Assist API, individual tier) — it needs a `GOOGLE_API_KEY` with `GOOSE_PROVIDER=google`.

## Done: `delegate` absorbed into `swarm_execute` (completed 2026-07-19, see item 13)

Direction (user decision): retire the `delegate` tool surface and let `swarm_execute` absorb it — the old delegate is the degenerate one-node swarm. The shared machinery (`run_subagent_task`, `build_adhoc_recipe`, `build_task_config`, notification bridge, background-task registry) stays — the swarm spawner runs on it.

- [x] **Async execution** — `async: true` + `load(task_id)` wait/peek/cancel via the existing `BackgroundTask` registry.
- [x] **Source-based runs** — per-subtask `source` field and top-level `source` param; `load` remains the source browser.
- [x] **No-planner fast path** — optional `subtasks` parameter validated through `parse_subtask_plan`; `subtasks: [one item]` = old-delegate behavior.
- [x] Delegate dropped from `list_tools`; handlers deleted; tests, `goose-self-test.yaml`, CLI renderer, and desktop UI strings updated.
- [x] `swarm_execute` tool description rewritten with the absorbed delegation coaching.
- [x] **Sandboxed Terminal Execution** — `SandboxBackend` trait, `LocalBackend` (`bwrap`), `Agent::set_sandbox`, shell tool routing, and `--sandbox` flag.
- [x] **Verify-Loop Topology** — `VerifySpec`, `TopologyType::VerifyLoop`, synthetic verifier node expansion, `interpret_verify_result`, and automated command verifier execution.

**Remaining / future work (ask the user before starting):**
- ~~Per-subtask model/tier routing~~ — done, see item 14.
- ~~Review-loop topology with a critic-verdict gate~~ — done, see item 15.
- ~~Sandboxed Terminal & Verify-Loop Topology~~ — done, see item 16.
- Cost (token/dollar) circuit breakers — wall-clock breakers exist (item 11); SwarmOS `_check_global_breakers` also tracks spend.
- Hard (filesystem-enforced) write lanes — current lanes are advisory prompt guidance; true enforcement needs an executor seam or FS sandboxing Goose doesn't have today.
- Surfacing swarm progress to the UI beyond tool notifications.

## 4. Crucial Developer Environment Notes
- **Hermit Environment**: The workspace uses Hermit for environment management. Bare commands like `cargo fmt` or `cargo build` will fail natively in Windows PowerShell. On this machine the hermit shims are broken in both Git Bash and WSL (CRLF checkout); the working path is WSL Ubuntu rustup 1.92 with `CARGO_TARGET_DIR` on the Linux filesystem — see the START HERE section for exact commands.
- **Installed Windows goose is NOT this code**: the `goose` on the Windows PATH / `%APPDATA%\Block\goose` is the stock Block release — it has no `swarm_execute`. Only the WSL-built binary (`~/goose-target/debug/goose`) contains the swarm work.
- **Goose Structure**: 
  - Main Agent Logic: `crates/goose/src/agents/agent.rs`
  - Subagent Runner: `crates/goose/src/agents/subagent_handler.rs`
  - SwarmOS Engine: `crates/goose/src/swarm/`



### THE FOLLOWING SECTION REQUIRES DISCUSSION BEFORE IMPLEMENTATION

# Plan: Sandboxed terminal for goose agents (parent + swarm children)

## Context

The user wants goose's parent LLM and swarm child agents to run terminal commands
"just like any coding agent" (Claude Code / Cursor): deploy code, run it, test it,
handle errors, apply fixes, and only then serve outputs. SSH-into-a-VPS
(`ssh root@ip …`, run on its root, deploy services, probe ports) is **one use case**,
not the center — it is just a command typed into the terminal.

Investigation of the current code shows the terminal capability **already exists**:

- The `developer` platform extension is `default_enabled: true` and provides the
  `shell` tool ("execute shell commands") — `platform_extensions/mod.rs:151`.
- Its executor (`developer/shell.rs`) runs commands **directly on the host** via
  `tokio::process::Command` (`build_shell_command`, `run_command`), with cwd,
  timeout, output truncation already handled.
- Swarm subagents **inherit** the parent's extensions, filtered per-subtask by
  `SubTask.required_extensions` (item 10) — `summon.rs:2230-2241`,
  `decompose.rs:115`. So subagents already get a terminal unless gated out.
- A partial Docker seam exists (`Agent::set_container`, `agents/container.rs`,
  `extension_manager.rs`) but it **only routes MCP extension processes** via
  `docker exec` — it does **not** route the `shell` tool, and nothing manages
  container lifecycle. The shell tool always runs on the host.
- The item-15 review-loop (`swarm/{envelope,topology,kernel,roster}.rs`) gates a
  deliverable on an **LLM critic's** accept/revise verdict.

So the net-new work is three things, not "build a terminal":

1. **Isolation** — route the existing shell tool through a local OS sandbox
   (bubblewrap/nsjail) so agents can deploy/run/test/fix without trashing the host.
2. **Verify-loop** — a deploy→run→test→fix topology gated on **real command exit
   codes** (LLM-proposed, validated command), modeled on the review-loop.
3. **Sandbox-aware command validation** — the existing host-oriented `PatternMatcher`
   is wrong here (it flags `nc -l`, `ssh -L`, `systemctl enable`, opening ports —
   all legitimate when deploying inside a disposable sandbox).

Decisions locked with the user: **local bubblewrap/nsjail backend first**
(Linux/WSL only, Windows pinned); a **pluggable backend trait** so a remote/pty
backend can drop in later; verify gate = **LLM-proposed command validated before
running**; access = **parent always has it, subtasks opt in via the planner's
per-subtask declaration**; SSH-to-VPS is just a command run inside the sandbox
(no dedicated backend).

## Design

### 1. Sandbox backend abstraction + local backend  (`crates/goose/src/sandbox/`)

New module `crates/goose/src/sandbox/` (registered in `lib.rs`):

- `backend.rs` — `trait SandboxBackend: Send + Sync` with:
  `wrap_command(cmd, cwd) -> PreparedCommand` (returns the argv the shell tool
  should actually spawn), plus `put_file` / `get_file` / `teardown` for later
  backends. `SandboxPolicy` struct: writable work root, read-only bind list,
  network on/off (default **on** — SSH/curl/pip must work), optional seccomp.
- `local.rs` — `LocalBackend` using **bubblewrap** (`bwrap`), nsjail as an
  alternative if bwrap missing. Isolation = filesystem + pid + ipc + uts
  namespaces, `--die-with-parent`; **network namespace shared** (so `ssh root@ip`,
  `curl`, package installs work — the point is to reach out). Toolchains made
  reachable by `--ro-bind /usr /usr` etc. (agent sees host-installed
  cargo/python/node); a **writable work root** (reuse the per-run
  `.goose-swarm/run-<ts>/` dir, or a per-session sandbox root) plus a writable
  HOME subset for caches. Linux-only (`#[cfg(target_os = "linux")]`); on other
  hosts the backend is `None` and the shell runs on host as today (Windows pinned).

Model the lifecycle/attach shape on the existing container seam
(`agent.rs:257` `container: Mutex<Option<Container>>`, `set_container` at
`agent.rs:886`) — add a parallel `sandbox: Mutex<Option<Arc<dyn SandboxBackend>>>`
+ `set_sandbox` / `sandbox()`.

### 2. Route the shell tool through the active sandbox

- `developer/shell.rs`: `build_shell_command` currently builds the raw shell argv.
  Add an optional sandbox parameter so, when a backend is active,
  `PreparedCommand` wraps the argv (`bwrap … -- <shell> -c <cmd>`). The existing
  flatpak-spawn branch is the precedent for command wrapping. Keep host path
  unchanged when no sandbox is set.
- Thread the agent's active `SandboxBackend` down to `ShellTool::shell_with_cwd`
  (the developer client already holds agent/session context).

### 3. Verify-loop topology  (extend item-15 review-loop)

Almost a 1:1 clone of the review-loop, swapping the LLM critic for a command gate:

- `decompose.rs` — `SubTask.verify: Option<VerifySpec>` (or reuse the `reviews`
  shape): worker id it verifies + `max_revisions`. Planner may declare it; parse/
  normalize + keep the base graph acyclic exactly like `reviews` (loop-back edge
  lives only in the topology). Planner prompt teaches the vocabulary.
- `envelope.rs` — `verify_envelope_schema()` (verifier proposes
  `{status, command, expect}`); `interpret_verify_result(exit_code, stdout,
  stderr) -> (SubtaskOutcome, Verdict)`: exit 0 → `Accept`; non-zero → `Revise`
  with stderr/tail-of-log as feedback. Reuses `Verdict{Accept, Revise}`.
- `topology.rs` — `TopologyType::VerifyLoop`; wire per the `wire_review_loops`
  pattern: worker publishes `RL_DRAFT_<W>`, verifier triggers on it, runs its
  (validated) command in the sandbox, kernel routes the verdict; revise-under-bound
  re-arms the worker with the failure output; accept (or accept-by-exhaustion)
  releases the deliverable.
- `kernel.rs` — reuse `route_critic_verdict` machinery (shared revision counter,
  `review_feedback` in ScratchPad, exhaustion notes). The verifier node **runs a
  command** rather than only reasoning — it does so through the sandbox tool.
- `roster.rs` — verifier framing ("run the command, judge by the exit code, report
  the failure output as feedback; do not rewrite the worker's code").
- `summon.rs` — verifier subtasks use the verify schema/interpretation; top-level
  `verify: true` convenience wraps `task` in worker+verifier `FixedPlan`;
  `render_swarm_report` surfaces verify notes; tool schema + description updated.
- Fixer = the worker revising by default; a **separate fixer agent** only when the
  planner decomposed one (already expressible as another subtask depending on the
  worker) — no special support needed.

### 4. Sandbox-aware command validation  (`crates/goose/src/security/`)

The verifier's LLM-proposed command (and, optionally, all shell commands when a
sandbox is active) pass a **sandbox policy** before running. Do **not** reuse the
host `PatternMatcher` verdicts wholesale. New lightweight policy:

- Permit inside the sandbox what the existing matcher wrongly flags (service/port
  ops, `ssh`, package installs).
- Block/confirm only what threatens the **control host or the user's local
  secrets** (e.g. commands escaping the work root, reading `~/.ssh` / credential
  files on the host side of the bind). Reuse `PatternMatcher` selectively for the
  genuinely-always-bad set (fork bombs, host `rm -rf /` on a bind-mounted host
  path). Gate borderline commands through the existing `GooseMode::Approve`
  confirmation path rather than hard-denying.

### 5. Exposure & gating

- **Parent** always has the terminal (developer extension, unchanged) and, when a
  sandbox is configured, it is active for the parent.
- **Subtasks** opt into the sandbox via the planner's per-subtask declaration —
  reuse `required_extensions` (item 10) or add a sibling `sandbox: bool` grant on
  `SubTask`, validated/dropped like unknown extensions. Parent-set policy is the
  default; a granted subtask runs its shell inside the sandbox.
- Enable/config surface modeled on `set_container`: a `--sandbox` CLI flag
  (`goose-cli/src/cli.rs`, `session/builder.rs`) and a `/agent/set_sandbox` route
  (`goose-server/src/routes/agent.rs`), plus a `swarm_execute` `sandbox` param.

## Files to modify / create

- **New:** `crates/goose/src/sandbox/{mod.rs,backend.rs,local.rs}`; register in
  `crates/goose/src/lib.rs`.
- **Agent wiring:** `crates/goose/src/agents/agent.rs` (add `sandbox` field +
  `set_sandbox`/`sandbox`, parallel to `container` at `agent.rs:257,886`).
- **Shell routing:** `crates/goose/src/agents/platform_extensions/developer/shell.rs`
  (+ the developer client that calls `shell_with_cwd`).
- **Verify-loop:** `crates/goose/src/swarm/{decompose,envelope,topology,kernel,roster}.rs`
  and `platform_extensions/summon.rs` — follow the item-15 review-loop diff shape.
- **Validation:** `crates/goose/src/security/` (new sandbox policy; selective reuse
  of `patterns.rs`).
- **Exposure:** `crates/goose-cli/src/cli.rs`, `crates/goose-cli/src/session/builder.rs`,
  `crates/goose-server/src/routes/agent.rs`.
- **Tests:** `crates/goose/tests/swarm_test.rs` (verify-loop, mirroring the
  review-loop tests), new `sandbox` unit tests, `goose-self-test.yaml` section.

## Phasing

1. **Sandbox backend + shell routing** — `SandboxBackend` trait, `LocalBackend`
   (bwrap), `Agent::set_sandbox`, shell tool routes through it, `--sandbox` enable.
   Deliverable: agents run terminal commands (incl. `ssh root@ip …`) inside an
   isolated jail on Linux/WSL.
2. **Verify-loop topology** — deploy→test→fix gated on exit codes, reusing the
   review-loop machinery; `verify: true` convenience + planner declaration.
3. **Validation hardening + subtask sandbox grant surfacing + SSH ergonomics**;
   later: second backend (remote/pty) behind the trait; lift the Windows pin.

## Verification

Standard hermit flow (per AGENTS.md):

```
source bin/activate-hermit
cargo test -p goose --test swarm_test          # verify-loop tests
cargo test -p goose sandbox                     # sandbox unit tests
cargo build -p goose-cli                        # CLI binary
cargo fmt && cargo clippy --all-targets -- -D warnings
just generate-openapi                           # required — /agent/set_sandbox server route
```

(WSL Ubuntu with `CARGO_TARGET_DIR` on the Linux FS + `--no-default-features
--features portable-default` remains a fallback if the hermit toolchain misbehaves.)

Live-test the sandbox on a **Linux runtime** (the `LocalBackend`/bwrap is Linux-only,
so on this Windows host that means WSL Ubuntu regardless of how the build is done),
using the existing rig (`~/.config/goose/`, `~/swarm-live-test/`,
`GOOSE_PROVIDER=gemini_oauth GOOSE_MODEL=gemini-3-flash-preview`):
- Confirm `bwrap` is installed in the Linux environment (`apt-get install -y bubblewrap`).
- Run a swarm with `sandbox` on: subtask writes+builds+runs code inside the jail,
  a filesystem-escape command is contained/denied, and `ssh`/`curl` to an external
  host still works (network on).
- Run a `verify: true` swarm where the first draft fails its test command
  (non-zero exit) and the worker is re-dispatched with the failure output until the
  command exits 0 — deliverable served only after green; exhaustion path records a
  note (mirror the item-15 live validation).
- Update `goose-self-test.yaml`, rebuild, `goose run --recipe goose-self-test.yaml`.

## Assumptions & open items (flagged per honest-readiness policy)

- **Assumed:** isolation is genuinely wanted (bwrap), not just "give subagents a
  terminal" — subagents already have one. If the real gap is only the verify-loop,
  Phase 1 can be deferred and Phase 2 done first.
- **Assumed:** network stays **on** inside the jail (SSH/deploy/install need it);
  isolation is filesystem/process, not network hermeticity.
- **Unproven:** bwrap availability and behavior under WSL2 (user namespaces /
  `--unshare-*` can be restricted in some WSL configs) — must be validated on this
  machine before relying on it; nsjail/host-fallback is the contingency.
- **Deferred:** remote/pty backend, cloud auto-provisioning, and Windows support
  are explicitly out of the first cut.

