use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use goose::swarm::decompose::{
    has_cycle, parse_subtask_plan, sink_ids, split_to_fit, structural_decompose_with_budget,
    Decomposer, StructuralDecomposer, SubTask,
};
use goose::swarm::envelope::{
    envelope_schema, envelope_schema_for, interpret_subtask_output, SubtaskOutcome,
};
use goose::swarm::kernel::{fit_dependency_context, AgentSpawner, Kernel, SubtaskResult};
use goose::swarm::roster::AgentConfig;
use goose::swarm::topology::{planner_for, select_topology, TopologyType};
use goose::token_counter::create_token_counter;
use tokio::sync::Mutex;

fn subtask(id: &str, deps: &[&str]) -> SubTask {
    let mut st = SubTask::new(id, format!("do {}", id));
    st.dependencies = deps.iter().map(|d| d.to_string()).collect();
    st
}

struct StaticDecomposer(Vec<SubTask>);

#[async_trait::async_trait]
impl Decomposer for StaticDecomposer {
    async fn decompose(&self, _task: &str) -> Result<Vec<SubTask>> {
        Ok(self.0.clone())
    }
}

/// Records dispatch order and dependency context; fails a subtask id a
/// configurable number of times before succeeding.
struct FakeSpawner {
    calls: Mutex<Vec<(String, String)>>,
    failures: Mutex<HashMap<String, usize>>,
    output_sizes: HashMap<String, usize>,
    artifacts_for: HashMap<String, Vec<String>>,
    delay: Option<Duration>,
    budget: Option<usize>,
    /// Rendezvous points keyed by subtask id: a participating subtask blocks
    /// until the barrier's full party is inside `run_subtask` simultaneously.
    /// Proves real overlap without sleeping — a kernel that under-dispatches
    /// deadlocks here (converted to a failure by the test's timeout wrapper).
    barriers: HashMap<String, Arc<tokio::sync::Barrier>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl FakeSpawner {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            failures: Mutex::new(HashMap::new()),
            output_sizes: HashMap::new(),
            artifacts_for: HashMap::new(),
            delay: None,
            budget: None,
            barriers: HashMap::new(),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
        }
    }

    fn failing(id: &str, times: usize) -> Self {
        let spawner = Self::new();
        spawner
            .failures
            .try_lock()
            .unwrap()
            .insert(id.to_string(), times);
        spawner
    }
}

#[async_trait::async_trait]
impl AgentSpawner for FakeSpawner {
    async fn run_subtask(
        &self,
        st: &SubTask,
        _agent: &AgentConfig,
        dependency_context: &str,
    ) -> Result<SubtaskResult> {
        struct InFlightGuard<'a>(&'a AtomicUsize);
        impl Drop for InFlightGuard<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let _guard = InFlightGuard(&self.in_flight);
        self.calls
            .lock()
            .await
            .push((st.id.clone(), dependency_context.to_string()));
        if let Some(barrier) = self.barriers.get(&st.id) {
            barrier.wait().await;
        }
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        let mut failures = self.failures.lock().await;
        if let Some(remaining) = failures.get_mut(&st.id) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(anyhow!("simulated failure for {}", st.id));
            }
        }
        let output = match self.output_sizes.get(&st.id) {
            Some(words) => format!("output of {} ", st.id).repeat(*words),
            None => format!("output of {}", st.id),
        };
        Ok(SubtaskResult {
            output,
            artifacts: self.artifacts_for.get(&st.id).cloned().unwrap_or_default(),
        })
    }

    fn prompt_token_budget(&self) -> Option<usize> {
        self.budget
    }
}

// ── decomposition ────────────────────────────────────────────────────────────

#[tokio::test]
async fn structural_decomposer_fans_out_list_items_with_synthesis() {
    let task = "Compare these sources:\n- source A says X\n- source B says Y\n- source C says Z";
    let subtasks = StructuralDecomposer.decompose(task).await.unwrap();
    assert_eq!(subtasks.len(), 4);
    assert_eq!(subtasks[0].content, "source A says X");
    assert!(subtasks[3].instruction.starts_with("Synthesize"));
    assert_eq!(
        subtasks[3].dependencies,
        vec!["subtask-1", "subtask-2", "subtask-3"]
    );
    assert_eq!(sink_ids(&subtasks), vec!["subtask-4"]);
}

#[tokio::test]
async fn structural_decomposer_single_subtask_without_items() {
    let subtasks = StructuralDecomposer
        .decompose("Just summarize this document.")
        .await
        .unwrap();
    assert_eq!(subtasks.len(), 1);
    assert!(subtasks[0].dependencies.is_empty());
}

#[test]
fn parse_plan_normalizes_index_and_id_dependencies() {
    let text = r#"Here is the plan:
[
  {"instruction": "research", "role": "researcher"},
  {"instruction": "draft", "depends_on": [0]},
  {"instruction": "review", "depends_on": ["subtask-2", "subtask-2", 1]}
]"#;
    let subtasks = parse_subtask_plan(text, 16, &[]).unwrap();
    assert_eq!(subtasks.len(), 3);
    assert_eq!(subtasks[1].dependencies, vec!["subtask-1"]);
    // duplicate deps collapse so join_after matches distinct completion events
    assert_eq!(subtasks[2].dependencies, vec!["subtask-2"]);
    assert_eq!(subtasks[0].role, "researcher");
}

#[test]
fn parse_plan_rejects_empty_instruction_and_cycles() {
    assert!(parse_subtask_plan(r#"[{"instruction": "  "}]"#, 16, &[]).is_none());
    let cyclic = r#"[
      {"instruction": "a", "depends_on": [1]},
      {"instruction": "b", "depends_on": [0]}
    ]"#;
    assert!(parse_subtask_plan(cyclic, 16, &[]).is_none());
    assert!(parse_subtask_plan("no json here", 16, &[]).is_none());
}

#[test]
fn cycle_detection() {
    let acyclic = vec![
        subtask("a", &[]),
        subtask("b", &["a"]),
        subtask("c", &["a", "b"]),
    ];
    assert!(!has_cycle(&acyclic));
    let cyclic = vec![subtask("a", &["b"]), subtask("b", &["a"])];
    assert!(has_cycle(&cyclic));
}

// ── topology ─────────────────────────────────────────────────────────────────

#[test]
fn select_topology_from_graph_shape() {
    let chain = vec![
        subtask("a", &[]),
        subtask("b", &["a"]),
        subtask("c", &["b"]),
    ];
    assert_eq!(select_topology(&chain), TopologyType::Pipeline);

    let fanout = vec![
        subtask("a", &[]),
        subtask("b", &[]),
        subtask("join", &["a", "b"]),
    ];
    assert_eq!(select_topology(&fanout), TopologyType::FanOutJoin);

    // diamond with a mid-graph join is not a clean fan-out-join
    let messy = vec![
        subtask("a", &[]),
        subtask("b", &["a"]),
        subtask("c", &["a"]),
        subtask("d", &["b", "c"]),
    ];
    assert_eq!(select_topology(&messy), TopologyType::Dag);

    assert_eq!(
        select_topology(&[subtask("only", &[])]),
        TopologyType::Pipeline
    );

    // independent sets and stars have sparse deps but are NOT chains — routing
    // them to the pipeline planner would serialize parallelizable siblings
    let independent = vec![subtask("a", &[]), subtask("b", &[]), subtask("c", &[])];
    assert_eq!(select_topology(&independent), TopologyType::Dag);
    let star = vec![
        subtask("a", &[]),
        subtask("b", &["a"]),
        subtask("c", &["a"]),
    ];
    assert_eq!(select_topology(&star), TopologyType::Dag);
}

#[test]
fn dag_planner_wires_joins_from_dependencies() {
    let subtasks = vec![
        subtask("a", &[]),
        subtask("b", &["a"]),
        subtask("c", &["a"]),
        subtask("d", &["b", "c"]),
    ];
    let nodes = planner_for(TopologyType::Dag).plan(&subtasks).unwrap();
    assert_eq!(nodes[0].trigger_events, vec!["SWARM_START"]);
    assert_eq!(nodes[1].trigger_events, vec!["SUBTASK_a_DONE"]);
    assert_eq!(nodes[3].join_after, 2);
    assert_eq!(
        nodes[3].trigger_events,
        vec!["SUBTASK_b_DONE", "SUBTASK_c_DONE"]
    );

    let unknown_dep = vec![subtask("a", &["ghost"])];
    assert!(planner_for(TopologyType::Dag).plan(&unknown_dep).is_err());
}

// ── kernel ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn kernel_runs_diamond_in_dependency_order_with_single_join() {
    let subtasks = vec![
        subtask("a", &[]),
        subtask("b", &["a"]),
        subtask("c", &["a"]),
        subtask("d", &["b", "c"]),
    ];
    let spawner = Arc::new(FakeSpawner::new());
    let mut kernel = Kernel::new(spawner.clone());
    let report = kernel
        .run("diamond", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    let calls = spawner.calls.lock().await;
    let order: Vec<&str> = calls.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(
        order,
        vec!["a", "b", "c", "d"],
        "join must fire exactly once"
    );

    let (_, d_context) = calls.iter().find(|(id, _)| id == "d").unwrap();
    assert!(d_context.contains("output of b") && d_context.contains("output of c"));

    assert_eq!(report.topology, TopologyType::Dag);
    assert_eq!(report.dispatches, 4);
    assert!(report.failures.is_empty());
    assert_eq!(report.final_output, "output of d");
}

#[tokio::test]
async fn kernel_retries_transient_failure_then_succeeds() {
    let subtasks = vec![subtask("a", &[]), subtask("b", &["a"])];
    let spawner = Arc::new(FakeSpawner::failing("a", 1));
    let mut kernel = Kernel::new(spawner.clone());
    let report = kernel
        .run("retry", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert!(report.failures.is_empty());
    assert_eq!(report.outputs.len(), 2);
    // subtask a attempted twice, then b once
    assert_eq!(spawner.calls.lock().await.len(), 3);
}

#[tokio::test]
async fn kernel_reports_exhausted_failure_and_blocks_downstream() {
    let subtasks = vec![subtask("a", &[]), subtask("b", &["a"])];
    let spawner = Arc::new(FakeSpawner::failing("a", usize::MAX));
    let mut kernel = Kernel::new(spawner.clone()).with_max_retries(1);
    let report = kernel
        .run("failure", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].subtask_id, "a");
    assert_eq!(report.failures[0].attempts, 2);
    assert!(report.outputs.is_empty());
    assert!(report.final_output.is_empty());
    // b never dispatched: its trigger event never fired
    assert_eq!(spawner.calls.lock().await.len(), 2);
}

// ── concurrent dispatch ──────────────────────────────────────────────────────

#[tokio::test]
async fn kernel_concurrent_siblings_overlap_and_join_once() {
    let subtasks = vec![
        subtask("a", &[]),
        subtask("b", &[]),
        subtask("c", &[]),
        subtask("join", &["a", "b", "c"]),
    ];
    let mut spawner = FakeSpawner::new();
    let rendezvous = Arc::new(tokio::sync::Barrier::new(3));
    for id in ["a", "b", "c"] {
        spawner.barriers.insert(id.to_string(), rendezvous.clone());
    }
    let spawner = Arc::new(spawner);
    let mut kernel = Kernel::new(spawner.clone()).with_max_concurrency(3);
    let report = tokio::time::timeout(
        Duration::from_secs(30),
        kernel.run("fanout", &StaticDecomposer(subtasks)),
    )
    .await
    .expect("kernel under-dispatched: roots never met at the rendezvous barrier")
    .unwrap();

    assert!(report.failures.is_empty());
    assert_eq!(report.dispatches, 4, "join must fire exactly once");
    assert_eq!(
        spawner.max_in_flight.load(Ordering::SeqCst),
        3,
        "all three independent roots must run simultaneously"
    );
    let calls = spawner.calls.lock().await;
    assert_eq!(calls.last().unwrap().0, "join", "join runs after all roots");
    let (_, join_context) = calls.iter().find(|(id, _)| id == "join").unwrap();
    for dep in ["a", "b", "c"] {
        assert!(join_context.contains(&format!("output of {dep}")));
    }
    assert_eq!(report.final_output, "output of join");
}

#[tokio::test]
async fn kernel_concurrency_cap_bounds_in_flight() {
    let subtasks = vec![
        subtask("a", &[]),
        subtask("b", &[]),
        subtask("c", &[]),
        subtask("d", &[]),
    ];
    let mut spawner = FakeSpawner::new();
    // Party of 2 across four subtasks: under a cap of 2 they can only clear the
    // barrier pairwise, so the run completing at all proves pairwise overlap.
    let rendezvous = Arc::new(tokio::sync::Barrier::new(2));
    for id in ["a", "b", "c", "d"] {
        spawner.barriers.insert(id.to_string(), rendezvous.clone());
    }
    let spawner = Arc::new(spawner);
    let mut kernel = Kernel::new(spawner.clone()).with_max_concurrency(2);
    let report = tokio::time::timeout(
        Duration::from_secs(30),
        kernel.run("capped", &StaticDecomposer(subtasks)),
    )
    .await
    .expect("kernel under-dispatched: siblings never met at the rendezvous barrier")
    .unwrap();

    assert_eq!(
        report.topology,
        TopologyType::Dag,
        "independent set must not be misclassified as a pipeline"
    );
    assert_eq!(report.outputs.len(), 4);
    let max = spawner.max_in_flight.load(Ordering::SeqCst);
    assert!(max <= 2, "cap of 2 exceeded: {max} in flight");
    assert!(max >= 2, "siblings never overlapped under a cap of 2");
}

#[tokio::test]
async fn kernel_concurrent_diamond_keeps_dependency_order() {
    let subtasks = vec![
        subtask("a", &[]),
        subtask("b", &["a"]),
        subtask("c", &["a"]),
        subtask("d", &["b", "c"]),
    ];
    let mut spawner = FakeSpawner::new();
    spawner.delay = Some(Duration::from_millis(10));
    let spawner = Arc::new(spawner);
    let mut kernel = Kernel::new(spawner.clone()).with_max_concurrency(4);
    let report = kernel
        .run("diamond", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert!(report.failures.is_empty());
    assert_eq!(report.dispatches, 4);
    let calls = spawner.calls.lock().await;
    let order: Vec<&str> = calls.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(order.len(), 4);
    assert_eq!(order[0], "a", "root must run before its dependents");
    assert_eq!(order[3], "d", "join must run after both branches");
    let (_, d_context) = calls.iter().find(|(id, _)| id == "d").unwrap();
    assert!(d_context.contains("output of b") && d_context.contains("output of c"));
    assert_eq!(report.final_output, "output of d");
}

#[tokio::test]
async fn kernel_concurrent_sibling_failure_blocks_join_and_terminates() {
    let subtasks = vec![
        subtask("a", &[]),
        subtask("b", &[]),
        subtask("join", &["a", "b"]),
    ];
    let mut spawner = FakeSpawner::failing("b", usize::MAX);
    spawner.delay = Some(Duration::from_millis(10));
    let spawner = Arc::new(spawner);
    let mut kernel = Kernel::new(spawner.clone())
        .with_max_retries(1)
        .with_max_concurrency(2);
    let report = kernel
        .run("partial failure", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].subtask_id, "b");
    assert_eq!(report.outputs.len(), 1, "only a completes");
    assert!(report.final_output.is_empty(), "join never produced output");
    let calls = spawner.calls.lock().await;
    assert!(
        !calls.iter().any(|(id, _)| id == "join"),
        "join must not dispatch with a failed prerequisite"
    );
}

// ── token budgeting ──────────────────────────────────────────────────────────

#[tokio::test]
async fn fit_dependency_context_passes_through_within_budget() {
    let st = subtask("a", &["dep"]);
    let context = "### Output of dep\nshort output".to_string();
    let fitted = fit_dependency_context(&st, context.clone(), 10_000).await;
    assert_eq!(fitted, context);
}

#[tokio::test]
async fn fit_dependency_context_truncates_with_explicit_marker() {
    let st = subtask("a", &["dep"]);
    let context = format!(
        "### Output of dep\n{}",
        "lorem ipsum dolor sit amet ".repeat(200)
    );
    let fitted = fit_dependency_context(&st, context.clone(), 150).await;
    assert!(fitted.len() < context.len());
    assert!(fitted.contains("truncated"));
    assert!(
        fitted.starts_with("### Output of dep"),
        "head must survive tail-first truncation"
    );
}

#[tokio::test]
async fn fit_dependency_context_collapses_to_marker_when_no_room() {
    let mut st = subtask("a", &["dep"]);
    st.instruction = "word ".repeat(500);
    let fitted = fit_dependency_context(&st, "some context".to_string(), 100).await;
    assert!(fitted.contains("truncated"));
    assert!(!fitted.contains("some context"));
}

#[tokio::test]
async fn kernel_caps_dependency_context_at_dispatch() {
    let subtasks = vec![subtask("a", &[]), subtask("b", &["a"])];
    let mut spawner = FakeSpawner::new();
    spawner.output_sizes.insert("a".to_string(), 500);
    spawner.budget = Some(200);
    let spawner = Arc::new(spawner);
    let mut kernel = Kernel::new(spawner.clone());
    let report = kernel
        .run("budget", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert!(report.failures.is_empty());
    let calls = spawner.calls.lock().await;
    let (_, b_context) = calls.iter().find(|(id, _)| id == "b").unwrap();
    assert!(b_context.contains("truncated"));
    assert!(b_context.starts_with("### Output of a"));
}

// ── structured output envelope ───────────────────────────────────────────────

#[test]
fn envelope_success_failed_and_raw_fallback() {
    assert_eq!(
        interpret_subtask_output(
            r#"{"status": "success", "output": "the result", "notes": "fyi"}"#
        ),
        SubtaskOutcome::Success("the result".to_string())
    );
    assert_eq!(
        interpret_subtask_output(r#"{"status": "failed", "output": "", "notes": "missing input"}"#),
        SubtaskOutcome::Failed("missing input".to_string())
    );
    assert_eq!(
        interpret_subtask_output(r#"{"status": "failed", "output": "partial attempt"}"#),
        SubtaskOutcome::Failed("partial attempt".to_string())
    );
    // non-envelope text degrades to success with the raw text
    assert_eq!(
        interpret_subtask_output("I finished the analysis: everything checks out."),
        SubtaskOutcome::Success("I finished the analysis: everything checks out.".to_string())
    );
}

#[test]
fn parse_plan_accepts_valid_output_schema_and_drops_invalid() {
    let text = r#"[
      {"instruction": "extract metrics",
       "output_schema": {"type": "object", "properties": {"sharpe": {"type": "number"}}}},
      {"instruction": "summarize", "depends_on": [0],
       "output_schema": {"type": 123}},
      {"instruction": "report", "depends_on": [1], "output_schema": {}}
    ]"#;
    let subtasks = parse_subtask_plan(text, 16, &[]).unwrap();
    assert_eq!(subtasks.len(), 3);
    assert!(subtasks[0].output_schema.is_some());
    assert_eq!(
        subtasks[0].output_schema.as_ref().unwrap()["properties"]["sharpe"]["type"],
        "number"
    );
    // invalid and empty schemas are dropped, but the plan survives
    assert!(subtasks[1].output_schema.is_none());
    assert!(subtasks[2].output_schema.is_none());
}

#[test]
fn envelope_schema_nests_task_specific_output_schema() {
    let plain = envelope_schema();
    assert_eq!(plain["properties"]["output"]["type"], "string");
    assert_eq!(plain["required"], serde_json::json!(["status", "output"]));

    let task_schema = serde_json::json!({
        "type": "object",
        "required": ["symbols"],
        "properties": {"symbols": {"type": "array", "items": {"type": "string"}}}
    });
    let nested = envelope_schema_for(Some(&task_schema));
    assert_eq!(nested["properties"]["output"]["type"], "object");
    assert_eq!(
        nested["properties"]["output"]["required"],
        serde_json::json!(["symbols"])
    );
    // status/notes machinery is untouched by nesting
    assert_eq!(nested["required"], serde_json::json!(["status", "output"]));
    assert_eq!(
        nested["properties"]["status"]["enum"],
        serde_json::json!(["success", "failed"])
    );
}

#[test]
fn envelope_interprets_structured_output_as_compact_json() {
    let raw = r#"{"status": "success", "output": {"symbols": ["VXX", "UVXY"], "count": 2}}"#;
    match interpret_subtask_output(raw) {
        SubtaskOutcome::Success(output) => {
            let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
            assert_eq!(parsed["symbols"][0], "VXX");
            assert_eq!(parsed["count"], 2);
        }
        other => panic!("expected success, got {:?}", other),
    }
    // failure with structured output and no notes stringifies the output as the reason
    match interpret_subtask_output(r#"{"status": "failed", "output": {"error": "no data"}}"#) {
        SubtaskOutcome::Failed(reason) => assert!(reason.contains("no data")),
        other => panic!("expected failure, got {:?}", other),
    }
}

// ── split_to_fit ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn split_to_fit_splits_and_packs_paragraphs() {
    let counter = create_token_counter().await.unwrap();
    let text = "short paragraph.";
    assert_eq!(split_to_fit(text, 100, &counter), vec![text.to_string()]);

    let big = (0..40)
        .map(|i| format!("Paragraph number {} has a handful of words in it.", i))
        .collect::<Vec<_>>()
        .join("\n\n");
    let parts = split_to_fit(&big, 60, &counter);
    assert!(parts.len() > 1);
    for part in &parts {
        assert!(counter.count_tokens(part) <= 60);
    }
    let rejoined = parts.join("\n\n");
    for i in 0..40 {
        assert!(
            rejoined.contains(&format!("Paragraph number {} ", i)),
            "lost paragraph {}",
            i
        );
    }
}

#[tokio::test]
async fn structural_budget_splits_oversized_item_and_keeps_synthesis() {
    let counter = create_token_counter().await.unwrap();
    let huge_item = "This sentence pads the item with words. ".repeat(40);
    let task = format!("Process these:\n- small item one\n- {}", huge_item.trim());
    let subtasks = structural_decompose_with_budget(&task, 120, &counter);
    assert!(
        subtasks.len() > 10,
        "huge item should split into many parts"
    );
    let synthesis = subtasks.last().unwrap();
    assert_eq!(synthesis.dependencies.len(), subtasks.len() - 1);
    let content_budget = 16; // 120 minus the oversized task instruction, floored
    for st in &subtasks[..subtasks.len() - 1] {
        assert!(counter.count_tokens(&st.content) <= content_budget);
    }
}

// ── lanes and extension scoping ──────────────────────────────────────────────

#[test]
fn parse_plan_sanitizes_lanes_and_extensions() {
    let text = r#"[
      {"instruction": "write code",
       "workspace_writes": ["src/lib.rs", "../evil.rs", "/abs/path", "src/lib.rs", "C:\\win\\path"],
       "required_extensions": ["developer", "made_up_ext", "developer"]}
    ]"#;
    let available = vec!["developer".to_string(), "memory".to_string()];
    let subtasks = parse_subtask_plan(text, 16, &available).unwrap();
    assert_eq!(subtasks[0].workspace_writes, vec!["src/lib.rs"]);
    assert_eq!(subtasks[0].required_extensions, vec!["developer"]);
}

// ── artifacts ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn kernel_flows_artifacts_downstream_and_reports_them() {
    let subtasks = vec![subtask("a", &[]), subtask("b", &["a"])];
    let mut spawner = FakeSpawner::new();
    spawner
        .artifacts_for
        .insert("a".to_string(), vec!["/ws/a/script.py".to_string()]);
    let spawner = Arc::new(spawner);
    let mut kernel = Kernel::new(spawner.clone());
    let report = kernel
        .run("artifacts", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert_eq!(report.artifacts["a"], vec!["/ws/a/script.py"]);
    assert!(!report.artifacts.contains_key("b"));
    let calls = spawner.calls.lock().await;
    let (_, b_context) = calls.iter().find(|(id, _)| id == "b").unwrap();
    assert!(b_context.contains("Artifact files from a"));
    assert!(b_context.contains("/ws/a/script.py"));
}

// ── circuit breakers ─────────────────────────────────────────────────────────

#[tokio::test]
async fn kernel_times_out_slow_subtask_and_retries_then_fails() {
    let subtasks = vec![subtask("a", &[])];
    let mut spawner = FakeSpawner::new();
    spawner.delay = Some(Duration::from_millis(200));
    let spawner = Arc::new(spawner);
    let mut kernel = Kernel::new(spawner.clone())
        .with_max_retries(1)
        .with_subtask_timeout(Duration::from_millis(50));
    let report = kernel
        .run("timeout", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert_eq!(report.failures.len(), 1);
    assert!(report.failures[0].error.contains("timed out"));
    assert_eq!(report.failures[0].attempts, 2);
    assert_eq!(spawner.calls.lock().await.len(), 2);
}

#[tokio::test]
async fn kernel_run_duration_breaker_halts_remaining_dispatch() {
    let subtasks = vec![subtask("a", &[]), subtask("b", &["a"])];
    let mut spawner = FakeSpawner::new();
    spawner.delay = Some(Duration::from_millis(100));
    let spawner = Arc::new(spawner);
    let mut kernel = Kernel::new(spawner.clone()).with_max_run_duration(Duration::from_millis(50));
    let report = kernel
        .run("breaker", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert!(report.halted.is_some());
    assert_eq!(spawner.calls.lock().await.len(), 1, "b must not dispatch");
    assert_eq!(report.outputs.len(), 1);
}

#[tokio::test]
async fn kernel_pipeline_passes_previous_stage_output() {
    let subtasks = vec![
        subtask("a", &[]),
        subtask("b", &["a"]),
        subtask("c", &["b"]),
    ];
    let spawner = Arc::new(FakeSpawner::new());
    let mut kernel = Kernel::new(spawner.clone());
    let report = kernel
        .run("pipeline", &StaticDecomposer(subtasks))
        .await
        .unwrap();

    assert_eq!(report.topology, TopologyType::Pipeline);
    let calls = spawner.calls.lock().await;
    let (_, c_context) = calls.iter().find(|(id, _)| id == "c").unwrap();
    assert!(c_context.contains("output of b"));
    assert_eq!(report.final_output, "output of c");
}
