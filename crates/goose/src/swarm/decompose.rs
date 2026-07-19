//! Task decomposition: turn one user prompt into a validated subtask graph.
//!
//! Port of SwarmOS `engine/semantic_decompose.py` + `engine/decompose.py`. The
//! semantic path asks an LLM to plan the subtask graph and validates the result
//! (non-empty instructions, normalized dependencies, acyclic) rather than trusting
//! it; any invalid plan falls back to the deterministic structural path, which
//! never invents structure that was not explicit in the prompt.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::conversation::message::Message;
use crate::providers::base::Provider;
use crate::token_counter::create_token_counter;
use goose_providers::model::ModelConfig;

/// Fraction of a model's context window a subtask's static prompt material
/// (instruction + content) may occupy, mirroring SwarmOS `context_budget()`:
/// the rest is headroom for the system prompt, tools, dependency outputs, the
/// agent's own turns, and its output.
pub const USABLE_CONTEXT_FRACTION: f64 = 0.4;

pub fn usable_budget(context_limit: usize) -> usize {
    ((context_limit as f64 * USABLE_CONTEXT_FRACTION) as usize).max(256)
}

/// Upper bound on subtasks in any plan, LLM-authored or caller-supplied.
pub const MAX_SUBTASKS: usize = 16;

/// Abstract compute class for a subtask. The planner assigns tiers, never model
/// names — the tier→model mapping is operator configuration, so a hallucinated
/// model name can never enter a plan. Unknown tier strings normalize to
/// `Standard` at parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    Light,
    #[default]
    Standard,
    Heavy,
}

impl Tier {
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "light" => Some(Self::Light),
            "standard" => Some(Self::Standard),
            "heavy" => Some(Self::Heavy),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Light => "light",
            Self::Standard => "standard",
            Self::Heavy => "heavy",
        }
    }

    pub fn is_standard(&self) -> bool {
        *self == Self::Standard
    }
}

/// Who authored the plan text being parsed — decides which fields are trusted.
/// A planner LLM may only route via abstract tiers; a caller (the parent
/// session's model or a human-authored plan) may also pin exact models, the same
/// trust level as the tool's top-level `model` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanTrust {
    Planner,
    Caller,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubTask {
    pub id: String,
    pub instruction: String,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
    /// Optional JSON Schema for this subtask's structured result. When set, the
    /// spawner nests it as the `output` property of the Tier-1 envelope, so the
    /// subagent returns validated typed JSON that downstream subtasks consume
    /// structurally instead of as prose.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<serde_json::Value>,
    /// Relative repo paths this subtask declares it will create or modify — soft
    /// write lanes injected into the subagent's prompt so parallel legs don't
    /// collide on files. Advisory, not filesystem-enforced.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workspace_writes: Vec<String>,
    /// Extension names this subtask needs, chosen from the parent session's
    /// enabled extensions. Empty means inherit everything.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_extensions: Vec<String>,
    /// Named source (subrecipe, recipe, or agent) this subtask runs instead of a
    /// purely ad-hoc worker. Resolution to a real recipe happens in the
    /// integration layer before dispatch; an unknown name fails the run early.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Template parameters for `source`. Ignored without a source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    /// Compute class this subtask needs; the integration layer maps it to a
    /// concrete model via operator configuration.
    #[serde(default, skip_serializing_if = "Tier::is_standard")]
    pub tier: Tier,
    /// Exact model for this subtask, overriding tier routing. Only survives
    /// parsing from caller-supplied plans ([`PlanTrust::Caller`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

impl SubTask {
    pub fn new(id: impl Into<String>, instruction: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            instruction: instruction.into(),
            content: String::new(),
            role: String::new(),
            dependencies: Vec::new(),
            output_schema: None,
            workspace_writes: Vec::new(),
            required_extensions: Vec::new(),
            source: None,
            parameters: None,
            tier: Tier::Standard,
            model: None,
        }
    }
}

/// Subtask ids that no other subtask depends on — the graph's final outputs.
pub fn sink_ids(subtasks: &[SubTask]) -> Vec<String> {
    let depended_on: HashSet<&str> = subtasks
        .iter()
        .flat_map(|s| s.dependencies.iter().map(String::as_str))
        .collect();
    subtasks
        .iter()
        .filter(|s| !depended_on.contains(s.id.as_str()))
        .map(|s| s.id.clone())
        .collect()
}

pub fn has_cycle(subtasks: &[SubTask]) -> bool {
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    let deps: HashMap<&str, &Vec<String>> = subtasks
        .iter()
        .map(|s| (s.id.as_str(), &s.dependencies))
        .collect();
    let mut color: HashMap<&str, Color> = deps.keys().map(|&id| (id, Color::White)).collect();

    fn visit<'a>(
        id: &'a str,
        deps: &HashMap<&'a str, &'a Vec<String>>,
        color: &mut HashMap<&'a str, Color>,
    ) -> bool {
        color.insert(id, Color::Gray);
        for dep in deps.get(id).copied().into_iter().flatten() {
            match color.get(dep.as_str()).copied() {
                Some(Color::Gray) => return true,
                Some(Color::White) => {
                    if visit(dep.as_str(), deps, color) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        color.insert(id, Color::Black);
        false
    }

    let ids: Vec<&str> = deps.keys().copied().collect();
    ids.into_iter()
        .any(|id| color[id] == Color::White && visit(id, &deps, &mut color))
}

#[async_trait::async_trait]
pub trait Decomposer: Send + Sync {
    async fn decompose(&self, task: &str) -> Result<Vec<SubTask>>;
}

/// Deterministic decomposition: one subtask per explicit list item in the prompt
/// (plus a synthesis subtask depending on all of them when there is more than one),
/// or a single subtask when the prompt has no explicit item structure. Never
/// invents subtask structure the prompt did not state.
pub struct StructuralDecomposer;

fn list_items(task: &str) -> Vec<String> {
    task.lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            let rest = trimmed
                .strip_prefix("- ")
                .or_else(|| trimmed.strip_prefix("* "))
                .or_else(|| trimmed.strip_prefix("• "))
                .or_else(|| {
                    let digits = trimmed.chars().take_while(|c| c.is_ascii_digit()).count();
                    if digits == 0 {
                        return None;
                    }
                    let after = trimmed.get(digits..)?;
                    after
                        .strip_prefix(". ")
                        .or_else(|| after.strip_prefix(") "))
                })?;
            let item = rest.trim();
            (!item.is_empty()).then(|| item.to_string())
        })
        .collect()
}

impl StructuralDecomposer {
    pub fn decompose_sync(task: &str) -> Vec<SubTask> {
        let task = task.trim();
        if task.is_empty() {
            return Vec::new();
        }
        let items = list_items(task);
        if items.len() < 2 {
            return vec![SubTask::new("subtask-1", task)];
        }
        let mut subtasks: Vec<SubTask> = items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let mut st = SubTask::new(format!("subtask-{}", i + 1), task);
                st.content = item.clone();
                st
            })
            .collect();
        let mut synthesis = SubTask::new(
            format!("subtask-{}", subtasks.len() + 1),
            format!("Synthesize the results of: {}", task),
        );
        synthesis.dependencies = subtasks.iter().map(|s| s.id.clone()).collect();
        subtasks.push(synthesis);
        subtasks
    }
}

#[async_trait::async_trait]
impl Decomposer for StructuralDecomposer {
    async fn decompose(&self, task: &str) -> Result<Vec<SubTask>> {
        let subtasks = Self::decompose_sync(task);
        if subtasks.is_empty() {
            return Err(anyhow!("cannot decompose an empty task"));
        }
        Ok(subtasks)
    }
}

/// A pre-validated plan passed through unchanged — the no-planner fast path for
/// callers that supply their own subtask graph (already run through
/// [`parse_subtask_plan`]).
pub struct FixedPlan(pub Vec<SubTask>);

#[async_trait::async_trait]
impl Decomposer for FixedPlan {
    async fn decompose(&self, _task: &str) -> Result<Vec<SubTask>> {
        if self.0.is_empty() {
            return Err(anyhow!("fixed plan contains no subtasks"));
        }
        Ok(self.0.clone())
    }
}

// ── split_to_fit (port of SwarmOS engine/decompose.py) ───────────────────────

fn sentence_units(paragraph: &str) -> Vec<String> {
    let mut units = Vec::new();
    let mut current = String::new();
    let mut chars = paragraph.chars().peekable();
    while let Some(c) = chars.next() {
        current.push(c);
        if matches!(c, '.' | '!' | '?') && chars.peek().is_none_or(|n| n.is_whitespace()) {
            while chars.peek().is_some_and(|n| n.is_whitespace()) {
                chars.next();
            }
            let unit = current.trim();
            if !unit.is_empty() {
                units.push(unit.to_string());
            }
            current.clear();
        }
    }
    let tail = current.trim();
    if !tail.is_empty() {
        units.push(tail.to_string());
    }
    units
}

fn hard_wrap(
    text: &str,
    max_tokens: usize,
    counter: &crate::token_counter::TokenCounter,
) -> Vec<String> {
    let mut parts = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let total = counter.count_tokens(rest);
        if total <= max_tokens {
            parts.push(rest.to_string());
            break;
        }
        let mut cut = (rest.len().saturating_mul(max_tokens) / total).clamp(1, rest.len());
        while cut > 0 && !rest.is_char_boundary(cut) {
            cut -= 1;
        }
        while cut > 1 && counter.count_tokens(rest.get(..cut).unwrap_or_default()) > max_tokens {
            cut = (cut * 9 / 10).max(1);
            while cut > 0 && !rest.is_char_boundary(cut) {
                cut -= 1;
            }
        }
        if cut == 0 {
            cut = rest
                .char_indices()
                .nth(1)
                .map(|(i, _)| i)
                .unwrap_or(rest.len());
        }
        parts.push(rest.get(..cut).unwrap_or_default().to_string());
        rest = rest.get(cut..).unwrap_or_default();
    }
    parts
}

fn fitting_units(
    text: &str,
    max_tokens: usize,
    counter: &crate::token_counter::TokenCounter,
) -> Vec<String> {
    let mut out = Vec::new();
    for para in text.split("\n\n") {
        let para = para.trim();
        if para.is_empty() {
            continue;
        }
        if counter.count_tokens(para) <= max_tokens {
            out.push(para.to_string());
            continue;
        }
        for sent in sentence_units(para) {
            if counter.count_tokens(&sent) <= max_tokens {
                out.push(sent);
            } else {
                out.extend(hard_wrap(&sent, max_tokens, counter));
            }
        }
    }
    out
}

/// Split text into segments that each fit `max_tokens` (measured with Goose's
/// real tokenizer), greedily packing adjacent units: paragraphs first, then
/// sentences, then a hard character wrap for anything still too large.
pub fn split_to_fit(
    text: &str,
    max_tokens: usize,
    counter: &crate::token_counter::TokenCounter,
) -> Vec<String> {
    let text = text.trim();
    if text.is_empty() {
        return Vec::new();
    }
    if counter.count_tokens(text) <= max_tokens {
        return vec![text.to_string()];
    }
    let mut segments = Vec::new();
    let mut current = String::new();
    for unit in fitting_units(text, max_tokens, counter) {
        if current.is_empty() {
            current = unit;
        } else {
            let combined = format!("{current}\n\n{unit}");
            if counter.count_tokens(&combined) <= max_tokens {
                current = combined;
            } else {
                segments.push(std::mem::replace(&mut current, unit));
            }
        }
    }
    if !current.is_empty() {
        segments.push(current);
    }
    segments
}

fn push_with_synthesis(mut subtasks: Vec<SubTask>, task: &str) -> Vec<SubTask> {
    for (i, st) in subtasks.iter_mut().enumerate() {
        st.id = format!("subtask-{}", i + 1);
    }
    if subtasks.len() > 1 {
        // Reference only the task's head: embedding the full text would recreate
        // the oversized prompt this split exists to avoid.
        let mut gist_end = task.len().min(300);
        while gist_end > 0 && !task.is_char_boundary(gist_end) {
            gist_end -= 1;
        }
        let gist = task.get(..gist_end).unwrap_or_default();
        let ellipsis = if gist_end < task.len() { "…" } else { "" };
        let mut synthesis = SubTask::new(
            format!("subtask-{}", subtasks.len() + 1),
            format!("Synthesize the results of all prior subtasks into one coherent deliverable for the original task: {gist}{ellipsis}"),
        );
        synthesis.dependencies = subtasks.iter().map(|s| s.id.clone()).collect();
        subtasks.push(synthesis);
    }
    subtasks
}

/// Budget-aware structural decomposition: like [`StructuralDecomposer`], but
/// oversized item content (or an oversized itemless task) is split via
/// [`split_to_fit`] so every subtask's static material fits the budget.
pub fn structural_decompose_with_budget(
    task: &str,
    max_tokens: usize,
    counter: &crate::token_counter::TokenCounter,
) -> Vec<SubTask> {
    let task = task.trim();
    if task.is_empty() {
        return Vec::new();
    }
    let items = list_items(task);
    if items.len() >= 2 {
        let content_budget = max_tokens
            .saturating_sub(counter.count_tokens(task))
            .max(16);
        let mut subtasks = Vec::new();
        for item in &items {
            let segments = split_to_fit(item, content_budget, counter);
            let segments = if segments.is_empty() {
                vec![item.clone()]
            } else {
                segments
            };
            for seg in segments {
                let mut st = SubTask::new(String::new(), task);
                st.content = seg;
                subtasks.push(st);
            }
        }
        return push_with_synthesis(subtasks, task);
    }
    if counter.count_tokens(task) <= max_tokens {
        return vec![SubTask::new("subtask-1", task)];
    }
    let parts = split_to_fit(task, max_tokens, counter);
    let subtasks = parts
        .into_iter()
        .map(|part| SubTask::new(String::new(), part))
        .collect();
    push_with_synthesis(subtasks, task)
}

const DECOMPOSER_SYSTEM_PROMPT: &str = r#"You are a task-decomposition planner inside an agent orchestration engine.

Break the user's task into the smallest set of genuinely independent subtasks whose combined results complete it. Respond with ONLY a JSON array (no prose, no code fences), where each element is:

{
  "instruction": "self-contained directive for one worker agent",
  "content": "verbatim material from the task this subtask needs (optional)",
  "role": "short role label like researcher, coder, reviewer, synthesizer (optional)",
  "depends_on": [0, 2],
  "tier": "light",
  "output_schema": {"type": "object", "properties": {"...": "..."}}
}

Rules:
- Each instruction must be fully self-contained: workers are isolated and see ONLY their own instruction, content, and the outputs of the subtasks they depend on. Copy any facts, names, or quantities a subtask needs into its instruction or content.
- "depends_on" lists zero-based indices of prerequisite subtasks whose outputs this subtask needs.
- A synthesis or packaging step must depend on EVERY earlier subtask whose content it needs, not just the most recent one.
- Prefer 1-8 subtasks. A simple task is ONE subtask — do not pad.
- Dependencies must be acyclic.
- "tier" is optional: "light", "standard" (the default), or "heavy" — the compute class this subtask needs. Use "light" for mechanical work (extraction, reformatting, simple lookups), "heavy" for deep reasoning, complex code, or final synthesis. Never name a concrete model; the engine maps tiers to configured models.
- "output_schema" is optional: a JSON Schema object for the subtask's result. Use it ONLY when downstream subtasks or the user need machine-readable data (extracted fields, metrics, configuration, lists of records) — the subtask's final output is then validated against it. Omit it for prose deliverables.
- "workspace_writes" is optional: relative file paths this subtask is expected to create or modify, when the task involves writing files. Two subtasks must never declare the same path."#;

fn decomposer_system_prompt(
    subtask_budget: Option<usize>,
    available_extensions: &[String],
) -> String {
    let mut prompt = DECOMPOSER_SYSTEM_PROMPT.to_string();
    if let Some(budget) = subtask_budget {
        prompt.push_str(&format!(
            "\n- Each subtask's instruction plus content must stay under roughly \
             {budget} tokens; split larger material across subtasks."
        ));
    }
    if !available_extensions.is_empty() {
        prompt.push_str(&format!(
            "\n- \"required_extensions\" is optional: the subset of these available tool \
             extensions the subtask needs: [{}]. Omit it to give the subtask all of them.",
            available_extensions.join(", ")
        ));
    }
    prompt
}

/// LLM-backed decomposition via the session's own provider. The model's plan is
/// validated, never trusted: a malformed entry or dependency cycle rejects the
/// whole plan and falls back to [`StructuralDecomposer`].
pub struct SemanticDecomposer {
    provider: Arc<dyn Provider>,
    model_config: ModelConfig,
    max_subtasks: usize,
    subtask_budget: usize,
    available_extensions: Vec<String>,
}

impl SemanticDecomposer {
    pub fn new(provider: Arc<dyn Provider>, model_config: ModelConfig) -> Self {
        let subtask_budget = usable_budget(model_config.context_limit());
        Self {
            provider,
            model_config,
            max_subtasks: MAX_SUBTASKS,
            subtask_budget,
            available_extensions: Vec::new(),
        }
    }

    /// Tell the planner which tool extensions exist so it can scope
    /// `required_extensions` per subtask; unknown names in the plan are dropped.
    pub fn with_available_extensions(mut self, names: Vec<String>) -> Self {
        self.available_extensions = names;
        self
    }

    /// Flag chunks whose static material already exceeds the per-subtask budget,
    /// measured with Goose's real tokenizer. Dispatch-time capping in the kernel is
    /// the hard guarantee; this surfaces planning misses early. LLM-planned chunks
    /// are warned rather than split: slicing a semantic plan's content would
    /// silently change what its author asked each worker to do.
    fn warn_oversized(&self, subtasks: &[SubTask], counter: &crate::token_counter::TokenCounter) {
        for st in subtasks {
            let tokens = counter.count_tokens(&st.instruction) + counter.count_tokens(&st.content);
            if tokens > self.subtask_budget {
                tracing::warn!(
                    subtask = %st.id,
                    tokens,
                    budget = self.subtask_budget,
                    "subtask static material exceeds its context budget; \
                     dependency context will be squeezed at dispatch"
                );
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawPlanEntry {
    #[serde(default)]
    instruction: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    depends_on: Vec<serde_json::Value>,
    #[serde(default)]
    output_schema: Option<serde_json::Value>,
    #[serde(default)]
    workspace_writes: Vec<String>,
    #[serde(default)]
    required_extensions: Vec<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    parameters: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    tier: Option<String>,
    #[serde(default)]
    model: Option<String>,
}

/// Keep only sane relative paths: non-empty, not absolute, no parent traversal.
fn sanitized_write_lanes(raw: &[String]) -> Vec<String> {
    let mut lanes: Vec<String> = Vec::new();
    for path in raw {
        let path = path.trim().replace('\\', "/");
        let unsafe_path = path.is_empty()
            || path.starts_with('/')
            || path.contains(':')
            || path.split('/').any(|part| part == "..");
        if unsafe_path {
            tracing::warn!(path, "dropping unsafe workspace_writes path from plan");
        } else if !lanes.contains(&path) {
            lanes.push(path);
        }
    }
    lanes.truncate(16);
    lanes
}

fn known_extensions(raw: &[String], available: &[String]) -> Vec<String> {
    let mut kept: Vec<String> = Vec::new();
    for name in raw {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        if !available.is_empty() && !available.iter().any(|a| a == name) {
            tracing::warn!(name, "dropping unknown required_extensions entry from plan");
            continue;
        }
        if !kept.iter().any(|k| k == name) {
            kept.push(name.to_string());
        }
    }
    kept
}

/// Accept a model-authored output schema only if it is a non-empty JSON object
/// that actually compiles as a JSON Schema. An unusable schema is dropped with a
/// warning rather than rejecting the whole plan — the subtask still runs with
/// the plain Tier-1 envelope, and a bad schema cannot hang or corrupt the graph.
fn validated_output_schema(
    raw: Option<serde_json::Value>,
    entry_index: usize,
) -> Option<serde_json::Value> {
    let schema = raw?;
    let valid = matches!(&schema, serde_json::Value::Object(map) if !map.is_empty())
        && jsonschema::validator_for(&schema).is_ok();
    if valid {
        Some(schema)
    } else {
        tracing::warn!(
            entry = entry_index,
            "dropping invalid output_schema from decomposition plan"
        );
        None
    }
}

fn extract_json_array(text: &str) -> Option<&str> {
    let start = text.find('[')?;
    let end = text.rfind(']')?;
    if end <= start {
        return None;
    }
    text.get(start..=end)
}

/// Parse and validate a subtask plan. Returns `None` when the plan is
/// unusable (not a JSON array, an entry without an instruction, or a dependency
/// cycle) so the caller can fall back to the deterministic path.
/// `available_extensions` gates `required_extensions` entries; pass an empty
/// slice to accept any name (the spawner still fails open against reality).
/// `trust` decides whether per-subtask `model` entries survive: planner-authored
/// plans may only route via tiers.
pub fn parse_subtask_plan(
    text: &str,
    max_subtasks: usize,
    available_extensions: &[String],
    trust: PlanTrust,
) -> Option<Vec<SubTask>> {
    let json = extract_json_array(text)?;
    let raw: Vec<RawPlanEntry> = serde_json::from_str(json).ok()?;
    if raw.is_empty() {
        return None;
    }
    let raw = &raw[..raw.len().min(max_subtasks)];
    let ids: Vec<String> = (1..=raw.len()).map(|i| format!("subtask-{}", i)).collect();
    let id_set: HashSet<&str> = ids.iter().map(String::as_str).collect();

    let mut subtasks = Vec::with_capacity(raw.len());
    for (i, entry) in raw.iter().enumerate() {
        let source = entry
            .source
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from);
        if entry.instruction.trim().is_empty() && source.is_none() {
            return None;
        }
        let instruction = if entry.instruction.trim().is_empty() {
            format!("Run source '{}'", source.as_deref().unwrap_or_default())
        } else {
            entry.instruction.trim().to_string()
        };
        let parameters = if source.is_some() {
            entry.parameters.clone()
        } else {
            if entry.parameters.is_some() {
                tracing::warn!(entry = i, "dropping 'parameters' on entry without 'source'");
            }
            None
        };
        let mut deps: Vec<String> = Vec::new();
        let push_dep = |deps: &mut Vec<String>, d: String| {
            if !deps.contains(&d) {
                deps.push(d);
            }
        };
        for dep in &entry.depends_on {
            match dep {
                serde_json::Value::Number(n) => {
                    if let Some(idx) = n.as_u64() {
                        let idx = idx as usize;
                        if idx < raw.len() && idx != i {
                            push_dep(&mut deps, ids[idx].clone());
                        }
                    }
                }
                serde_json::Value::String(s) => {
                    if id_set.contains(s.as_str()) && s != &ids[i] {
                        push_dep(&mut deps, s.clone());
                    }
                }
                _ => {}
            }
        }
        let tier = match entry
            .tier
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            None => Tier::Standard,
            Some(raw) => Tier::parse(raw).unwrap_or_else(|| {
                tracing::warn!(
                    entry = i,
                    tier = raw,
                    "unknown tier in plan; using standard"
                );
                Tier::Standard
            }),
        };
        let model = entry
            .model
            .as_deref()
            .map(str::trim)
            .filter(|m| !m.is_empty())
            .map(String::from);
        let model = match trust {
            PlanTrust::Caller => model,
            PlanTrust::Planner => {
                if let Some(name) = &model {
                    tracing::warn!(
                        entry = i,
                        model = %name,
                        "dropping planner-authored model from plan; planners route via tiers"
                    );
                }
                None
            }
        };
        subtasks.push(SubTask {
            id: ids[i].clone(),
            instruction,
            content: entry.content.trim().to_string(),
            role: entry.role.trim().to_string(),
            dependencies: deps,
            output_schema: validated_output_schema(entry.output_schema.clone(), i),
            workspace_writes: sanitized_write_lanes(&entry.workspace_writes),
            required_extensions: known_extensions(&entry.required_extensions, available_extensions),
            source,
            parameters,
            tier,
            model,
        });
    }

    if has_cycle(&subtasks) {
        return None;
    }
    Some(subtasks)
}

#[async_trait::async_trait]
impl Decomposer for SemanticDecomposer {
    async fn decompose(&self, task: &str) -> Result<Vec<SubTask>> {
        if task.trim().is_empty() {
            return Err(anyhow!("cannot decompose an empty task"));
        }
        let messages = [Message::user().with_text(task)];
        let system_prompt =
            decomposer_system_prompt(Some(self.subtask_budget), &self.available_extensions);
        let plan = match self
            .provider
            .complete(&self.model_config, &system_prompt, &messages, &[])
            .await
        {
            Ok((message, _usage)) => parse_subtask_plan(
                &message.as_concat_text(),
                self.max_subtasks,
                &self.available_extensions,
                PlanTrust::Planner,
            ),
            Err(e) => {
                tracing::warn!(
                    "swarm decomposer provider call failed, using structural fallback: {e}"
                );
                None
            }
        };
        let counter = match create_token_counter().await {
            Ok(counter) => counter,
            Err(e) => {
                tracing::warn!("token counter unavailable, skipping budget validation: {e}");
                return match plan {
                    Some(subtasks) => Ok(subtasks),
                    None => StructuralDecomposer.decompose(task).await,
                };
            }
        };
        let subtasks = match plan {
            Some(subtasks) => {
                self.warn_oversized(&subtasks, &counter);
                subtasks
            }
            None => {
                let subtasks =
                    structural_decompose_with_budget(task, self.subtask_budget, &counter);
                if subtasks.is_empty() {
                    return Err(anyhow!("cannot decompose an empty task"));
                }
                subtasks
            }
        };
        Ok(subtasks)
    }
}
