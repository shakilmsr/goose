use crate::agents::extension::PlatformExtensionContext;
use crate::agents::mcp_client::{Error, McpClientTrait};
use crate::agents::subagent_handler::{run_subagent_task, OnMessageCallback, SubagentRunParams};
use crate::agents::subagent_task_config::{TaskConfig, DEFAULT_SUBAGENT_MAX_TURNS};
use crate::agents::tool_execution::ToolCallContext;
use crate::agents::AgentConfig;
use crate::config::paths::Paths;
use crate::config::{Config, GooseMode};
use crate::providers;
use crate::recipe::build_recipe::build_recipe_from_template;
use crate::recipe::local_recipes::load_local_recipe_file;
use crate::recipe::{Recipe, RecipeParameter, Response, Settings, RECIPE_FILE_EXTENSIONS};
use crate::session::extension_data::EnabledExtensionsState;
use crate::session::SessionType;
use crate::sources::parse_frontmatter;
use crate::swarm::decompose::{
    parse_subtask_plan, usable_budget, Decomposer, FixedPlan, SemanticDecomposer, SubTask,
    MAX_SUBTASKS,
};
use crate::swarm::envelope::{envelope_schema_for, interpret_subtask_output, SubtaskOutcome};
use crate::swarm::kernel::{AgentSpawner, Kernel, SubtaskResult, SwarmReport};
use crate::swarm::roster::AgentConfig as SwarmAgentConfig;
use crate::utils::safe_truncate;
use anyhow::Result;
use async_trait::async_trait;
use goose_sdk_types::custom_requests::{SourceEntry, SourceType};
use rmcp::model::{
    CallToolResult, Content, Implementation, InitializeResult, JsonObject, ListToolsResult, Meta,
    ServerCapabilities, ServerNotification, Tool,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

pub static EXTENSION_NAME: &str = "summon";

const SUBAGENT_DESCRIPTION_BUDGET: usize = 160;

const TASK_LABEL_BUDGET: usize = 60;

fn kind_plural(kind: SourceType) -> &'static str {
    match kind {
        SourceType::Subrecipe => "Subrecipes",
        SourceType::Recipe => "Recipes",
        SourceType::Agent => "Agents",
        _ => "Other",
    }
}

/// Internal knobs for building one subagent run (recipe + task config). This
/// was the `delegate` tool's parameter surface; the tool is gone but the swarm
/// spawner and source resolution run on the same machinery.
#[derive(Debug, Default)]
pub struct SubagentParams {
    pub instructions: Option<String>,
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    pub extensions: Option<Vec<String>>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub temperature: Option<f32>,
    pub max_turns: Option<usize>,
    pub working_dir: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SwarmExecuteParams {
    pub task: String,
    #[serde(default)]
    pub subtasks: Option<serde_json::Value>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub parameters: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub max_turns: Option<usize>,
    #[serde(default)]
    pub subtask_timeout_secs: Option<u64>,
    #[serde(default)]
    pub max_duration_secs: Option<u64>,
    #[serde(default)]
    pub max_concurrent_subtasks: Option<usize>,
    #[serde(default)]
    pub r#async: bool,
}

/// Validate a caller-supplied subtask plan. Unlike the LLM path there is no
/// fallback: an explicit plan that fails validation is an error the caller can
/// fix, not something to silently replace.
fn parse_explicit_subtasks(
    value: &serde_json::Value,
    available_extensions: &[String],
) -> Result<Vec<SubTask>, String> {
    let arr = value
        .as_array()
        .ok_or("'subtasks' must be a JSON array of subtask objects")?;
    if arr.is_empty() {
        return Err("'subtasks' must not be empty".to_string());
    }
    if arr.len() > MAX_SUBTASKS {
        return Err(format!(
            "'subtasks' supports at most {} entries (got {})",
            MAX_SUBTASKS,
            arr.len()
        ));
    }
    let text = serde_json::to_string(value).map_err(|e| e.to_string())?;
    parse_subtask_plan(&text, MAX_SUBTASKS, available_extensions).ok_or_else(|| {
        "'subtasks' is not a valid plan: every entry needs a non-empty 'instruction', \
         and 'depends_on' (zero-based indices) must reference other entries without cycles"
            .to_string()
    })
}

pub struct BackgroundTask {
    pub id: String,
    pub description: String,
    pub started_at: Instant,
    pub turns: Arc<AtomicU32>,
    pub last_activity: Arc<AtomicU64>,
    pub handle: JoinHandle<Result<String>>,
    pub cancellation_token: CancellationToken,
    pub notification_buffer: Arc<Mutex<Vec<ServerNotification>>>,
}

pub struct CompletedTask {
    pub id: String,
    pub description: String,
    pub result: Result<String, String>,
    pub turns_taken: u32,
    pub duration: Duration,
    pub completed_at: Instant,
}

/// Result from handle_load_task_result with structured metadata for the caller
#[derive(Debug)]
struct TaskLoadResult {
    content: Vec<Content>,
    status: &'static str,
    turns: Option<u32>,
    duration_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct AgentMetadata {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    model: Option<String>,
}

fn parse_agent_content(content: &str, path: &Path) -> Option<SourceEntry> {
    let (metadata, body): (AgentMetadata, String) = match parse_frontmatter(content) {
        Ok(Some(parsed)) => parsed,
        Ok(None) => return None,
        Err(e) => {
            // Missing fields means this file has valid YAML but isn't an agent — skip silently.
            // Only warn on actual YAML syntax errors.
            if e.to_string().contains("missing field") {
                return None;
            }
            warn!("Failed to parse agent file {}: {}", path.display(), e);
            return None;
        }
    };

    let description = metadata.description.unwrap_or_else(|| {
        let model_info = metadata
            .model
            .as_ref()
            .map(|m| format!(" ({})", m))
            .unwrap_or_default();
        format!("Agent{}", model_info)
    });

    Some(SourceEntry {
        source_type: SourceType::Agent,
        name: metadata.name,
        description,
        content: body,
        path: path.to_string_lossy().into_owned(),
        global: false,
        writable: true,
        supporting_files: Vec::new(),
        properties: std::collections::HashMap::new(),
    })
}

fn scan_recipes_from_dir(
    dir: &Path,
    kind: SourceType,
    suppress_config_warnings: bool,
    sources: &mut Vec<SourceEntry>,
    seen: &mut std::collections::HashSet<String>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !RECIPE_FILE_EXTENSIONS.contains(&ext) {
            continue;
        }

        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

        if name.is_empty() || seen.contains(&name) {
            continue;
        }

        match Recipe::from_file_path(&path) {
            Ok(recipe) => {
                seen.insert(name.clone());
                sources.push(SourceEntry {
                    source_type: kind,
                    name,
                    description: recipe.description.clone(),
                    content: recipe.instructions.clone().unwrap_or_default(),
                    path: path.to_string_lossy().into_owned(),
                    global: false,
                    writable: true,
                    supporting_files: Vec::new(),
                    properties: std::collections::HashMap::new(),
                });
            }
            Err(e) => {
                // The working directory commonly contains project config like package.json
                // and tsconfig.json, which parse as valid JSON but lack Recipe fields. In that
                // case treat them as "not a recipe" rather than warning. Dedicated recipe
                // directories still warn so a real recipe with a typo is not silently dropped.
                if suppress_config_warnings && e.to_string().contains("missing field") {
                    continue;
                }
                warn!("Failed to parse recipe {}: {}", path.display(), e);
            }
        }
    }
}

fn scan_agents_from_dir(
    dir: &Path,
    sources: &mut Vec<SourceEntry>,
    seen: &mut std::collections::HashSet<String>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext != "md" {
            continue;
        }

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to read agent file {}: {}", path.display(), e);
                continue;
            }
        };

        if let Some(source) = parse_agent_content(&content, &path) {
            if !seen.contains(&source.name) {
                seen.insert(source.name.clone());
                sources.push(source);
            }
        }
    }
}

pub fn discover_filesystem_sources(working_dir: &Path) -> Vec<SourceEntry> {
    let mut sources: Vec<SourceEntry> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    let home = dirs::home_dir();
    let config = Paths::config_dir();

    let local_recipe_dirs: Vec<PathBuf> = vec![
        working_dir.join(".goose/recipes"),
        working_dir.join(".agents/recipes"),
    ];

    let global_recipe_dirs: Vec<PathBuf> = std::env::var("GOOSE_RECIPE_PATH")
        .ok()
        .into_iter()
        .flat_map(|p| {
            let sep = if cfg!(windows) { ';' } else { ':' };
            p.split(sep).map(PathBuf::from).collect::<Vec<_>>()
        })
        .chain(
            [
                home.as_ref().map(|h| h.join(".goose/recipes")),
                Some(config.join("recipes")),
                home.as_ref().map(|h| h.join(".agents/recipes")),
            ]
            .into_iter()
            .flatten(),
        )
        .collect();

    let local_agent_dirs: Vec<PathBuf> = vec![
        working_dir.join(".goose/agents"),
        working_dir.join(".claude/agents"),
        working_dir.join(".agents/agents"),
    ];

    let global_agent_dirs: Vec<PathBuf> = [
        home.as_ref().map(|h| h.join(".goose/agents")),
        home.as_ref().map(|h| h.join(".agents/agents")),
        Some(config.join("agents")),
        home.as_ref().map(|h| h.join(".claude/agents")),
    ]
    .into_iter()
    .flatten()
    .collect();

    scan_recipes_from_dir(
        working_dir,
        SourceType::Recipe,
        true,
        &mut sources,
        &mut seen,
    );

    for dir in local_recipe_dirs {
        scan_recipes_from_dir(&dir, SourceType::Recipe, false, &mut sources, &mut seen);
    }

    for dir in local_agent_dirs {
        scan_agents_from_dir(&dir, &mut sources, &mut seen);
    }

    for dir in global_recipe_dirs {
        scan_recipes_from_dir(&dir, SourceType::Recipe, false, &mut sources, &mut seen);
    }

    for dir in global_agent_dirs {
        scan_agents_from_dir(&dir, &mut sources, &mut seen);
    }

    sources
}

fn build_subagent_instructions(session: Option<&crate::session::Session>) -> String {
    let Some(session) = session else {
        return String::new();
    };

    // filter the sources down to what we want even though currently that is what we get
    let mut sources: Vec<SourceEntry> = discover_filesystem_sources(&session.working_dir)
        .into_iter()
        .filter(|s| {
            matches!(
                s.source_type,
                SourceType::Agent | SourceType::Recipe | SourceType::Subrecipe
            )
        })
        .collect();

    // If the session is started from a recipe, also use the subrecipes for
    // that recipe as swarm targets
    if let Some(recipe) = session.recipe.as_ref() {
        if let Some(subs) = recipe.sub_recipes.as_ref() {
            let mut seen: std::collections::HashSet<String> =
                sources.iter().map(|s| s.name.clone()).collect();
            for sr in subs {
                if !seen.insert(sr.name.clone()) {
                    continue;
                }
                sources.push(SourceEntry {
                    source_type: SourceType::Subrecipe,
                    name: sr.name.clone(),
                    description: sr.description.clone().unwrap_or_default(),
                    content: String::new(),
                    path: sr.path.clone(),
                    global: false,
                    writable: false,
                    supporting_files: Vec::new(),
                    properties: std::collections::HashMap::new(),
                });
            }
        }
    }

    if sources.is_empty() {
        return String::new();
    }

    sources.sort_by(|a, b| (&a.source_type, &a.name).cmp(&(&b.source_type, &b.name)));
    let subagents: Vec<&SourceEntry> = sources.iter().collect();

    let names = subagents
        .iter()
        .map(|s| s.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let mut out = String::new();
    out.push_str(
        "\n\nThe following named subagents are available in this session and \
         can be invoked through the `swarm_execute` tool (run as a subagent) \
         or the `load` tool (read their instructions into your own context):\n",
    );

    let mut current_kind: Option<SourceType> = None;
    for s in &subagents {
        if current_kind != Some(s.source_type) {
            out.push_str(&format!("\n{}:", kind_plural(s.source_type)));
            current_kind = Some(s.source_type);
        }
        out.push_str(&format!(
            "\n• {} — {}",
            s.name,
            safe_truncate(&s.description, SUBAGENT_DESCRIPTION_BUDGET)
        ));
    }

    out.push_str(&format!(
        "\n\nWhen to call a subagent (one of [{names}]):\n\
         • `@<name>` in the user's message — always call that subagent.\n\
         • The user mentions a subagent by name without `@` — infer from \
         context whether they want it invoked, and if so, call it.\n\
         • The user's request strongly matches a subagent's description — \
         call it.\n\n\
         Calling a subagent normally means `swarm_execute(source: \"<name>\", \
         task: ...)`, which runs it as an isolated subagent and returns its \
         result. Use `load(source: \"<name>\")` instead if you only want to \
         read the subagent's instructions into your own context. For \
         long-running work, pass `async: true` to `swarm_execute` — it \
         returns a task id immediately, and you collect the result later \
         with `load(source: \"<task_id>\")`, which waits for completion.",
    ));

    out
}

fn round_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{}s", (secs / 10) * 10)
    } else {
        format!("{}m", secs / 60)
    }
}

fn current_epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Get maximum number of concurrent background tasks
fn max_background_tasks() -> usize {
    Config::global()
        .get_param::<usize>("GOOSE_MAX_BACKGROUND_TASKS")
        .unwrap_or(5)
}

fn completed_task_ttl() -> Duration {
    let secs = Config::global()
        .get_param::<u64>("GOOSE_COMPLETED_TASK_TTL_SECS")
        .unwrap_or(600);
    Duration::from_secs(secs)
}

fn is_session_id(s: &str) -> bool {
    let parts: Vec<&str> = s.split('_').collect();
    parts.len() == 2 && parts[0].len() == 8 && parts[0].chars().all(|c| c.is_ascii_digit())
}

pub struct SummonClient {
    info: InitializeResult,
    context: PlatformExtensionContext,
    source_cache: Mutex<Option<(Instant, PathBuf, Vec<SourceEntry>)>>,
    background_tasks: Mutex<HashMap<String, BackgroundTask>>,
    completed_tasks: Mutex<HashMap<String, CompletedTask>>,
    notification_subscribers: Arc<Mutex<Vec<mpsc::Sender<ServerNotification>>>>,
}

impl Drop for SummonClient {
    fn drop(&mut self) {
        // Best-effort cancellation of running tasks on shutdown
        if let Ok(tasks) = self.background_tasks.try_lock() {
            for task in tasks.values() {
                task.cancellation_token.cancel();
            }
        }
    }
}

impl SummonClient {
    pub fn new(context: PlatformExtensionContext) -> Result<Self> {
        let info = InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(EXTENSION_NAME, "1.0.0").with_title("Summon"));

        Ok(Self {
            info,
            context,
            source_cache: Mutex::new(None),
            background_tasks: Mutex::new(HashMap::new()),
            completed_tasks: Mutex::new(HashMap::new()),
            notification_subscribers: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn spawn_notification_bridge(
        mut notif_rx: tokio::sync::mpsc::UnboundedReceiver<ServerNotification>,
        subscribers: Arc<Mutex<Vec<mpsc::Sender<ServerNotification>>>>,
        buffer: Arc<Mutex<Vec<ServerNotification>>>,
    ) {
        tokio::spawn(async move {
            while let Some(notification) = notif_rx.recv().await {
                let mut subs = subscribers.lock().await;
                if subs.is_empty() {
                    drop(subs);
                    buffer.lock().await.push(notification);
                } else {
                    subs.retain(|tx| match tx.try_send(notification.clone()) {
                        Ok(()) => true,
                        Err(mpsc::error::TrySendError::Full(_)) => true,
                        Err(mpsc::error::TrySendError::Closed(_)) => false,
                    });
                }
            }
        });
    }

    fn create_load_tool(&self) -> Tool {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Name of the source to load. If omitted, lists all available sources."
                },
                "cancel": {
                    "type": "boolean",
                    "default": false,
                    "description": "For running background tasks: cancel and return output."
                },
                "peek": {
                    "type": "boolean",
                    "default": false,
                    "description": "For running background tasks: check progress without blocking. Returns turn count, idle time, and recent tool activity."
                }
            }
        });

        Tool::new(
            "load",
            "Load knowledge into your current context or discover available sources.\n\n\
             Call with no arguments to list all available sources (subrecipes, recipes, agents).\n\
             Call with a source name to load its content into your context.\n\
             For background tasks: load(source: \"task_id\") waits for the task and returns the result.\n\
             To cancel a running task: load(source: \"task_id\", cancel: true) stops and returns output.\n\
             To check progress: load(source: \"task_id\", peek: true) returns status without blocking.\n\n\
             Examples:\n\
             - load() → Lists available sources\n\
             - load(source: \"deploy\") → Loads the deploy recipe\n\
             - load(source: \"20260219_1\") → Waits for background task, then returns result\n\
             - load(source: \"20260219_1\", peek: true) → Check task progress without waiting"
                .to_string(),
            schema.as_object().unwrap().clone(),
        )
    }

    fn create_swarm_tool(&self) -> Tool {
        let schema = serde_json::json!({
            "type": "object",
            "required": ["task"],
            "properties": {
                "task": {
                    "type": "string",
                    "description": "The complex task to run as a swarm. Pass the full task verbatim, including any lists, constraints, and reference material — the decomposer only sees this text."
                },
                "subtasks": {
                    "type": "array",
                    "description": "Explicit subtask plan. When provided, the planning LLM call is skipped and this plan is validated and executed as-is. A single entry runs one subagent directly (no planning overhead).",
                    "items": {
                        "type": "object",
                        "properties": {
                            "instruction": {
                                "type": "string",
                                "description": "Self-contained directive for one worker agent. Required unless 'source' is set. Workers are isolated: they see only their own instruction, content, and the outputs of subtasks they depend on."
                            },
                            "source": {
                                "type": "string",
                                "description": "Name of a subrecipe, recipe, or agent to run as this subtask. Combine with 'instruction' to give it a specific task."
                            },
                            "parameters": {
                                "type": "object",
                                "additionalProperties": true,
                                "description": "Template parameters for this subtask's 'source'."
                            },
                            "content": {
                                "type": "string",
                                "description": "Verbatim reference material this subtask needs."
                            },
                            "role": {
                                "type": "string",
                                "description": "Short role label like researcher, coder, reviewer, synthesizer."
                            },
                            "depends_on": {
                                "type": "array",
                                "items": {"type": "integer"},
                                "description": "Zero-based indices of prerequisite subtasks whose outputs this one needs."
                            },
                            "output_schema": {
                                "type": "object",
                                "description": "Optional JSON Schema for this subtask's result when downstream steps need machine-readable data."
                            },
                            "workspace_writes": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Relative repo paths this subtask will create or modify. Two subtasks must never declare the same path."
                            },
                            "required_extensions": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Extension names this subtask needs; omit to inherit all."
                            }
                        }
                    }
                },
                "source": {
                    "type": "string",
                    "description": "Name of a subrecipe, recipe, or agent to run as a single-subtask swarm; 'task' becomes its work instructions. For multi-node plans, use the per-subtask 'source' field inside 'subtasks' instead."
                },
                "parameters": {
                    "type": "object",
                    "additionalProperties": true,
                    "description": "Template parameters for the top-level 'source' (only valid with 'source')."
                },
                "provider": {
                    "type": "string",
                    "description": "Override LLM provider for the swarm's agents."
                },
                "model": {
                    "type": "string",
                    "description": "Override model for the swarm's agents."
                },
                "max_turns": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Maximum turns per subtask agent."
                },
                "subtask_timeout_secs": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Wall-clock timeout per subtask attempt; a timed-out attempt is retried up to the retry budget."
                },
                "max_duration_secs": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Wall-clock circuit breaker for the whole swarm run; when exceeded, no further subtasks dispatch."
                },
                "max_concurrent_subtasks": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "How many independent subtasks may run at once (default: GOOSE_MAX_BACKGROUND_TASKS, 5). Use 1 to force sequential dispatch."
                },
                "async": {
                    "type": "boolean",
                    "default": false,
                    "description": "Run the swarm in the background. Returns a task id immediately; load(source: \"<task_id>\") waits for the report, peek: true checks progress, cancel: true stops the run."
                }
            }
        });

        Tool::new(
            "swarm_execute",
            "Run a task as an orchestrated swarm of subagents — from a single delegated \
             worker to a full dependency graph.\n\n\
             Planning modes:\n\
             1. `task` alone: the task is semantically decomposed into a dependency graph of \
             subtasks, a topology (pipeline, fan-out-join, or general DAG) is selected, and \
             each subtask runs as an independent subagent.\n\
             2. `subtasks`: supply the plan yourself — no planning LLM call. A single entry \
             runs one subagent directly (the replacement for a plain delegated task).\n\
             3. `source`: run a named subrecipe, recipe, or agent as a single-subtask swarm; \
             `task` becomes its work instructions.\n\n\
             Outputs pass between subtasks through a shared scratch pad — each dependent \
             receives exactly its prerequisites' outputs — and the final sink output(s) are \
             returned here.\n\n\
             Effective use:\n\
             - Workers are isolated: each sees only its own instruction, content, and its \
             prerequisites' outputs. Make every instruction self-contained.\n\
             - Workers cannot coordinate beyond declared dependencies. Same-file work = \
             conflicts: partition files via workspace_writes; never two subtasks on one file.\n\
             - Research (read-only) parallelizes freely; write work must be partitioned.\n\
             - For long-running work pass `async: true`, keep working, and collect the report \
             with load(source: \"<task_id>\")."
                .to_string(),
            schema.as_object().unwrap().clone(),
        )
    }

    async fn handle_swarm_execute(
        &self,
        session_id: &str,
        arguments: Option<JsonObject>,
        cancellation_token: CancellationToken,
    ) -> Result<CallToolResult, String> {
        self.cleanup_completed_tasks().await;

        let params: SwarmExecuteParams = arguments
            .map(|args| serde_json::from_value(serde_json::Value::Object(args)))
            .transpose()
            .map_err(|e| format!("Invalid parameters: {}", e))?
            .ok_or("Missing parameters: 'task' is required")?;

        if params.task.trim().is_empty() {
            return Err("'task' must be a non-empty string".to_string());
        }
        if params.source.is_some() && params.subtasks.is_some() {
            return Err(
                "'source' and 'subtasks' cannot be combined; use the per-subtask 'source' \
                 field inside 'subtasks' instead"
                    .to_string(),
            );
        }
        if params.parameters.is_some() && params.source.is_none() {
            return Err(
                "top-level 'parameters' can only be used with a top-level 'source'".to_string(),
            );
        }
        if let Some(max) = params.max_turns {
            if max < 1 {
                return Err("'max_turns' must be at least 1".to_string());
            }
        }
        if let Some(max) = params.max_concurrent_subtasks {
            if max < 1 {
                return Err("'max_concurrent_subtasks' must be at least 1".to_string());
            }
        }

        let session = self
            .context
            .session_manager
            .get_session(session_id, false)
            .await
            .map_err(|e| format!("Failed to get session: {}", e))?;

        if session.session_type == SessionType::SubAgent {
            return Err("Delegated tasks cannot spawn swarms".to_string());
        }

        if params.r#async {
            let (content, task_id) = self.handle_async_swarm(&params, &session).await?;
            let mut meta = Meta::new();
            meta.0.insert(
                "subagent_session_id".to_string(),
                serde_json::Value::String(task_id),
            );
            return Ok(CallToolResult::success(content).with_meta(Some(meta)));
        }

        let (mut kernel, decomposer) = self
            .prepare_swarm(
                &params,
                &session,
                cancellation_token,
                Arc::new(Mutex::new(Vec::new())),
                None,
            )
            .await?;
        let report = kernel
            .run(&params.task, decomposer.as_ref())
            .await
            .map_err(|e| format!("Swarm execution failed: {}", e))?;

        Ok(CallToolResult::success(vec![Content::text(
            render_swarm_report(&report),
        )]))
    }

    /// Build everything a swarm run needs — task config, plan source, workspace,
    /// notification bridge, spawner — shared by the sync and background paths.
    async fn prepare_swarm(
        &self,
        params: &SwarmExecuteParams,
        session: &crate::session::Session,
        cancellation_token: CancellationToken,
        notification_buffer: Arc<Mutex<Vec<ServerNotification>>>,
        on_message: Option<OnMessageCallback>,
    ) -> Result<(Kernel, Box<dyn Decomposer>), String> {
        let subagent_params = SubagentParams {
            instructions: Some(params.task.clone()),
            provider: params.provider.clone(),
            model: params.model.clone(),
            max_turns: params.max_turns,
            ..Default::default()
        };
        let recipe = self.build_adhoc_recipe(&subagent_params)?;
        let task_config = self
            .build_task_config(&subagent_params, &recipe, session)
            .await
            .map_err(|e| format!("Failed to build task config: {}", e))?;

        let available_extensions: Vec<String> =
            task_config.extensions.iter().map(|e| e.name()).collect();
        let explicit_plan = if let Some(source_name) = &params.source {
            let mut st = SubTask::new("subtask-1", params.task.clone());
            st.source = Some(source_name.clone());
            st.parameters = params.parameters.clone();
            Some(vec![st])
        } else {
            params
                .subtasks
                .as_ref()
                .map(|value| parse_explicit_subtasks(value, &available_extensions))
                .transpose()?
        };
        let source_recipes = match &explicit_plan {
            Some(plan) => self.resolve_subtask_sources(plan, session).await?,
            None => HashMap::new(),
        };
        let decomposer: Box<dyn Decomposer> = match explicit_plan {
            Some(plan) => Box::new(FixedPlan(plan)),
            None => Box::new(
                SemanticDecomposer::new(
                    task_config.provider.clone(),
                    task_config.model_config.clone(),
                )
                .with_available_extensions(available_extensions),
            ),
        };

        let workspace_root = session
            .working_dir
            .join(".goose-swarm")
            .join(format!("run-{}", current_epoch_millis()));
        std::fs::create_dir_all(&workspace_root)
            .map_err(|e| format!("Failed to create swarm workspace: {}", e))?;

        let (notif_tx, notif_rx) = tokio::sync::mpsc::unbounded_channel::<ServerNotification>();
        Self::spawn_notification_bridge(
            notif_rx,
            Arc::clone(&self.notification_subscribers),
            notification_buffer,
        );

        let spawner = Arc::new(SummonAgentSpawner {
            session_manager: self.context.session_manager.clone(),
            task_config,
            use_login_shell_path: self.context.use_login_shell_path,
            cancellation_token,
            notification_tx: notif_tx,
            workspace_root,
            on_message,
            source_recipes,
        });

        let mut kernel = Kernel::new(spawner).with_max_concurrency(
            params
                .max_concurrent_subtasks
                .unwrap_or_else(max_background_tasks),
        );
        if let Some(secs) = params.subtask_timeout_secs {
            kernel = kernel.with_subtask_timeout(Duration::from_secs(secs));
        }
        if let Some(secs) = params.max_duration_secs {
            kernel = kernel.with_max_run_duration(Duration::from_secs(secs));
        }
        Ok((kernel, decomposer))
    }

    /// Resolve every subtask's named source to a concrete recipe before any
    /// dispatch, so an unknown name fails the whole run up front instead of
    /// mid-graph. Instructions are NOT folded in here — the spawner appends the
    /// subtask's instruction and dependency context to the recipe prompt.
    async fn resolve_subtask_sources(
        &self,
        subtasks: &[SubTask],
        session: &crate::session::Session,
    ) -> Result<HashMap<String, Recipe>, String> {
        let mut recipes = HashMap::new();
        for st in subtasks {
            if let Some(source_name) = &st.source {
                let subagent_params = SubagentParams {
                    parameters: st.parameters.clone(),
                    ..Default::default()
                };
                let recipe = self
                    .build_source_recipe(
                        source_name,
                        &subagent_params,
                        &session.id,
                        &session.working_dir,
                    )
                    .await?;
                recipes.insert(st.id.clone(), recipe);
            }
        }
        Ok(recipes)
    }

    async fn handle_async_swarm(
        &self,
        params: &SwarmExecuteParams,
        session: &crate::session::Session,
    ) -> Result<(Vec<Content>, String), String> {
        let task_count = self.background_tasks.lock().await.len();
        let max_tasks = max_background_tasks();
        if task_count >= max_tasks {
            return Err(format!(
                "Maximum {} background tasks already running. Wait for completion or use sync mode.",
                max_tasks
            ));
        }

        let turns = Arc::new(AtomicU32::new(0));
        let last_activity = Arc::new(AtomicU64::new(current_epoch_millis()));
        let turns_clone = Arc::clone(&turns);
        let last_activity_clone = Arc::clone(&last_activity);
        let on_message: OnMessageCallback = Arc::new(move |_msg| {
            turns_clone.fetch_add(1, Ordering::Relaxed);
            last_activity_clone.store(current_epoch_millis(), Ordering::Relaxed);
        });

        let task_token = CancellationToken::new();
        let notification_buffer = Arc::new(Mutex::new(Vec::new()));

        let (mut kernel, decomposer) = self
            .prepare_swarm(
                params,
                session,
                task_token.clone(),
                Arc::clone(&notification_buffer),
                Some(on_message),
            )
            .await?;

        static SWARM_TASK_SEQ: AtomicU64 = AtomicU64::new(1);
        let task_id = format!("swarm-{}", SWARM_TASK_SEQ.fetch_add(1, Ordering::Relaxed));
        let description = safe_truncate(&format!("swarm: {}", params.task), TASK_LABEL_BUDGET);

        let task_text = params.task.clone();
        let handle = tokio::spawn(async move {
            let report = kernel.run(&task_text, decomposer.as_ref()).await?;
            Ok(render_swarm_report(&report))
        });

        let task = BackgroundTask {
            id: task_id.clone(),
            description: description.clone(),
            started_at: Instant::now(),
            turns,
            last_activity,
            handle,
            cancellation_token: task_token,
            notification_buffer,
        };
        self.background_tasks
            .lock()
            .await
            .insert(task_id.clone(), task);

        let content = vec![Content::text(format!(
            "Swarm task {} started in background: \"{}\"\n\
             Continue with other work. When you need the report, use load(source: \"{}\").",
            task_id, description, task_id
        ))];
        Ok((content, task_id))
    }

    async fn get_working_dir(&self, session_id: &str) -> PathBuf {
        self.context
            .session_manager
            .get_session(session_id, false)
            .await
            .ok()
            .map(|s| s.working_dir)
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_default())
    }

    async fn get_sources(&self, session_id: &str, working_dir: &Path) -> Vec<SourceEntry> {
        let fs_sources = self.get_filesystem_sources(working_dir).await;

        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut sources: Vec<SourceEntry> = Vec::new();

        self.add_subrecipes(session_id, &mut sources, &mut seen)
            .await;

        for source in fs_sources {
            if !seen.contains(&source.name) {
                seen.insert(source.name.clone());
                sources.push(source);
            }
        }

        sources.sort_by(|a, b| (&a.source_type, &a.name).cmp(&(&b.source_type, &b.name)));
        sources
    }

    async fn get_filesystem_sources(&self, working_dir: &Path) -> Vec<SourceEntry> {
        let mut cache = self.source_cache.lock().await;
        if let Some((cached_at, cached_dir, sources)) = cache.as_ref() {
            if cached_dir == working_dir && cached_at.elapsed() < Duration::from_secs(60) {
                return sources.clone();
            }
        }
        let sources = self.discover_filesystem_sources(working_dir);
        *cache = Some((Instant::now(), working_dir.to_path_buf(), sources.clone()));
        sources
    }

    async fn resolve_source(
        &self,
        session_id: &str,
        name: &str,
        working_dir: &Path,
    ) -> Result<Option<SourceEntry>, String> {
        let sources = self.get_sources(session_id, working_dir).await;

        if let Some(mut source) = sources.iter().find(|s| s.name == name).cloned() {
            if source.source_type == SourceType::Subrecipe && source.content.is_empty() {
                source.content = self.load_subrecipe_content(session_id, &source.name).await;
            }
            return Ok(Some(source));
        }

        Ok(None)
    }

    async fn load_subrecipe_content(&self, session_id: &str, name: &str) -> String {
        let session = match self
            .context
            .session_manager
            .get_session(session_id, false)
            .await
        {
            Ok(s) => s,
            Err(_) => return String::new(),
        };

        let sub_recipes = match session.recipe.as_ref().and_then(|r| r.sub_recipes.as_ref()) {
            Some(sr) => sr,
            None => return String::new(),
        };

        let sr = match sub_recipes.iter().find(|sr| sr.name == name) {
            Some(sr) => sr,
            None => return String::new(),
        };

        match load_local_recipe_file(&sr.path) {
            Ok(recipe_file) => match Recipe::from_content(&recipe_file.content) {
                Ok(recipe) => {
                    let mut content = recipe.instructions.unwrap_or_default();
                    if let Some(params) = &recipe.parameters {
                        if !params.is_empty() {
                            content.push_str("\n\n");
                            content.push_str(&Self::format_parameters(params));
                        }
                    }
                    content
                }
                Err(_) => recipe_file.content,
            },
            Err(_) => String::new(),
        }
    }

    fn discover_filesystem_sources(&self, working_dir: &Path) -> Vec<SourceEntry> {
        discover_filesystem_sources(working_dir)
    }

    async fn add_subrecipes(
        &self,
        session_id: &str,
        sources: &mut Vec<SourceEntry>,
        seen: &mut std::collections::HashSet<String>,
    ) {
        let session = match self
            .context
            .session_manager
            .get_session(session_id, false)
            .await
        {
            Ok(s) => s,
            Err(_) => return,
        };

        let sub_recipes = match session.recipe.as_ref().and_then(|r| r.sub_recipes.as_ref()) {
            Some(sr) => sr,
            None => return,
        };

        for sr in sub_recipes {
            if seen.contains(&sr.name) {
                continue;
            }
            seen.insert(sr.name.clone());

            let description = self.build_subrecipe_description(sr).await;

            sources.push(SourceEntry {
                source_type: SourceType::Subrecipe,
                name: sr.name.clone(),
                description,
                content: String::new(),
                path: sr.path.clone(),
                global: false,
                writable: true,
                supporting_files: Vec::new(),
                properties: std::collections::HashMap::new(),
            });
        }
    }

    async fn build_subrecipe_description(&self, sr: &crate::recipe::SubRecipe) -> String {
        if let Some(desc) = &sr.description {
            return desc.clone();
        }

        if let Ok(recipe_file) = load_local_recipe_file(&sr.path) {
            if let Ok(recipe) = Recipe::from_content(&recipe_file.content) {
                let mut desc = recipe.description.clone();

                if let Some(params) = &recipe.parameters {
                    if !params.is_empty() {
                        desc = format!("{}\n{}", desc, Self::format_parameters(params));
                    }
                }

                return desc;
            }
        }

        format!("Subrecipe from {}", sr.path)
    }

    fn format_parameters(params: &[RecipeParameter]) -> String {
        let mut out = String::from("Parameters:");
        for p in params {
            let mut detail = format!("\n  - {} ({}, {})", p.key, p.input_type, p.requirement);
            if let Some(default) = &p.default {
                detail.push_str(&format!(", default: \"{}\"", default));
            }
            if let Some(options) = &p.options {
                if !options.is_empty() {
                    detail.push_str(&format!(", options: [{}]", options.join(", ")));
                }
            }
            detail.push_str(&format!(": {}", p.description));
            out.push_str(&detail);
        }
        out
    }

    async fn handle_load(
        &self,
        session_id: &str,
        arguments: Option<JsonObject>,
    ) -> Result<CallToolResult, String> {
        self.cleanup_completed_tasks().await;

        let source_name = arguments
            .as_ref()
            .and_then(|args| args.get("source"))
            .and_then(|v| v.as_str());

        let cancel = arguments
            .as_ref()
            .and_then(|args| args.get("cancel"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let peek = arguments
            .as_ref()
            .and_then(|args| args.get("peek"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let working_dir = self.get_working_dir(session_id).await;

        if source_name.is_none() {
            return self
                .handle_load_discovery(session_id, &working_dir)
                .await
                .map(CallToolResult::success);
        }

        let name = source_name.unwrap();

        // Swarm task ids ("swarm-N") don't look like session ids, so also route
        // by registry membership.
        let is_registered_task = self.background_tasks.lock().await.contains_key(name)
            || self.completed_tasks.lock().await.contains_key(name);

        if is_session_id(name) || is_registered_task {
            let task_result = self.handle_load_task_result(name, cancel, peek).await?;
            let mut meta = Meta::new();
            meta.0.insert(
                "subagent_session_id".to_string(),
                serde_json::Value::String(name.to_string()),
            );
            meta.0.insert(
                "task_status".to_string(),
                serde_json::Value::String(task_result.status.to_string()),
            );
            if let Some(turns) = task_result.turns {
                meta.0.insert(
                    "turns_taken".to_string(),
                    serde_json::Value::Number(turns.into()),
                );
            }
            if let Some(secs) = task_result.duration_secs {
                meta.0.insert(
                    "duration_secs".to_string(),
                    serde_json::Value::Number(secs.into()),
                );
            }
            return Ok(CallToolResult::success(task_result.content).with_meta(Some(meta)));
        }

        self.handle_load_source(session_id, name, &working_dir)
            .await
            .map(CallToolResult::success)
    }

    async fn handle_load_task_result(
        &self,
        task_id: &str,
        cancel: bool,
        peek: bool,
    ) -> Result<TaskLoadResult, String> {
        let mut completed = self.completed_tasks.lock().await;

        let completed_entry = if peek {
            completed.get(task_id).map(|task| {
                (
                    task.result.clone(),
                    task.description.clone(),
                    task.duration,
                    task.turns_taken,
                )
            })
        } else {
            completed.remove(task_id).map(|task| {
                (
                    task.result,
                    task.description,
                    task.duration,
                    task.turns_taken,
                )
            })
        };

        if let Some((result, description, duration, turns_taken)) = completed_entry {
            let status_key = match &result {
                Ok(_) => "completed",
                Err(e) if e.starts_with("Task panicked:") => "panicked",
                Err(_) => "failed",
            };
            let status = match status_key {
                "completed" => "✓ Completed",
                "panicked" => "✗ Panicked",
                _ => "✗ Failed",
            };
            let output = match result {
                Ok(output) => output,
                Err(error) => format!("Error: {}", error),
            };
            return Ok(TaskLoadResult {
                content: vec![Content::text(format!(
                    "# Background Task Result: {}\n\n\
                     **Task:** {}\n\
                     **Status:** {}\n\
                     **Duration:** {} ({} turns)\n\n\
                     ## Output\n\n{}",
                    task_id,
                    description,
                    status,
                    round_duration(duration),
                    turns_taken,
                    output
                ))],
                status: status_key,
                turns: Some(turns_taken),
                duration_secs: Some(duration.as_secs()),
            });
        }

        drop(completed);

        let mut running = self.background_tasks.lock().await;
        if running.contains_key(task_id) {
            if peek {
                let task = running.get(task_id).unwrap();
                let elapsed = task.started_at.elapsed();
                let turns_taken = task.turns.load(Ordering::Relaxed);
                let now = current_epoch_millis();
                let idle_ms = now.saturating_sub(task.last_activity.load(Ordering::Relaxed));
                let description = task.description.clone();

                let buffered_count = task.notification_buffer.lock().await.len();

                drop(running);

                let mut output = format!(
                    "# Background Task Status: {}\n\n**Task:** {}\n**Status:** ⏳ Running\n**Elapsed:** {}\n**Turns taken:** {}\n**Idle:** {}\n**Buffered tool calls:** {}",
                    task_id,
                    description,
                    round_duration(elapsed),
                    turns_taken,
                    round_duration(Duration::from_millis(idle_ms)),
                    buffered_count,
                );

                if buffered_count == 0 && turns_taken == 0 {
                    output.push_str("\n\n_Task is initialising (no tool activity yet)._");
                }

                return Ok(TaskLoadResult {
                    content: vec![Content::text(output)],
                    status: "running",
                    turns: Some(turns_taken),
                    duration_secs: Some(elapsed.as_secs()),
                });
            }

            if cancel {
                let task = running.remove(task_id).unwrap();
                drop(running);

                task.cancellation_token.cancel();

                let duration = task.started_at.elapsed();
                let turns_taken = task.turns.load(Ordering::Relaxed);

                let mut handle = task.handle;
                let output = tokio::select! {
                    result = &mut handle => {
                        match result {
                            Ok(Ok(s)) => s,
                            Ok(Err(e)) => format!("Error: {}", e),
                            Err(e) => format!("Task panicked: {}", e),
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        handle.abort();
                        "Task did not stop in time (aborted)".to_string()
                    }
                };

                return Ok(TaskLoadResult {
                    content: vec![Content::text(format!(
                        "# Background Task Result: {}\n\n\
                         **Task:** {}\n\
                         **Status:** ⊘ Cancelled\n\
                         **Duration:** {} ({} turns)\n\n\
                         ## Output\n\n{}",
                        task_id,
                        task.description,
                        round_duration(duration),
                        turns_taken,
                        output
                    ))],
                    status: "cancelled",
                    turns: Some(turns_taken),
                    duration_secs: Some(duration.as_secs()),
                });
            }

            // Wait for the running task to complete, keeping the tool call
            // alive so notifications (subagent tool calls) stream in real time.
            let mut task = running.remove(task_id).unwrap();
            drop(running);

            let buffered = {
                let mut buf = task.notification_buffer.lock().await;
                std::mem::take(&mut *buf)
            };
            if !buffered.is_empty() {
                let subs = self.notification_subscribers.lock().await;
                for notif in buffered {
                    for tx in subs.iter() {
                        let _ = tx.try_send(notif.clone());
                    }
                }
            }

            tokio::select! {
                result = &mut task.handle => {
                    let (output, status_key) = match result {
                        Ok(Ok(s)) => (s, "completed"),
                        Ok(Err(e)) => (format!("Error: {}", e), "failed"),
                        Err(e) => (format!("Task panicked: {}", e), "panicked"),
                    };

                    let turns_taken = task.turns.load(Ordering::Relaxed);
                    let elapsed = task.started_at.elapsed();
                    let status_display = match status_key {
                        "completed" => "✓ Completed",
                        "panicked" => "✗ Panicked",
                        _ => "✗ Failed",
                    };
                    return Ok(TaskLoadResult {
                        content: vec![Content::text(format!(
                            "# Background Task Result: {}\n\n\
                             **Task:** {}\n\
                             **Status:** {}\n\
                             **Duration:** {} ({} turns)\n\n\
                             ## Output\n\n{}",
                            task_id,
                            task.description,
                            status_display,
                            round_duration(elapsed),
                            turns_taken,
                            output
                        ))],
                        status: status_key,
                        turns: Some(turns_taken),
                        duration_secs: Some(elapsed.as_secs()),
                    });
                }
                _ = tokio::time::sleep(Duration::from_secs(300)) => {
                    self.background_tasks.lock().await.insert(task_id.to_string(), task);

                    return Err(format!(
                        "Task '{task_id}' is still running after waiting 5 min. \
                         Use load(source: \"{task_id}\") to wait again, or \
                         load(source: \"{task_id}\", cancel: true) to stop."
                    ));
                }
            }
        }

        Err(format!("Task '{}' not found.", task_id))
    }

    async fn handle_load_discovery(
        &self,
        session_id: &str,
        working_dir: &Path,
    ) -> Result<Vec<Content>, String> {
        {
            let mut cache = self.source_cache.lock().await;
            *cache = None;
        }

        let sources = self.get_sources(session_id, working_dir).await;
        let completed = self.completed_tasks.lock().await;

        if sources.is_empty() && completed.is_empty() {
            return Ok(vec![Content::text(
                "No sources available for load/swarm_execute.\n\n\
                 Sources are discovered from:\n\
                 • Current recipe's sub_recipes\n\
                 • .agents/recipes/, .agents/agents/ (project-level)\n\
                 • ~/.agents/agents/ (global)\n\
                 • GOOSE_RECIPE_PATH directories",
            )]);
        }

        let mut output = String::from("Available sources for load/swarm_execute:\n");

        if !completed.is_empty() {
            output.push_str("\nCompleted Tasks (awaiting retrieval):\n");
            let mut sorted_completed: Vec<_> = completed.values().collect();
            sorted_completed.sort_by_key(|t| &t.id);
            for task in sorted_completed {
                let status = if task.result.is_ok() {
                    "completed"
                } else {
                    "failed"
                };
                output.push_str(&format!(
                    "• {} - \"{}\" ({})\n",
                    task.id, task.description, status
                ));
            }
        }

        for kind in [SourceType::Subrecipe, SourceType::Recipe, SourceType::Agent] {
            let kind_sources: Vec<_> = sources.iter().filter(|s| s.source_type == kind).collect();
            if !kind_sources.is_empty() {
                output.push_str(&format!("\n{}:\n", kind_plural(kind)));
                for source in kind_sources {
                    output.push_str(&format!(
                        "• {} - {}\n",
                        source.name,
                        safe_truncate(&source.description, SUBAGENT_DESCRIPTION_BUDGET)
                    ));
                }
            }
        }

        output.push_str("\nUse load(source: \"name\") to load into context.\n");
        output.push_str("Use swarm_execute(source: \"name\", task: \"...\") to run as subagent.");

        Ok(vec![Content::text(output)])
    }

    async fn handle_load_source(
        &self,
        session_id: &str,
        name: &str,
        working_dir: &Path,
    ) -> Result<Vec<Content>, String> {
        let source = self.resolve_source(session_id, name, working_dir).await?;

        match source {
            Some(source) => {
                let content = source.to_load_text();

                let output = format!(
                    "# Loaded: {} ({})\n\n{}\n\n---\nThis knowledge is now available in your context.",
                    source.name, source.source_type, content
                );

                Ok(vec![Content::text(output)])
            }
            None => {
                let sources = self.get_sources(session_id, working_dir).await;

                let suggestions: Vec<&str> = sources
                    .iter()
                    .filter(|s| {
                        s.name.to_lowercase().contains(&name.to_lowercase())
                            || name.to_lowercase().contains(&s.name.to_lowercase())
                    })
                    .take(3)
                    .map(|s| s.name.as_str())
                    .collect();

                let error_msg = if suggestions.is_empty() {
                    format!(
                        "Source '{}' not found. Use load() to see available sources.",
                        name
                    )
                } else {
                    format!(
                        "Source '{}' not found. Did you mean: {}?",
                        name,
                        suggestions.join(", ")
                    )
                };

                Err(error_msg)
            }
        }
    }

    fn build_adhoc_recipe(&self, params: &SubagentParams) -> Result<Recipe, String> {
        let task = params
            .instructions
            .as_ref()
            .ok_or("Instructions required for ad-hoc task")?;

        Recipe::builder()
            .version("1.0.0")
            .title("Delegated Task")
            .description("Ad-hoc delegated task")
            .prompt(task)
            .build()
            .map_err(|e| format!("Failed to build recipe: {}", e))
    }

    async fn build_source_recipe(
        &self,
        source_name: &str,
        params: &SubagentParams,
        session_id: &str,
        working_dir: &Path,
    ) -> Result<Recipe, String> {
        let source = self
            .resolve_source(session_id, source_name, working_dir)
            .await?
            .ok_or_else(|| format!("Source '{}' not found", source_name))?;

        let mut recipe = match source.source_type {
            SourceType::Recipe | SourceType::Subrecipe => {
                self.build_recipe_from_source(&source, params, session_id)
                    .await?
            }
            SourceType::Agent => self.build_recipe_from_agent(&source, params)?,
            _ => {
                return Err(format!(
                    "Source '{}' has kind '{}' which cannot be delegated from summon",
                    source_name, source.source_type
                ));
            }
        };

        if let Some(extra_instructions) = &params.instructions {
            if recipe.prompt.is_some() {
                let current_prompt = recipe.prompt.take().unwrap();
                recipe.prompt = Some(format!("{}\n\n{}", current_prompt, extra_instructions));
            } else {
                recipe.prompt = Some(extra_instructions.clone());
            }
        }

        Ok(recipe)
    }

    async fn build_recipe_from_source(
        &self,
        source: &SourceEntry,
        params: &SubagentParams,
        session_id: &str,
    ) -> Result<Recipe, String> {
        if source.source_type == SourceType::Subrecipe {
            // Session-declared subrecipes carry preset values to merge; the
            // session is only needed here, so filesystem recipes and agents
            // stay resolvable without one.
            let session = self
                .context
                .session_manager
                .get_session(session_id, false)
                .await
                .ok();
            let sub_recipes = session
                .as_ref()
                .and_then(|s| s.recipe.as_ref())
                .and_then(|r| r.sub_recipes.as_ref());

            if let Some(sub_recipes) = sub_recipes {
                if let Some(sr) = sub_recipes.iter().find(|sr| sr.name == source.name) {
                    let recipe_file = load_local_recipe_file(&sr.path).map_err(|e| {
                        format!("Failed to load subrecipe '{}': {}", source.name, e)
                    })?;

                    let mut merged: HashMap<String, String> = HashMap::new();
                    if let Some(values) = &sr.values {
                        for (k, v) in values {
                            merged.insert(k.clone(), v.clone());
                        }
                    }
                    if let Some(provided_params) = &params.parameters {
                        for (k, v) in provided_params {
                            let value_str = match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            merged.insert(k.clone(), value_str);
                        }
                    }
                    let param_values: Vec<(String, String)> = merged.into_iter().collect();

                    return build_recipe_from_template(
                        recipe_file.content,
                        &recipe_file.parent_dir,
                        param_values,
                        None::<fn(&str, &str) -> Result<String, anyhow::Error>>,
                    )
                    .map_err(|e| format!("Failed to build subrecipe: {}", e));
                }
            }
        }

        let recipe_file = load_local_recipe_file(&source.path)
            .map_err(|e| format!("Failed to load recipe '{}': {}", source.name, e))?;

        let param_values: Vec<(String, String)> = params
            .parameters
            .as_ref()
            .map(|p| {
                p.iter()
                    .map(|(k, v)| {
                        let value_str = match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        (k.clone(), value_str)
                    })
                    .collect()
            })
            .unwrap_or_default();

        build_recipe_from_template(
            recipe_file.content,
            &recipe_file.parent_dir,
            param_values,
            None::<fn(&str, &str) -> Result<String, anyhow::Error>>,
        )
        .map_err(|e| format!("Failed to build recipe: {}", e))
    }

    fn build_recipe_from_agent(
        &self,
        source: &SourceEntry,
        params: &SubagentParams,
    ) -> Result<Recipe, String> {
        let agent_content = if source.path.is_empty() {
            return Err("Agent source has no path".to_string());
        } else {
            std::fs::read_to_string(&source.path)
                .map_err(|e| format!("Failed to read agent file: {}", e))?
        };

        let (metadata, _): (AgentMetadata, String) = parse_frontmatter(&agent_content)
            .map_err(|e| format!("Failed to parse agent frontmatter: {}", e))?
            .ok_or("No frontmatter found in agent file")?;

        let model = metadata.model;

        // max_turns is set later in build_task_config so it can incorporate params.max_turns
        // with the correct priority ordering; setting it here would cause it to be overridden
        // by the parent session's recipe instead.
        let settings = model.map(|m| Settings {
            goose_model: Some(m),
            goose_provider: params.provider.clone(),
            temperature: params.temperature,
            max_turns: None,
        });

        let mut builder = Recipe::builder()
            .version("1.0.0")
            .title(format!("Agent: {}", source.name))
            .description(source.description.clone())
            .instructions(&source.content);

        if let Some(settings) = settings {
            builder = builder.settings(settings);
        }

        if params.instructions.is_none() {
            builder = builder.prompt("Proceed with your expertise to produce a useful result.");
        }

        builder
            .build()
            .map_err(|e| format!("Failed to build recipe from agent: {}", e))
    }

    async fn build_task_config(
        &self,
        params: &SubagentParams,
        recipe: &Recipe,
        session: &crate::session::Session,
    ) -> Result<TaskConfig, anyhow::Error> {
        let (provider, model_config) = self.resolve_provider(params, recipe, session).await?;

        let mut extensions = EnabledExtensionsState::extensions_or_default(
            Some(&session.extension_data),
            Config::global(),
        );

        if let Some(filter) = &params.extensions {
            if filter.is_empty() {
                extensions = Vec::new();
            } else {
                let available_names: Vec<String> =
                    extensions.iter().map(|ext| ext.name()).collect();
                extensions.retain(|ext| filter.contains(&ext.name()));
                let unmatched: Vec<&str> = filter
                    .iter()
                    .filter(|name| !available_names.iter().any(|n| n == *name))
                    .map(String::as_str)
                    .collect();
                if !unmatched.is_empty() {
                    warn!(
                        "Delegate requested extensions not available in session: {:?}. Available: {:?}",
                        unmatched, available_names
                    );
                }
            }
        }

        let max_turns = params
            .max_turns
            .or_else(|| recipe.settings.as_ref().and_then(|s| s.max_turns))
            .unwrap_or_else(|| self.resolve_max_turns(session));

        if max_turns == 0 || max_turns > u32::MAX as usize {
            anyhow::bail!(
                "max_turns must be between 1 and {} (got {})",
                u32::MAX,
                max_turns
            );
        }

        let effective_working_dir = match &params.working_dir {
            Some(dir) => resolve_working_dir(&session.working_dir, dir)?,
            None => session.working_dir.clone(),
        };

        let task_config = TaskConfig::new(
            provider,
            model_config,
            &session.id,
            &effective_working_dir,
            extensions,
        )
        .with_max_turns(Some(max_turns));

        Ok(task_config)
    }

    fn resolve_model_config(
        &self,
        params: &SubagentParams,
        recipe: &Recipe,
        session: &crate::session::Session,
        provider_name: &str,
    ) -> Result<goose_providers::model::ModelConfig, anyhow::Error> {
        let mut model_config = session.model_config.clone().map(Ok).unwrap_or_else(|| {
            crate::model_config::model_config_from_user_config(provider_name, "default")
        })?;

        let override_model = params
            .model
            .clone()
            .or_else(|| recipe.settings.as_ref().and_then(|s| s.goose_model.clone()))
            .or_else(|| {
                Config::global()
                    .get_param::<String>("GOOSE_SUBAGENT_MODEL")
                    .ok()
            });

        if let Some(model) = override_model {
            if model != model_config.model_name {
                // Build the new config from scratch so canonical fields
                // (context_limit, max_tokens, reasoning) and env-derived
                // overrides (GOOSE_CONTEXT_LIMIT, GOOSE_MAX_TOKENS) match the
                // overridden model, then preserve session-level state that is
                // not model-specific from the parent.
                let parent = model_config;
                let mut cfg =
                    crate::model_config::model_config_from_user_config(provider_name, &model)?;
                cfg.toolshim = parent.toolshim;
                cfg.toolshim_model = parent.toolshim_model;
                cfg.temperature = cfg.temperature.or(parent.temperature);
                if let Some(parent_params) = parent.request_params {
                    let merged = cfg.request_params.get_or_insert_with(Default::default);
                    for (k, v) in parent_params {
                        merged.insert(k, v);
                    }
                }
                model_config = cfg;
            }
        }

        if let Some(temp) = params.temperature {
            model_config = model_config.with_temperature(Some(temp));
        } else if let Some(temp) = recipe.settings.as_ref().and_then(|s| s.temperature) {
            model_config = model_config.with_temperature(Some(temp));
        }

        Ok(model_config)
    }

    async fn resolve_provider(
        &self,
        params: &SubagentParams,
        recipe: &Recipe,
        session: &crate::session::Session,
    ) -> Result<
        (
            Arc<dyn crate::providers::base::Provider>,
            goose_providers::model::ModelConfig,
        ),
        anyhow::Error,
    > {
        let provider_name = params
            .provider
            .clone()
            .or_else(|| {
                recipe
                    .settings
                    .as_ref()
                    .and_then(|s| s.goose_provider.clone())
            })
            .or_else(|| {
                Config::global()
                    .get_param::<String>("GOOSE_SUBAGENT_PROVIDER")
                    .ok()
            })
            .or_else(|| session.provider_name.clone())
            .ok_or_else(|| anyhow::anyhow!("No provider configured"))?;

        let model_config = self.resolve_model_config(params, recipe, session, &provider_name)?;
        let provider = providers::create(&provider_name, Vec::new()).await?;
        Ok((provider, model_config))
    }

    fn resolve_max_turns(&self, session: &crate::session::Session) -> usize {
        session
            .recipe
            .as_ref()
            .and_then(|r| r.settings.as_ref())
            .and_then(|s| s.max_turns)
            .or_else(|| {
                std::env::var("GOOSE_SUBAGENT_MAX_TURNS")
                    .ok()
                    .and_then(|v| v.parse().ok())
            })
            .or_else(|| {
                Config::global()
                    .get_param::<usize>("GOOSE_SUBAGENT_MAX_TURNS")
                    .ok()
            })
            .unwrap_or(DEFAULT_SUBAGENT_MAX_TURNS)
    }

    async fn cleanup_completed_tasks(&self) {
        let finished: Vec<(String, BackgroundTask)> = {
            let mut tasks = self.background_tasks.lock().await;
            let ids: Vec<String> = tasks
                .iter()
                .filter(|(_, t)| t.handle.is_finished())
                .map(|(id, _)| id.clone())
                .collect();
            ids.into_iter()
                .filter_map(|id| tasks.remove(&id).map(|t| (id, t)))
                .collect()
        };

        let mut completed = self.completed_tasks.lock().await;

        for (id, task) in finished {
            let duration = task.started_at.elapsed();
            let turns_taken = task.turns.load(Ordering::Relaxed);

            let result = match task.handle.await {
                Ok(Ok(output)) => {
                    info!("Background task {} completed successfully", id);
                    Ok(output)
                }
                Ok(Err(e)) => {
                    warn!("Background task {} failed: {}", id, e);
                    Err(e.to_string())
                }
                Err(e) => {
                    warn!("Background task {} panicked: {}", id, e);
                    Err(format!("Task panicked: {}", e))
                }
            };

            completed.insert(
                id.clone(),
                CompletedTask {
                    id,
                    description: task.description,
                    result,
                    turns_taken,
                    duration,
                    completed_at: Instant::now(),
                },
            );
        }

        let ttl = completed_task_ttl();
        completed.retain(|_id, task| task.completed_at.elapsed() <= ttl);
    }
}

/// [`AgentSpawner`] backed by Goose's real subagent machinery: each subtask runs
/// as its own ephemeral subagent session via `run_subagent_task`, inheriting the
/// parent session's provider, extensions, and working directory.
struct SummonAgentSpawner {
    session_manager: Arc<crate::session::SessionManager>,
    task_config: TaskConfig,
    use_login_shell_path: bool,
    cancellation_token: CancellationToken,
    notification_tx: tokio::sync::mpsc::UnboundedSender<ServerNotification>,
    workspace_root: PathBuf,
    /// Progress callback shared across all subtasks; background runs use it to
    /// track aggregate turn count and last-activity time for peek.
    on_message: Option<OnMessageCallback>,
    /// Pre-resolved recipes for subtasks that run a named source, keyed by
    /// subtask id. Subtasks not in this map run as ad-hoc workers.
    source_recipes: HashMap<String, Recipe>,
}

impl SummonAgentSpawner {
    /// Least-privilege extension scoping: keep only the subtask's declared
    /// extensions, failing open (full inheritance) when the declaration matches
    /// nothing real — a planner typo must not strand a subtask without tools.
    fn scoped_task_config(&self, subtask: &SubTask) -> TaskConfig {
        let mut task_config = self.task_config.clone();
        if !subtask.required_extensions.is_empty() {
            let filtered: Vec<_> = task_config
                .extensions
                .iter()
                .filter(|e| subtask.required_extensions.iter().any(|r| *r == e.name()))
                .cloned()
                .collect();
            if filtered.is_empty() {
                warn!(
                    "subtask {} required extensions {:?} matched none of the session's; \
                     inheriting all",
                    subtask.id, subtask.required_extensions
                );
            } else {
                task_config.extensions = filtered;
            }
        }
        task_config
    }
}

fn collect_artifact_files(dir: &Path) -> Vec<String> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        if let Ok(entries) = std::fs::read_dir(&current) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    files.push(path.to_string_lossy().replace('\\', "/"));
                }
            }
        }
    }
    files.sort();
    files
}

#[async_trait]
impl AgentSpawner for SummonAgentSpawner {
    async fn run_subtask(
        &self,
        subtask: &SubTask,
        agent: &SwarmAgentConfig,
        dependency_context: &str,
    ) -> Result<SubtaskResult> {
        let artifact_dir = self.workspace_root.join(&subtask.id);
        if let Err(e) = std::fs::create_dir_all(&artifact_dir) {
            warn!("failed to create artifact dir for {}: {}", subtask.id, e);
        }

        let mut prompt = subtask.instruction.clone();
        if !subtask.content.is_empty() {
            prompt.push_str("\n\nReference material:\n");
            prompt.push_str(&subtask.content);
        }
        if !dependency_context.is_empty() {
            prompt.push_str("\n\nOutputs from prerequisite subtasks:\n");
            prompt.push_str(dependency_context);
        }
        prompt.push_str(&format!(
            "\n\nArtifact directory: {}\nWrite any file deliverables (scripts, code, data \
             files) into that directory so downstream steps can use them.",
            artifact_dir.to_string_lossy().replace('\\', "/")
        ));
        if subtask.workspace_writes.is_empty() {
            prompt.push_str(
                "\nDo not create or modify repository files unless your instruction \
                 explicitly requires it.",
            );
        } else {
            prompt.push_str(&format!(
                "\nYour declared write lanes in the repository are: {}. Do not write \
                 repository paths outside these lanes.",
                subtask.workspace_writes.join(", ")
            ));
        }

        let mut recipe = match self.source_recipes.get(&subtask.id) {
            Some(base) => {
                // The source recipe keeps its own instructions/settings; the
                // swarm-specific material (subtask instruction, dependency
                // context, artifact guidance) extends its prompt.
                let mut recipe = base.clone();
                let mut base_prompt = recipe.prompt.take().unwrap_or_default();
                if !base_prompt.is_empty() {
                    base_prompt.push_str("\n\n");
                }
                base_prompt.push_str(&prompt);
                recipe.prompt = Some(base_prompt);
                recipe
            }
            None => Recipe::builder()
                .version("1.0.0")
                .title(format!("Swarm subtask {}", subtask.id))
                .description(format!("Swarm {} subtask", agent.role))
                .instructions(&agent.system_prompt)
                .prompt(&prompt)
                .build()
                .map_err(|e| anyhow::anyhow!("Failed to build swarm subtask recipe: {}", e))?,
        };
        recipe.response = Some(Response {
            json_schema: Some(envelope_schema_for(subtask.output_schema.as_ref())),
        });

        let agent_config = AgentConfig::new(
            self.session_manager.clone(),
            crate::config::permission::PermissionManager::instance(),
            None,
            GooseMode::Auto,
            true, // disable session naming for subagents
            crate::agents::GoosePlatform::GooseCli,
        )
        .with_use_login_shell_path(self.use_login_shell_path);

        let subagent_session = self
            .session_manager
            .create_session(
                self.task_config.parent_working_dir.clone(),
                safe_truncate(
                    &format!("Swarm {}: {}", subtask.id, subtask.instruction),
                    TASK_LABEL_BUDGET,
                ),
                SessionType::SubAgent,
                GooseMode::Auto,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create subagent session: {}", e))?;

        let raw = run_subagent_task(SubagentRunParams {
            config: agent_config,
            recipe,
            task_config: self.scoped_task_config(subtask),
            return_last_only: true,
            session_id: subagent_session.id,
            cancellation_token: Some(self.cancellation_token.child_token()),
            on_message: self.on_message.clone(),
            notification_tx: Some(self.notification_tx.clone()),
        })
        .await?;

        match interpret_subtask_output(&raw) {
            SubtaskOutcome::Success(output) => Ok(SubtaskResult {
                output,
                artifacts: collect_artifact_files(&artifact_dir),
            }),
            SubtaskOutcome::Failed(reason) => Err(anyhow::anyhow!(
                "subtask {} reported failure: {}",
                subtask.id,
                reason
            )),
        }
    }

    fn prompt_token_budget(&self) -> Option<usize> {
        Some(usable_budget(self.task_config.model_config.context_limit()))
    }
}

fn render_swarm_report(report: &SwarmReport) -> String {
    let mut out = format!(
        "Swarm run complete: {} subtask(s), topology={}, {} dispatched, {} failed.\n",
        report.subtasks.len(),
        report.topology.as_str(),
        report.dispatches,
        report.failures.len(),
    );
    for st in &report.subtasks {
        let status = if report.outputs.contains_key(&st.id) {
            "ok"
        } else if report.failures.iter().any(|f| f.subtask_id == st.id) {
            "FAILED"
        } else {
            "blocked"
        };
        out.push_str(&format!(
            "  [{}] {} — {}\n",
            status,
            st.id,
            safe_truncate(&st.instruction, SUBAGENT_DESCRIPTION_BUDGET)
        ));
    }
    for f in &report.failures {
        out.push_str(&format!(
            "\nFailure in {} after {} attempt(s): {}\n",
            f.subtask_id, f.attempts, f.error
        ));
    }
    if let Some(reason) = &report.halted {
        out.push_str(&format!("\nRun halted early: {}\n", reason));
    }
    if !report.artifacts.is_empty() {
        out.push_str("\nArtifact files produced:\n");
        for (subtask_id, paths) in &report.artifacts {
            for path in paths {
                out.push_str(&format!("  {} — {}\n", subtask_id, path));
            }
        }
    }
    if report.final_output.is_empty() {
        out.push_str("\nNo final output was produced.");
    } else {
        out.push_str("\n=== Final output ===\n");
        out.push_str(&report.final_output);
    }
    out
}

#[async_trait]
impl McpClientTrait for SummonClient {
    async fn list_tools(
        &self,
        session_id: &str,
        _next_cursor: Option<String>,
        _cancellation_token: CancellationToken,
    ) -> Result<ListToolsResult, Error> {
        self.cleanup_completed_tasks().await;

        let is_subagent = self
            .context
            .session_manager
            .get_session(session_id, false)
            .await
            .map(|s| s.session_type == SessionType::SubAgent)
            .unwrap_or(false);

        let mut tools = vec![self.create_load_tool()];

        if !is_subagent {
            tools.push(self.create_swarm_tool());
        }

        Ok(ListToolsResult {
            tools,
            next_cursor: None,
            meta: None,
        })
    }

    async fn call_tool(
        &self,
        ctx: &ToolCallContext,
        name: &str,
        arguments: Option<JsonObject>,
        cancellation_token: CancellationToken,
    ) -> Result<CallToolResult, Error> {
        let session_id = &ctx.session_id;
        match name {
            "load" => match self.handle_load(session_id, arguments).await {
                Ok(result) => Ok(result),
                Err(error) => Ok(CallToolResult::error(vec![Content::text(format!(
                    "Error: {}",
                    error
                ))])),
            },
            "swarm_execute" => {
                match self
                    .handle_swarm_execute(session_id, arguments, cancellation_token)
                    .await
                {
                    Ok(result) => Ok(result),
                    Err(error) => Ok(CallToolResult::error(vec![Content::text(format!(
                        "Error: {}",
                        error
                    ))])),
                }
            }
            _ => Ok(CallToolResult::error(vec![Content::text(format!(
                "Error: Unknown tool: {}",
                name
            ))])),
        }
    }

    fn get_info(&self) -> Option<&InitializeResult> {
        Some(&self.info)
    }

    fn get_instructions(&self) -> Option<String> {
        let instructions = build_subagent_instructions(self.context.session.as_deref());
        if instructions.is_empty() {
            None
        } else {
            Some(instructions)
        }
    }

    async fn subscribe(&self) -> mpsc::Receiver<ServerNotification> {
        let (tx, rx) = mpsc::channel(16);
        self.notification_subscribers.lock().await.push(tx);
        rx
    }

    async fn get_moim(&self, _session_id: &str) -> Option<String> {
        self.cleanup_completed_tasks().await;

        let running = self.background_tasks.lock().await;
        let completed = self.completed_tasks.lock().await;

        if running.is_empty() && completed.is_empty() {
            return None;
        }

        let mut lines = vec!["Background tasks:".to_string()];
        let now = current_epoch_millis();

        let mut sorted_running: Vec<_> = running.values().collect();
        sorted_running.sort_by_key(|t| &t.id);

        for task in sorted_running {
            let elapsed = task.started_at.elapsed();
            let idle_ms = now.saturating_sub(task.last_activity.load(Ordering::Relaxed));

            lines.push(format!(
                "• {}: \"{}\" - running {}, {} turns, idle {}",
                task.id,
                task.description,
                round_duration(elapsed),
                task.turns.load(Ordering::Relaxed),
                round_duration(Duration::from_millis(idle_ms)),
            ));
        }

        let mut sorted_completed: Vec<_> = completed.values().collect();
        sorted_completed.sort_by_key(|t| &t.id);

        for task in sorted_completed {
            let status = if task.result.is_ok() {
                "completed"
            } else {
                "failed"
            };
            lines.push(format!(
                "• {}: \"{}\" - {} in {} ({} turns) - use load(\"{}\") to get result",
                task.id,
                task.description,
                status,
                round_duration(task.duration),
                task.turns_taken,
                task.id
            ));
        }

        if !running.is_empty() {
            lines.push(
                "\n→ Use load(source: \"<id>\") to wait for a task, or load(source: \"<id>\", cancel: true) to stop it"
                    .to_string(),
            );
        }

        Some(lines.join("\n"))
    }
}

/// Resolve a requested `working_dir` override against the parent session
/// directory. Relative paths are joined to the parent dir; the result must
/// canonicalize to an existing directory contained within the parent dir.
fn resolve_working_dir(parent_dir: &Path, requested: &str) -> Result<PathBuf, anyhow::Error> {
    let requested_path = PathBuf::from(requested);
    let resolved = if requested_path.is_absolute() {
        requested_path
    } else {
        parent_dir.join(&requested_path)
    };
    let canonical = resolved
        .canonicalize()
        .map_err(|e| anyhow::anyhow!("working_dir '{}' could not be resolved: {}", requested, e))?;
    let parent_canonical = parent_dir
        .canonicalize()
        .unwrap_or_else(|_| parent_dir.to_path_buf());
    if !canonical.starts_with(&parent_canonical) {
        anyhow::bail!(
            "working_dir '{}' is outside the parent session directory",
            requested
        );
    }
    if !canonical.is_dir() {
        anyhow::bail!("working_dir '{}' is not a directory", requested);
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_context() -> PlatformExtensionContext {
        PlatformExtensionContext {
            extension_manager: None,
            session_manager: Arc::new(crate::session::SessionManager::instance()),
            session: None,
            use_login_shell_path: false,
        }
    }

    #[test]
    fn test_agent_frontmatter_parsing() {
        let agent = r#"---
name: reviewer
model: sonnet
---
You review code."#;
        let source = parse_agent_content(agent, Path::new("")).unwrap();
        assert_eq!(source.name, "reviewer");
        assert!(source.description.contains("sonnet"));
    }

    #[test]
    fn test_resolve_working_dir_relative_subdir() {
        let temp_dir = TempDir::new().unwrap();
        let parent = temp_dir.path().canonicalize().unwrap();
        let subdir = parent.join("sub");
        fs::create_dir(&subdir).unwrap();

        let resolved = resolve_working_dir(&parent, "sub").unwrap();
        assert_eq!(resolved, subdir.canonicalize().unwrap());
    }

    #[test]
    fn test_resolve_working_dir_rejects_traversal_outside_parent() {
        let temp_dir = TempDir::new().unwrap();
        let parent = temp_dir.path().join("parent");
        let sibling = temp_dir.path().join("sibling");
        fs::create_dir(&parent).unwrap();
        fs::create_dir(&sibling).unwrap();

        let err = resolve_working_dir(&parent, "../sibling").unwrap_err();
        assert!(
            err.to_string()
                .contains("outside the parent session directory"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_resolve_working_dir_rejects_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let parent = temp_dir.path().canonicalize().unwrap();
        let file = parent.join("a.txt");
        fs::write(&file, "hello").unwrap();

        let err = resolve_working_dir(&parent, "a.txt").unwrap_err();
        assert!(
            err.to_string().contains("is not a directory"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_resolve_working_dir_rejects_nonexistent_path() {
        let temp_dir = TempDir::new().unwrap();
        let parent = temp_dir.path().canonicalize().unwrap();

        let err = resolve_working_dir(&parent, "does-not-exist").unwrap_err();
        assert!(
            err.to_string().contains("could not be resolved"),
            "unexpected error: {err}"
        );
    }
    #[test]
    fn test_agent_scan_skips_non_agent_markdown() {
        let temp_dir = TempDir::new().unwrap();
        let agents_dir = temp_dir.path().join("agents");
        fs::create_dir_all(&agents_dir).unwrap();
        fs::write(
            agents_dir.join("README.md"),
            "---\ntitle: Notes\n---\nThis is not an agent.",
        )
        .unwrap();
        fs::write(
            agents_dir.join("notes.md"),
            "---\nauthor: someone\ntags: [docs]\n---\nJust documentation.",
        )
        .unwrap();
        fs::write(
            agents_dir.join("reviewer.md"),
            "---\nname: reviewer\nmodel: sonnet\n---\nYou review code.",
        )
        .unwrap();
        fs::write(agents_dir.join("plain.md"), "No frontmatter at all.").unwrap();
        fs::write(
            agents_dir.join("broken.md"),
            "---\nname: [unterminated\n---\nBroken YAML.",
        )
        .unwrap();

        let mut sources = Vec::new();
        let mut seen = HashSet::new();
        scan_agents_from_dir(&agents_dir, &mut sources, &mut seen);

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name, "reviewer");
    }

    #[test]
    fn test_recipe_scan_skips_non_recipe_project_config_files() {
        let temp_dir = TempDir::new().unwrap();
        fs::write(
            temp_dir.path().join("package.json"),
            r#"{"scripts":{"test":"cargo test"}}"#,
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("tsconfig.json"),
            r#"{"compilerOptions":{"strict":true}}"#,
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("valid.yaml"),
            "title: Valid\ndescription: Real recipe\ninstructions: Run valid steps",
        )
        .unwrap();

        let mut sources = Vec::new();
        let mut seen = HashSet::new();
        scan_recipes_from_dir(
            temp_dir.path(),
            SourceType::Recipe,
            true,
            &mut sources,
            &mut seen,
        );

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].name, "valid");
        assert_eq!(sources[0].description, "Real recipe");
    }

    #[tokio::test]
    async fn test_discover_recipes_and_agents() {
        let temp_dir = TempDir::new().unwrap();

        let recipes = temp_dir.path().join(".goose/recipes");
        fs::create_dir_all(&recipes).unwrap();
        fs::write(
            recipes.join("deploy.yaml"),
            "title: Deploy\ndescription: Deploy to production\ninstructions: Run deploy steps",
        )
        .unwrap();

        let agents = temp_dir.path().join(".goose/agents");
        fs::create_dir_all(&agents).unwrap();
        fs::write(
            agents.join("reviewer.md"),
            "---\nname: reviewer\nmodel: sonnet\ndescription: Code reviewer\n---\nYou review code.",
        )
        .unwrap();

        let client = SummonClient::new(create_test_context()).unwrap();
        let sources = client.discover_filesystem_sources(temp_dir.path());

        let recipe = sources
            .iter()
            .find(|s| s.name == "deploy" && s.source_type == SourceType::Recipe)
            .unwrap();
        assert_eq!(recipe.description, "Deploy to production");
        assert_eq!(recipe.content, "Run deploy steps");

        let agent = sources
            .iter()
            .find(|s| s.name == "reviewer" && s.source_type == SourceType::Agent)
            .unwrap();
        assert_eq!(agent.description, "Code reviewer");
        assert!(agent.content.contains("You review code"));
    }

    #[tokio::test]
    async fn test_recipe_deduplication_local_wins() {
        let temp_dir = TempDir::new().unwrap();

        let local = temp_dir.path().join(".goose/recipes");
        fs::create_dir_all(&local).unwrap();
        fs::write(
            local.join("deploy.yaml"),
            "title: Deploy\ndescription: Local deploy\ninstructions: local steps",
        )
        .unwrap();

        let also_local = temp_dir.path().join(".agents/recipes");
        fs::create_dir_all(&also_local).unwrap();
        fs::write(
            also_local.join("deploy.yaml"),
            "title: Deploy\ndescription: Agents deploy\ninstructions: agents steps",
        )
        .unwrap();

        let client = SummonClient::new(create_test_context()).unwrap();
        let sources = client.discover_filesystem_sources(temp_dir.path());

        let deploys: Vec<_> = sources.iter().filter(|s| s.name == "deploy").collect();
        assert_eq!(deploys.len(), 1);
    }

    #[tokio::test]
    async fn test_load_recipe_source() {
        let temp_dir = TempDir::new().unwrap();

        let recipes = temp_dir.path().join(".goose/recipes");
        fs::create_dir_all(&recipes).unwrap();
        fs::write(
            recipes.join("deploy.yaml"),
            "title: Deploy\ndescription: Deploy to production\ninstructions: Run deploy steps",
        )
        .unwrap();

        let client = SummonClient::new(create_test_context()).unwrap();
        let result = client
            .handle_load_source("test", "deploy", temp_dir.path())
            .await
            .unwrap();

        let text = &result[0].as_text().expect("expected text content").text;
        assert!(text.contains("deploy"));
        assert!(text.contains("Run deploy steps"));
        assert!(text.contains("now available in your context"));
    }

    #[tokio::test]
    async fn test_load_agent_source() {
        let temp_dir = TempDir::new().unwrap();

        let agents = temp_dir.path().join(".goose/agents");
        fs::create_dir_all(&agents).unwrap();
        fs::write(
            agents.join("reviewer.md"),
            "---\nname: reviewer\nmodel: sonnet\ndescription: Code reviewer\n---\nYou review code carefully.",
        )
        .unwrap();

        let client = SummonClient::new(create_test_context()).unwrap();
        let result = client
            .handle_load_source("test", "reviewer", temp_dir.path())
            .await
            .unwrap();

        let text = &result[0].as_text().expect("expected text content").text;
        assert!(text.contains("reviewer"));
        assert!(text.contains("You review code carefully"));
        assert!(text.contains("now available in your context"));
    }

    #[tokio::test]
    async fn test_load_nonexistent_source_suggests_similar() {
        let temp_dir = TempDir::new().unwrap();

        let recipes = temp_dir.path().join(".goose/recipes");
        fs::create_dir_all(&recipes).unwrap();
        fs::write(
            recipes.join("deploy.yaml"),
            "title: Deploy\ndescription: Deploy to production\ninstructions: steps",
        )
        .unwrap();

        let client = SummonClient::new(create_test_context()).unwrap();
        let err = client
            .handle_load_source("test", "deploy-prod", temp_dir.path())
            .await
            .unwrap_err();

        assert!(err.contains("not found"));
        assert!(err.contains("deploy"), "should suggest 'deploy': {}", err);
    }

    #[tokio::test]
    async fn test_load_completely_unknown_source() {
        let temp_dir = TempDir::new().unwrap();

        let client = SummonClient::new(create_test_context()).unwrap();
        let err = client
            .handle_load_source("test", "zzz-nonexistent", temp_dir.path())
            .await
            .unwrap_err();

        assert!(err.contains("not found"));
        assert!(err.contains("Use load()"));
    }

    #[tokio::test]
    async fn test_client_tools_and_unknown_tool() {
        let client = SummonClient::new(create_test_context()).unwrap();

        let result = client
            .list_tools("test", None, CancellationToken::new())
            .await
            .unwrap();
        let names: Vec<_> = result.tools.iter().map(|t| t.name.as_ref()).collect();
        assert!(names.contains(&"load") && names.contains(&"swarm_execute"));
        assert!(
            !names.contains(&"delegate"),
            "the delegate tool surface is retired"
        );

        let ctx = ToolCallContext::new("test".to_string(), None, None);
        let result = client
            .call_tool(&ctx, "unknown", None, CancellationToken::new())
            .await
            .unwrap();
        assert!(result.is_error.unwrap_or(false));
    }

    #[test]
    fn test_duration_rounding_for_moim() {
        assert_eq!(round_duration(Duration::from_secs(5)), "0s");
        assert_eq!(round_duration(Duration::from_secs(15)), "10s");
        assert_eq!(round_duration(Duration::from_secs(59)), "50s");

        assert_eq!(round_duration(Duration::from_secs(60)), "1m");
        assert_eq!(round_duration(Duration::from_secs(90)), "1m");
        assert_eq!(round_duration(Duration::from_secs(120)), "2m");
    }

    #[test]
    #[serial]
    fn test_resolve_max_turns_recipe_overrides_env_var() {
        let context = create_test_context();
        let client = SummonClient::new(context).unwrap();

        let session = crate::session::Session {
            recipe: Some(crate::recipe::Recipe {
                version: "1.0.0".to_string(),
                title: String::new(),
                description: String::new(),
                instructions: None,
                prompt: None,
                extensions: None,
                settings: Some(crate::recipe::Settings {
                    goose_provider: None,
                    goose_model: None,
                    temperature: None,
                    max_turns: Some(10),
                }),
                activities: None,
                author: None,
                parameters: None,
                response: None,
                sub_recipes: None,
                retry: None,
            }),
            ..Default::default()
        };

        // Set env var to a different value — recipe should still win
        std::env::set_var("GOOSE_SUBAGENT_MAX_TURNS", "99");
        let result = client.resolve_max_turns(&session);
        std::env::remove_var("GOOSE_SUBAGENT_MAX_TURNS");

        assert_eq!(
            result, 10,
            "recipe settings.max_turns should take priority over env var"
        );
    }

    #[test]
    #[serial]
    fn test_resolve_max_turns_falls_back_to_env_var() {
        let context = create_test_context();
        let client = SummonClient::new(context).unwrap();

        let session = crate::session::Session::default(); // no recipe

        std::env::set_var("GOOSE_SUBAGENT_MAX_TURNS", "7");
        let result = client.resolve_max_turns(&session);
        std::env::remove_var("GOOSE_SUBAGENT_MAX_TURNS");

        assert_eq!(
            result, 7,
            "should fall back to GOOSE_SUBAGENT_MAX_TURNS env var"
        );
    }

    #[test]
    #[serial]
    fn test_resolve_max_turns_falls_back_to_default() {
        let context = create_test_context();
        let client = SummonClient::new(context).unwrap();

        let session = crate::session::Session::default(); // no recipe

        std::env::remove_var("GOOSE_SUBAGENT_MAX_TURNS");
        let result = client.resolve_max_turns(&session);

        assert_eq!(
            result,
            crate::agents::subagent_task_config::DEFAULT_SUBAGENT_MAX_TURNS,
            "should fall back to DEFAULT_SUBAGENT_MAX_TURNS"
        );
    }

    fn empty_recipe() -> crate::recipe::Recipe {
        crate::recipe::Recipe {
            version: "1.0.0".to_string(),
            title: String::new(),
            description: String::new(),
            instructions: None,
            prompt: None,
            extensions: None,
            settings: None,
            activities: None,
            author: None,
            parameters: None,
            response: None,
            sub_recipes: None,
            retry: None,
        }
    }

    const PARENT_MODEL: &str = "claude-3-5-sonnet-20241022";
    const OVERRIDE_MODEL: &str = "claude-opus-4-6";
    const PROVIDER: &str = "anthropic";

    fn session_with(parent: goose_providers::model::ModelConfig) -> crate::session::Session {
        crate::session::Session {
            provider_name: Some(PROVIDER.to_string()),
            model_config: Some(parent),
            ..Default::default()
        }
    }

    fn resolve_with_override(
        model: Option<&str>,
        parent: goose_providers::model::ModelConfig,
    ) -> goose_providers::model::ModelConfig {
        let client = SummonClient::new(create_test_context()).unwrap();
        let params = SubagentParams {
            model: model.map(String::from),
            ..Default::default()
        };
        client
            .resolve_model_config(&params, &empty_recipe(), &session_with(parent), PROVIDER)
            .expect("resolve_model_config")
    }

    fn parent_config() -> goose_providers::model::ModelConfig {
        goose_providers::model::ModelConfig::new(PARENT_MODEL).with_canonical_limits(PROVIDER)
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_model_config_applies_canonical_limits_to_overridden_model() {
        let _env = env_lock::lock_env([
            ("GOOSE_CONTEXT_LIMIT", None::<&str>),
            ("GOOSE_MAX_TOKENS", None::<&str>),
            ("GOOSE_SUBAGENT_MODEL", None::<&str>),
        ]);

        let parent = parent_config();
        let overridden = goose_providers::model::ModelConfig::new(OVERRIDE_MODEL)
            .with_canonical_limits(PROVIDER);
        assert_ne!(parent.context_limit, overridden.context_limit);
        assert_ne!(parent.reasoning, overridden.reasoning);

        let resolved = resolve_with_override(Some(OVERRIDE_MODEL), parent);

        assert_eq!(resolved.model_name, OVERRIDE_MODEL);
        assert_eq!(resolved.context_limit, overridden.context_limit);
        assert_eq!(resolved.max_tokens, overridden.max_tokens);
        assert_eq!(resolved.reasoning, overridden.reasoning);
    }

    #[tokio::test]
    #[serial]
    async fn test_resolve_model_config_preserves_parent_request_params_on_override() {
        let _env = env_lock::lock_env([
            ("GOOSE_CONTEXT_LIMIT", None::<&str>),
            ("GOOSE_MAX_TOKENS", None::<&str>),
            ("GOOSE_SUBAGENT_MODEL", None::<&str>),
        ]);

        let mut parent = parent_config();
        parent.request_params = Some(HashMap::from([(
            "anthropic_beta".to_string(),
            serde_json::json!("custom-beta-header"),
        )]));

        let resolved = resolve_with_override(Some(OVERRIDE_MODEL), parent);

        assert_eq!(
            resolved
                .request_params
                .as_ref()
                .and_then(|p| p.get("anthropic_beta")),
            Some(&serde_json::json!("custom-beta-header")),
        );
    }

    fn extract_text(content: &Content) -> &str {
        use rmcp::model::RawContent;
        match &content.raw {
            RawContent::Text(t) => t.text.as_str(),
            _ => panic!("Expected text content"),
        }
    }

    #[test]
    fn test_parse_explicit_subtasks_valid_plan() {
        let value = serde_json::json!([
            {"instruction": "research the topic", "role": "researcher"},
            {"instruction": "write the summary", "depends_on": [0]}
        ]);
        let plan = parse_explicit_subtasks(&value, &[]).unwrap();
        assert_eq!(plan.len(), 2);
        assert_eq!(plan[0].id, "subtask-1");
        assert_eq!(plan[1].dependencies, vec!["subtask-1".to_string()]);
    }

    #[test]
    fn test_parse_explicit_subtasks_single_entry_is_valid() {
        let value = serde_json::json!([{"instruction": "do the one thing"}]);
        let plan = parse_explicit_subtasks(&value, &[]).unwrap();
        assert_eq!(plan.len(), 1);
        assert!(plan[0].dependencies.is_empty());
    }

    #[test]
    fn test_parse_explicit_subtasks_source_only_entry() {
        let value = serde_json::json!([
            {"source": "deploy", "parameters": {"env": "staging"}},
            {"instruction": "summarize the deploy output", "depends_on": [0]}
        ]);
        let plan = parse_explicit_subtasks(&value, &[]).unwrap();
        assert_eq!(plan[0].source.as_deref(), Some("deploy"));
        assert!(plan[0].instruction.contains("deploy"));
        assert_eq!(
            plan[0].parameters.as_ref().unwrap().get("env"),
            Some(&serde_json::json!("staging"))
        );
        assert!(plan[1].source.is_none());
    }

    #[tokio::test]
    async fn test_resolve_subtask_sources() {
        let temp_dir = TempDir::new().unwrap();
        let recipes = temp_dir.path().join(".goose/recipes");
        fs::create_dir_all(&recipes).unwrap();
        fs::write(
            recipes.join("deploy.yaml"),
            "title: Deploy\ndescription: Deploy to production\ninstructions: Run deploy steps",
        )
        .unwrap();

        let client = SummonClient::new(create_test_context()).unwrap();
        let session = crate::session::Session {
            working_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let mut with_source = SubTask::new("subtask-1", "deploy to staging");
        with_source.source = Some("deploy".to_string());
        let adhoc = SubTask::new("subtask-2", "just prose work");

        let resolved = client
            .resolve_subtask_sources(&[with_source, adhoc], &session)
            .await
            .unwrap();
        assert_eq!(resolved.len(), 1);
        let recipe = resolved.get("subtask-1").unwrap();
        assert_eq!(recipe.instructions.as_deref(), Some("Run deploy steps"));

        let mut unknown = SubTask::new("subtask-1", "x");
        unknown.source = Some("no-such-source".to_string());
        let err = client
            .resolve_subtask_sources(&[unknown], &session)
            .await
            .unwrap_err();
        assert!(err.contains("not found"), "unexpected error: {err}");
    }

    #[test]
    fn test_parse_explicit_subtasks_rejects_bad_input() {
        let not_array = serde_json::json!({"instruction": "x"});
        assert!(parse_explicit_subtasks(&not_array, &[])
            .unwrap_err()
            .contains("JSON array"));

        let empty = serde_json::json!([]);
        assert!(parse_explicit_subtasks(&empty, &[])
            .unwrap_err()
            .contains("must not be empty"));

        let missing_instruction = serde_json::json!([{"role": "coder"}]);
        assert!(parse_explicit_subtasks(&missing_instruction, &[])
            .unwrap_err()
            .contains("not a valid plan"));

        let cyclic = serde_json::json!([
            {"instruction": "a", "depends_on": [1]},
            {"instruction": "b", "depends_on": [0]}
        ]);
        assert!(parse_explicit_subtasks(&cyclic, &[])
            .unwrap_err()
            .contains("not a valid plan"));

        let too_many: Vec<serde_json::Value> = (0..MAX_SUBTASKS + 1)
            .map(|i| serde_json::json!({"instruction": format!("task {i}")}))
            .collect();
        assert!(parse_explicit_subtasks(&serde_json::json!(too_many), &[])
            .unwrap_err()
            .contains("at most"));
    }

    #[test]
    fn test_is_session_id() {
        assert!(is_session_id("20260204_1"));
        assert!(is_session_id("20260204_42"));
        assert!(is_session_id("20260204_999"));
        assert!(!is_session_id("task_12345_0001"));
        assert!(!is_session_id("my-recipe"));
        assert!(!is_session_id("2026020_1"));
        assert!(!is_session_id("20260204"));
    }

    #[tokio::test]
    async fn test_async_task_result_lifecycle() {
        let client = SummonClient::new(create_test_context()).unwrap();
        let temp_dir = TempDir::new().unwrap();

        let result = client
            .handle_load_task_result("20260204_999", false, false)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));

        {
            use crate::agents::subagent_handler::create_tool_notification;
            use crate::conversation::message::MessageContent;
            use rmcp::model::CallToolRequestParams;

            let tool_call = CallToolRequestParams::new("developer__shell").with_arguments(
                serde_json::json!({"command": "ls"})
                    .as_object()
                    .unwrap()
                    .clone(),
            );
            let content = MessageContent::tool_request("req1", Ok(tool_call));
            let notif = create_tool_notification(&content, "20260204_1").unwrap();

            let buffer = Arc::new(Mutex::new(vec![notif]));

            let mut running = client.background_tasks.lock().await;
            running.insert(
                "20260204_1".to_string(),
                BackgroundTask {
                    id: "20260204_1".to_string(),
                    description: "Running task".to_string(),
                    started_at: Instant::now(),
                    turns: Arc::new(AtomicU32::new(2)),
                    last_activity: Arc::new(AtomicU64::new(current_epoch_millis())),
                    handle: tokio::spawn(async {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok("done".to_string())
                    }),
                    cancellation_token: CancellationToken::new(),
                    notification_buffer: buffer,
                },
            );
        }

        let mut subscriber = client.subscribe().await;

        let result = client
            .handle_load_task_result("20260204_1", false, false)
            .await
            .expect("load should wait and return result");
        let text = extract_text(&result.content[0]);
        assert!(text.contains("Completed"));
        assert!(text.contains("done"));

        let notif = subscriber
            .try_recv()
            .expect("subscriber should receive buffered notification");
        if let ServerNotification::LoggingMessageNotification(log) = notif {
            let data = log.params.data.as_object().unwrap();
            assert_eq!(
                data.get("subagent_id").and_then(|v| v.as_str()),
                Some("20260204_1")
            );
        } else {
            panic!("expected logging notification");
        }

        {
            let mut completed = client.completed_tasks.lock().await;
            completed.insert(
                "20260204_2".to_string(),
                CompletedTask {
                    id: "20260204_2".to_string(),
                    description: "Successful task".to_string(),
                    result: Ok("Task completed successfully with output".to_string()),
                    turns_taken: 5,
                    duration: Duration::from_secs(60),
                    completed_at: Instant::now(),
                },
            );
            completed.insert(
                "20260204_3".to_string(),
                CompletedTask {
                    id: "20260204_3".to_string(),
                    description: "Failed task".to_string(),
                    result: Err("Something went wrong".to_string()),
                    turns_taken: 3,
                    duration: Duration::from_secs(30),
                    completed_at: Instant::now(),
                },
            );
        }

        let moim = client.get_moim("test").await.unwrap();
        assert!(moim.contains("20260204_2"));
        assert!(moim.contains("20260204_3"));
        assert!(moim.contains(r#"use load("20260204_2") to get result"#));
        assert!(moim.contains(r#"use load("20260204_3") to get result"#));

        let discovery = client
            .handle_load_discovery("test", temp_dir.path())
            .await
            .unwrap();
        let discovery_text = extract_text(&discovery[0]);
        assert!(discovery_text.contains("Completed Tasks (awaiting retrieval)"));
        assert!(discovery_text.contains("20260204_2"));
        assert!(discovery_text.contains("20260204_3"));

        let result = client
            .handle_load_task_result("20260204_2", false, false)
            .await
            .unwrap();
        let text = extract_text(&result.content[0]);
        assert!(text.contains("20260204_2"));
        assert!(text.contains("Successful task"));
        assert!(text.contains("✓ Completed"));
        assert!(text.contains("1m"));
        assert!(text.contains("5 turns"));
        assert!(text.contains("Task completed successfully with output"));
        assert_eq!(result.status, "completed");
        assert_eq!(result.turns, Some(5));

        assert!(!client
            .completed_tasks
            .lock()
            .await
            .contains_key("20260204_2"));

        let result = client
            .handle_load_task_result("20260204_3", false, false)
            .await
            .unwrap();
        let text = extract_text(&result.content[0]);
        assert!(text.contains("✗ Failed"));
        assert!(text.contains("Error: Something went wrong"));
        assert_eq!(result.status, "failed");

        let result = client
            .handle_load_task_result("20260204_3", false, false)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));

        // All tasks consumed -- moim should be empty
        assert!(client.get_moim("test").await.is_none());
    }

    #[tokio::test]
    async fn test_load_routes_swarm_task_ids_to_task_results() {
        let client = SummonClient::new(create_test_context()).unwrap();

        {
            let mut running = client.background_tasks.lock().await;
            running.insert(
                "swarm-1".to_string(),
                BackgroundTask {
                    id: "swarm-1".to_string(),
                    description: "swarm: analyze the repo".to_string(),
                    started_at: Instant::now(),
                    turns: Arc::new(AtomicU32::new(0)),
                    last_activity: Arc::new(AtomicU64::new(current_epoch_millis())),
                    handle: tokio::spawn(async {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok("swarm report text".to_string())
                    }),
                    cancellation_token: CancellationToken::new(),
                    notification_buffer: Arc::new(Mutex::new(Vec::new())),
                },
            );
        }

        let args = serde_json::json!({"source": "swarm-1"})
            .as_object()
            .unwrap()
            .clone();
        let result = client.handle_load("test", Some(args)).await.unwrap();
        let text = extract_text(&result.content[0]);
        assert!(
            text.contains("swarm report text"),
            "swarm task id should route to the task result path, got: {text}"
        );
    }

    #[tokio::test]
    async fn test_cancel_running_task() {
        let client = SummonClient::new(create_test_context()).unwrap();
        let token = CancellationToken::new();

        {
            let mut running = client.background_tasks.lock().await;
            running.insert(
                "20260204_1".to_string(),
                BackgroundTask {
                    id: "20260204_1".to_string(),
                    description: "Cancellable task".to_string(),
                    started_at: Instant::now(),
                    turns: Arc::new(AtomicU32::new(3)),
                    last_activity: Arc::new(AtomicU64::new(current_epoch_millis())),
                    handle: tokio::spawn(async {
                        tokio::time::sleep(Duration::from_secs(1000)).await;
                        Ok("should not see this".to_string())
                    }),
                    cancellation_token: token.clone(),
                    notification_buffer: Arc::new(Mutex::new(Vec::new())),
                },
            );
        }

        let result = client
            .handle_load_task_result("20260204_1", true, false)
            .await
            .unwrap();
        let text = extract_text(&result.content[0]);
        assert!(text.contains("Cancelled"));
        assert!(text.contains("20260204_1"));
        assert!(text.contains("Cancellable task"));
        assert_eq!(result.status, "cancelled");
        assert_eq!(result.turns, Some(3));
        assert!(token.is_cancelled());
        assert!(!client
            .background_tasks
            .lock()
            .await
            .contains_key("20260204_1"));
    }

    #[tokio::test]
    async fn test_peek_running_task() {
        let client = SummonClient::new(create_test_context()).unwrap();

        {
            let mut running = client.background_tasks.lock().await;
            running.insert(
                "20260204_1".to_string(),
                BackgroundTask {
                    id: "20260204_1".to_string(),
                    description: "Long running analysis".to_string(),
                    started_at: Instant::now(),
                    turns: Arc::new(AtomicU32::new(7)),
                    last_activity: Arc::new(AtomicU64::new(current_epoch_millis())),
                    handle: tokio::spawn(async {
                        tokio::time::sleep(Duration::from_secs(1000)).await;
                        Ok("eventual result".to_string())
                    }),
                    cancellation_token: CancellationToken::new(),
                    notification_buffer: Arc::new(Mutex::new(Vec::new())),
                },
            );
        }

        // Peek should return status without removing the task
        let result = client
            .handle_load_task_result("20260204_1", false, true)
            .await
            .unwrap();
        let text = extract_text(&result.content[0]);
        assert!(text.contains("Running"));
        assert!(text.contains("Long running analysis"));
        assert!(text.contains("7")); // turns taken

        // Task should still be in background_tasks (not consumed)
        assert!(client
            .background_tasks
            .lock()
            .await
            .contains_key("20260204_1"));
    }

    #[tokio::test]
    async fn test_peek_nonexistent_task() {
        let client = SummonClient::new(create_test_context()).unwrap();

        let result = client
            .handle_load_task_result("20260204_999", false, true)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[tokio::test]
    async fn test_peek_completed_task_returns_result() {
        let client = SummonClient::new(create_test_context()).unwrap();

        {
            let mut completed = client.completed_tasks.lock().await;
            completed.insert(
                "20260204_1".to_string(),
                CompletedTask {
                    id: "20260204_1".to_string(),
                    description: "Finished task".to_string(),
                    result: Ok("final output".to_string()),
                    turns_taken: 4,
                    duration: Duration::from_secs(30),
                    completed_at: Instant::now(),
                },
            );
        }

        // Peek on a completed task should return the full result (same as non-peek)
        let result = client
            .handle_load_task_result("20260204_1", false, true)
            .await
            .unwrap();
        let text = extract_text(&result.content[0]);
        assert!(text.contains("Completed"));
        assert!(text.contains("final output"));

        // Peek must be non-destructive: the result is still retrievable afterwards.
        assert!(client
            .completed_tasks
            .lock()
            .await
            .contains_key("20260204_1"));
        let result = client
            .handle_load_task_result("20260204_1", false, false)
            .await
            .unwrap();
        assert!(extract_text(&result.content[0]).contains("final output"));
    }
}
