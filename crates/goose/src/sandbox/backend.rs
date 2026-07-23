use anyhow::Result;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Prepared command ready to be executed by a shell or process builder inside a sandbox wrapper.
#[derive(Debug, Clone)]
pub struct PreparedCommand {
    pub program: String,
    pub args: Vec<String>,
    pub envs: Vec<(String, String)>,
    pub cwd: Option<PathBuf>,
}

/// Policy configuration specifying sandbox boundaries.
#[derive(Debug, Clone)]
pub struct SandboxPolicy {
    pub writable_work_root: PathBuf,
    pub ro_binds: Vec<PathBuf>,
    pub allow_network: bool,
}

impl Default for SandboxPolicy {
    fn default() -> Self {
        Self {
            writable_work_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            ro_binds: vec![
                PathBuf::from("/usr"),
                PathBuf::from("/lib"),
                PathBuf::from("/lib64"),
                PathBuf::from("/bin"),
                PathBuf::from("/etc"),
            ],
            allow_network: true,
        }
    }
}

/// Abstract interface for sandbox backends (e.g. bubblewrap/nsjail locally, container/remote backends).
#[async_trait]
pub trait SandboxBackend: Send + Sync {
    /// Wrap a target command string (or argv) into a sandboxed command execution line.
    fn wrap_command(&self, cmd: &str, cwd: Option<&Path>) -> Result<PreparedCommand>;

    /// Return the name/identifier of this sandbox backend.
    fn sandbox_type(&self) -> &'static str;

    /// Copy a host file into the sandbox workspace.
    async fn put_file(&self, src: &Path, dst: &Path) -> Result<()>;

    /// Extract a file from the sandbox workspace to host.
    async fn get_file(&self, src: &Path, dst: &Path) -> Result<()>;

    /// Clean up any ephemeral mounts, namespaces, or resources.
    async fn teardown(&self) -> Result<()>;
}

pub type DynSandboxBackend = Arc<dyn SandboxBackend>;
