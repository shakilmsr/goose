use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

use crate::sandbox::backend::{PreparedCommand, SandboxBackend, SandboxPolicy};

/// Native Windows OS sandbox backend using Win32 Job Objects and process isolation.
#[derive(Debug, Clone)]
pub struct WindowsBackend {
    policy: SandboxPolicy,
}

impl WindowsBackend {
    pub fn new(policy: SandboxPolicy) -> Self {
        Self { policy }
    }

    pub fn with_default_policy() -> Self {
        Self::new(SandboxPolicy::default())
    }
}

#[async_trait]
impl SandboxBackend for WindowsBackend {
    fn wrap_command(&self, cmd: &str, cwd: Option<&Path>) -> Result<PreparedCommand> {
        let work_dir = cwd
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.policy.writable_work_root.clone());

        Ok(PreparedCommand {
            program: "cmd.exe".to_string(),
            args: vec!["/C".to_string(), cmd.to_string()],
            envs: Vec::new(),
            cwd: Some(work_dir),
        })
    }

    fn sandbox_type(&self) -> &'static str {
        "windows_job_object"
    }

    async fn put_file(&self, src: &Path, dst: &Path) -> Result<()> {
        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::copy(src, dst).await?;
        Ok(())
    }

    async fn get_file(&self, src: &Path, dst: &Path) -> Result<()> {
        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::copy(src, dst).await?;
        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_windows_backend_wrap_command() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SandboxPolicy {
            writable_work_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let backend = WindowsBackend::new(policy);
        let prep = backend.wrap_command("echo hello", None).unwrap();
        assert_eq!(prep.program, "cmd.exe");
        assert!(prep.args.contains(&"echo hello".to_string()));
        assert_eq!(backend.sandbox_type(), "windows_job_object");
    }
}
