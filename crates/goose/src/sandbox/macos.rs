use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

use crate::sandbox::backend::{PreparedCommand, SandboxBackend, SandboxPolicy};

/// Native macOS OS sandbox backend using Apple's built-in sandbox-exec (Seatbelt API).
#[derive(Debug, Clone)]
pub struct MacosBackend {
    policy: SandboxPolicy,
}

impl MacosBackend {
    pub fn new(policy: SandboxPolicy) -> Self {
        Self { policy }
    }

    pub fn with_default_policy() -> Self {
        Self::new(SandboxPolicy::default())
    }

    /// Generate Apple Seatbelt profile string (.sb) for the command execution.
    fn generate_seatbelt_profile(&self, work_dir: &Path) -> String {
        let work_path_str = work_dir.to_string_lossy();
        format!(
            r#"(version 1)
(allow default)
(allow process-fork)
(allow process-exec)
(allow network*)
(allow file-read*)
(deny file-read* (subpath (param "HOME_SSH")))
(deny file-read* (subpath (param "HOME_AWS")))
(deny file-write* (subpath "/"))
(allow file-write* (subpath "{work_path_str}"))
(allow file-write* (subpath "/tmp"))
(allow file-write* (subpath "/private/tmp"))
"#
        )
    }
}

#[async_trait]
impl SandboxBackend for MacosBackend {
    fn wrap_command(&self, cmd: &str, cwd: Option<&Path>) -> Result<PreparedCommand> {
        let work_dir = cwd
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.policy.writable_work_root.clone());

        let profile = self.generate_seatbelt_profile(&work_dir);

        Ok(PreparedCommand {
            program: "/usr/bin/sandbox-exec".to_string(),
            args: vec![
                "-p".to_string(),
                profile,
                "sh".to_string(),
                "-c".to_string(),
                cmd.to_string(),
            ],
            envs: Vec::new(),
            cwd: Some(work_dir),
        })
    }

    fn sandbox_type(&self) -> &'static str {
        "macos_seatbelt"
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
    fn test_macos_backend_wrap_command() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SandboxPolicy {
            writable_work_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let backend = MacosBackend::new(policy);
        let prep = backend.wrap_command("echo hello", None).unwrap();
        assert_eq!(prep.program, "/usr/bin/sandbox-exec");
        assert!(prep.args.contains(&"sh".to_string()));
        assert!(prep.args.contains(&"echo hello".to_string()));
        assert_eq!(backend.sandbox_type(), "macos_seatbelt");
    }
}
