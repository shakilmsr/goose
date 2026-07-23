use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

use crate::sandbox::backend::{PreparedCommand, SandboxBackend, SandboxPolicy};

/// Local OS sandbox backend using bubblewrap (`bwrap`) on Linux systems.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    policy: SandboxPolicy,
}

impl LocalBackend {
    pub fn new(policy: SandboxPolicy) -> Self {
        Self { policy }
    }

    pub fn with_default_policy() -> Self {
        Self::new(SandboxPolicy::default())
    }
}

#[async_trait]
impl SandboxBackend for LocalBackend {
    fn wrap_command(&self, cmd: &str, cwd: Option<&Path>) -> Result<PreparedCommand> {
        let work_dir = cwd
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| self.policy.writable_work_root.clone());

        #[cfg(target_os = "linux")]
        {
            let mut args = vec![
                "--unshare-pid".to_string(),
                "--unshare-ipc".to_string(),
                "--unshare-uts".to_string(),
                "--die-with-parent".to_string(),
            ];

            // Writable workspace bind
            if self.policy.writable_work_root.exists() {
                args.push("--bind".to_string());
                args.push(self.policy.writable_work_root.to_string_lossy().to_string());
                args.push(self.policy.writable_work_root.to_string_lossy().to_string());
            }

            // Read-only binds for system libraries and tools
            for ro in &self.policy.ro_binds {
                if ro.exists() {
                    args.push("--ro-bind".to_string());
                    args.push(ro.to_string_lossy().to_string());
                    args.push(ro.to_string_lossy().to_string());
                }
            }

            // Target working directory
            args.push("--chdir".to_string());
            args.push(work_dir.to_string_lossy().to_string());

            // Command payload
            args.push("--".to_string());
            args.push("/bin/sh".to_string());
            args.push("-c".to_string());
            args.push(cmd.to_string());

            Ok(PreparedCommand {
                program: "bwrap".to_string(),
                args,
                envs: Vec::new(),
                cwd: Some(work_dir),
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            // On non-Linux hosts (e.g. Windows), fall back to running command directly on host.
            #[cfg(windows)]
            {
                Ok(PreparedCommand {
                    program: "cmd".to_string(),
                    args: vec!["/C".to_string(), cmd.to_string()],
                    envs: Vec::new(),
                    cwd: Some(work_dir),
                })
            }
            #[cfg(not(windows))]
            {
                Ok(PreparedCommand {
                    program: "/bin/sh".to_string(),
                    args: vec!["-c".to_string(), cmd.to_string()],
                    envs: Vec::new(),
                    cwd: Some(work_dir),
                })
            }
        }
    }

    fn sandbox_type(&self) -> &'static str {
        if cfg!(target_os = "linux") {
            "bubblewrap"
        } else {
            "host_fallback"
        }
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
    fn test_local_backend_wrap_command() {
        let temp_dir = TempDir::new().unwrap();
        let policy = SandboxPolicy {
            writable_work_root: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let backend = LocalBackend::new(policy);
        let prep = backend.wrap_command("echo hello", None).unwrap();
        assert!(prep.args.iter().any(|a| a.contains("echo hello")));
        assert_eq!(prep.cwd, Some(temp_dir.path().to_path_buf()));
    }
}
