pub mod backend;
pub mod local;
pub mod windows;

pub use backend::{DynSandboxBackend, PreparedCommand, SandboxBackend, SandboxPolicy};
pub use local::LocalBackend;
pub use windows::WindowsBackend;

#[cfg(target_os = "linux")]
pub type DefaultSandboxBackend = local::LocalBackend;

#[cfg(windows)]
pub type DefaultSandboxBackend = windows::WindowsBackend;
