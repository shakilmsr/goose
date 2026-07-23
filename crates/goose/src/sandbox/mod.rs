pub mod backend;
pub mod local;
pub mod macos;
pub mod windows;

pub use backend::{DynSandboxBackend, PreparedCommand, SandboxBackend, SandboxPolicy};
pub use local::LocalBackend;
pub use macos::MacosBackend;
pub use windows::WindowsBackend;

#[cfg(target_os = "linux")]
pub type DefaultSandboxBackend = local::LocalBackend;

#[cfg(target_os = "macos")]
pub type DefaultSandboxBackend = macos::MacosBackend;

#[cfg(windows)]
pub type DefaultSandboxBackend = windows::WindowsBackend;
