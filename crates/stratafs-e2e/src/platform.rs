// How this operating system makes a store and mounts its volume through the kernel.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

use std::path::PathBuf;

#[cfg(target_os = "linux")]
pub use linux::{Arguments, Mounted, Store};
#[cfg(target_os = "macos")]
pub use macos::{Arguments, Mounted, Store};

// beside is the path of the binary called name in this binary's directory, where cargo puts a workspace's binaries.
fn beside(name: &str) -> Result<PathBuf, String> {
    let this = std::env::current_exe().map_err(|error| format!("finding this binary: {error}"))?;
    Ok(this.with_file_name(name))
}
