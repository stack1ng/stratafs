// How this operating system makes a store and mounts its volume through the kernel.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(target_os = "linux")]
pub use linux::{Arguments, Mounted, Store};
#[cfg(target_os = "macos")]
pub use macos::{Arguments, Mounted, Store};

// beside is the path of the binary called name in this binary's directory, where cargo puts a workspace's binaries.
fn beside(name: &str) -> Result<PathBuf, String> {
    let this = std::env::current_exe().map_err(|error| format!("finding this binary: {error}"))?;
    Ok(this.with_file_name(name))
}

// local is the stratafs-local binary: given, or the one beside this binary.
fn local(given: &Option<PathBuf>) -> Result<PathBuf, String> {
    match given {
        Some(local) => Ok(local.clone()),
        None => beside("stratafs-local"),
    }
}

// create makes a volume in the store of database in directory with the stratafs-local binary local, signed with
// signing_key when there is one, and returns the volume's id in hex.
fn create(
    local: &Path,
    directory: &Path,
    database: &str,
    signing_key: Option<&str>,
) -> Result<String, String> {
    let created = Command::new(local)
        .arg("create")
        .arg(directory)
        .args(["--store", database])
        .args(signing_key.map(|_| "--signing-key"))
        .args(signing_key)
        .output()
        .map_err(|error| format!("starting {}: {error}", local.display()))?;
    if !created.status.success() {
        return Err(format!(
            "creating the volume failed: {}",
            String::from_utf8_lossy(&created.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&created.stdout).trim().to_owned())
}
