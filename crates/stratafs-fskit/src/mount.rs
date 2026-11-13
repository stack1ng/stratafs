use std::io;
use std::path::Path;
use std::process::{Command, ExitStatus};

#[derive(Debug, thiserror::Error)]
pub enum MountError {
    #[error("the command could not be started")]
    NotStarted(#[source] io::Error),
    #[error("the command exited with {status}: {stderr}")]
    Failed { status: ExitStatus, stderr: String },
}

// mount mounts the store directory special on mount_point with the module mount(8) knows as external_type, passing it
// options as -o does.
pub fn mount(
    // mount(8)'s -t external_type: https://keith.github.io/xcode-man-pages/mount.8.html#t
    external_type: &str,
    // mount(8)'s -o options: https://keith.github.io/xcode-man-pages/mount.8.html#o
    options: &str,
    // mount(8)'s special: https://keith.github.io/xcode-man-pages/mount.8.html#DESCRIPTION
    special: &Path,
    // mount(8)'s mount_point: https://keith.github.io/xcode-man-pages/mount.8.html#DESCRIPTION
    mount_point: &Path,
) -> Result<(), MountError> {
    run(Command::new("/sbin/mount")
        .args(["-F", "-t", external_type, "-o", options])
        .arg(special)
        .arg(mount_point))
}

// unmount unmounts whatever volume is mounted on node.
pub fn unmount(
    // umount(8)'s node: https://keith.github.io/xcode-man-pages/umount.8.html#DESCRIPTION
    node: &Path,
) -> Result<(), MountError> {
    run(Command::new("/sbin/umount").arg(node))
}

fn run(command: &mut Command) -> Result<(), MountError> {
    let output = command.output().map_err(MountError::NotStarted)?;
    if output.status.success() {
        return Ok(());
    }
    Err(MountError::Failed {
        status: output.status,
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}
