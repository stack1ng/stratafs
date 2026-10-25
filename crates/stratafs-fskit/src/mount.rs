use std::io;
use std::path::Path;
use std::process::{Command, ExitStatus};

use bytes::Bytes;

use crate::volume_url::VolumeUrl;

// FILE_SYSTEM_NAME is the short name the extension declares in its Info.plist, which mount looks it up by.
pub const FILE_SYSTEM_NAME: &str = "stratafs";

#[derive(Debug, thiserror::Error)]
pub enum MountError {
    #[error("the command could not be started")]
    NotStarted(#[source] io::Error),
    #[error("the command exited with {status}: {stderr}")]
    Failed { status: ExitStatus, stderr: String },
}

// mount mounts the volume on dir through the stratafs FSKit extension, and returns once it is mounted.
// dir must exist and belong to the user.
pub fn mount<V: Clone + Into<Bytes>>(url: &VolumeUrl<V>, dir: &Path) -> Result<(), MountError> {
    run(Command::new("/sbin/mount")
        .args(["-F", "-t", FILE_SYSTEM_NAME])
        .arg(url.to_string())
        .arg(dir))
}

// unmount unmounts whatever volume is mounted on dir.
pub fn unmount(dir: &Path) -> Result<(), MountError> {
    run(Command::new("/sbin/umount").arg(dir))
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
