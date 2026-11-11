// Mounts through the FSKit module installed on this Mac, which must be built with the sqlite or mdbx store when the
// volume gets more than one mount, as the module of each mount is a process of its own.

use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::Command;

use bytes::Bytes;
use clap::Args;
use stratafs_fskit::mount::{mount, unmount};
use stratafs_fskit::mount_options;

use crate::ConsistencyKind;

#[derive(Args)]
pub struct Arguments {
    #[arg(
        long,
        help = "The stratafs-bench binary of the installed FSKit module's build; the one beside this binary when left out"
    )]
    bench: Option<PathBuf>,
}

// Store is a store directory with one volume in it.
pub struct Store {
    directory: PathBuf,
    volume: Bytes,
}

impl Store {
    // create makes the store in directory with one volume, whose mounts reach the coordinator at its URL.
    pub fn create(
        arguments: &Arguments,
        directory: &Path,
        coordinator: &str,
    ) -> Result<Store, String> {
        let bench = match &arguments.bench {
            Some(bench) => bench.clone(),
            None => super::beside("stratafs-bench")?,
        };
        let created = Command::new(&bench)
            .arg("create")
            .arg(directory)
            .output()
            .map_err(|error| format!("starting {}: {error}", bench.display()))?;
        if !created.status.success() {
            return Err(format!(
                "creating the volume failed: {}",
                String::from_utf8_lossy(&created.stderr)
            ));
        }
        let volume = hex::decode(String::from_utf8_lossy(&created.stdout).trim())
            .map_err(|error| format!("reading the new volume's id: {error}"))?;
        // The module reads the coordinator's URL from this file in the store directory.
        fs::write(directory.join("coordinator"), coordinator)
            .map_err(|error| format!("writing the coordinator's URL: {error}"))?;
        Ok(Store {
            directory: directory.to_owned(),
            volume: Bytes::from(volume),
        })
    }

    // mount mounts the volume on mount_point with consistency. index tells the mount from the volume's others.
    pub fn mount(
        &self,
        index: usize,
        consistency: ConsistencyKind,
        mount_point: &Path,
    ) -> Result<Mounted, String> {
        // FSKit refuses a second mount of one store path, so each mount names the store through a link of its own.
        let mut link = self.directory.clone().into_os_string();
        link.push(format!(".{index}"));
        let link = PathBuf::from(link);
        symlink(&self.directory, &link)
            .map_err(|error| format!("linking the store for mount {index}: {error}"))?;
        let consistency = match consistency {
            ConsistencyKind::Strong => mount_options::ConsistencyKind::Strong,
            ConsistencyKind::Eventual => mount_options::ConsistencyKind::Eventual,
        };
        mount(
            "stratafs",
            self.volume.clone(),
            consistency,
            &link,
            mount_point,
        )
        .map_err(|error| format!("mounting on {}: {error}", mount_point.display()))?;
        Ok(Mounted {
            mount_point: mount_point.to_owned(),
        })
    }
}

// Mounted is a mount of the volume, until it is unmounted.
pub struct Mounted {
    mount_point: PathBuf,
}

impl Mounted {
    pub fn unmount(self) -> Result<(), String> {
        unmount(&self.mount_point)
            .map_err(|error| format!("unmounting {}: {error}", self.mount_point.display()))
    }
}
