// Mounts through the stratafs-local FSKit module installed on this Mac. The store must be mdbx or sqlite when the
// volume gets more than one mount, as the module of each mount is a process of its own.

use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use clap::Args;
use stratafs_fskit::mount::{mount, unmount};

use crate::ConsistencyKind;

#[derive(Args)]
pub struct Arguments {
    #[arg(
        long,
        help = "The stratafs-local binary of the installed FSKit module's build; the one beside this binary when left out"
    )]
    local: Option<PathBuf>,
    #[arg(
        long,
        default_value = "sqlite",
        help = "The store's database: mdbx, redb or sqlite"
    )]
    store: String,
}

// Store is a store directory with one volume in it, and the options a mount of it takes.
pub struct Store {
    directory: PathBuf,
    database: String,
    volume: String,
    coordinator: String,
    // signing_key is the key a signed volume is signed with, as the mount options take it.
    signing_key: Option<String>,
}

impl Store {
    // create makes the store in directory with one volume, whose mounts reach the coordinator at its URL. The volume
    // is signed with signing_key when there is one.
    pub fn create(
        arguments: &Arguments,
        directory: &Path,
        coordinator: &str,
        signing_key: Option<&str>,
    ) -> Result<Store, String> {
        let local = super::local(&arguments.local)?;
        let volume = super::create(&local, directory, &arguments.store, signing_key)?;
        Ok(Store {
            directory: directory.to_owned(),
            database: arguments.store.clone(),
            volume,
            coordinator: coordinator.to_owned(),
            signing_key: signing_key.map(str::to_owned),
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
        let mut options = format!(
            "store={},volume={},consistency={consistency},coordinator={}",
            self.database, self.volume, self.coordinator
        );
        if let Some(signing_key) = &self.signing_key {
            options.push_str(&format!(",signing-key={signing_key}"));
        }
        mount("stratafs", &options, &link, mount_point)
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
