use std::path::{Path, PathBuf};
use std::process::Command;

use crate::suite::{SuiteError, execute, make_directory};

// REPOSITORIES are the repositories the workloads clone and unpack, by size: few files, about fifteen thousand
// and about ninety thousand. Only their newest commit is kept.
pub const REPOSITORIES: [(&str, &str); 3] = [
    ("small", "https://github.com/BurntSushi/ripgrep.git"),
    ("medium", "https://github.com/golang/go.git"),
    ("large", "https://github.com/torvalds/linux.git"),
];

// Inputs are the repositories and archives the workloads read, fetched once into a directory on the machine's
// own disk, so fetching never counts toward a file system's time.
pub struct Inputs {
    root: PathBuf,
}

impl Inputs {
    pub fn prepare(root: &Path) -> Result<Self, SuiteError> {
        make_directory(root)?;
        let inputs = Inputs {
            root: root.to_owned(),
        };
        for (size, url) in REPOSITORIES {
            let repository = inputs.repository(size);
            if !repository.exists() {
                execute(
                    Command::new("git")
                        .args([
                            "clone",
                            "--quiet",
                            "--bare",
                            "--depth",
                            "1",
                            "--single-branch",
                            url,
                        ])
                        .arg(&repository),
                )?;
            }
            let archive = inputs.archive(size);
            if !archive.exists() {
                execute(
                    Command::new("git")
                        .arg("-C")
                        .arg(&repository)
                        .args(["archive", "--format=tar", "--output"])
                        .arg(&archive)
                        .arg("HEAD"),
                )?;
            }
        }
        // The build workload builds offline, so its dependencies are fetched here.
        let checkout = root.join("small-checkout");
        if !checkout.exists() {
            execute(
                Command::new("git")
                    .args(["clone", "--quiet"])
                    .arg(inputs.repository("small"))
                    .arg(&checkout),
            )?;
        }
        execute(
            Command::new("cargo")
                .args(["fetch", "--quiet", "--manifest-path"])
                .arg(checkout.join("Cargo.toml")),
        )?;
        Ok(inputs)
    }

    // repository is the bare repository of a size.
    pub fn repository(&self, size: &str) -> PathBuf {
        self.root.join(format!("{size}.git"))
    }

    // archive is a size's files as a tar archive.
    pub fn archive(&self, size: &str) -> PathBuf {
        self.root.join(format!("{size}.tar"))
    }
}
