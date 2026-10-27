use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::suite::SuiteError;

// Config is a benchmark run, as a TOML file describes it. Relative paths are relative to the file.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    // results is where each run's results go, in a directory of its own.
    pub results: PathBuf,
    // inputs is where the repositories and archives the workloads use are kept between runs.
    pub inputs: PathBuf,
    // runs is how many times each timed workload runs.
    #[serde(default = "five")]
    pub runs: u32,
    // warmup is how many untimed runs each timed workload makes before its timed ones, unless caches are cleared.
    #[serde(default = "one")]
    pub warmup: u32,
    // tree is the repository the search, unpack, copy and remove workloads use, by size: small, medium or large.
    #[serde(default = "large")]
    pub tree: String,
    // files is how many files the create, stat and delete workloads make.
    #[serde(default = "hundred_thousand")]
    pub files: u32,
    // clear_caches clears the caches before every run, which needs `sudo -n /usr/sbin/purge` to work.
    #[serde(default)]
    pub clear_caches: bool,
    // workloads are the workloads to run, by name. Leaving it out runs every one.
    #[serde(default)]
    pub workloads: Option<Vec<String>>,
    pub targets: Vec<TargetConfig>,
}

// TargetConfig is a file system to measure.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TargetConfig {
    // Directory is an existing directory on whatever file system holds it, such as APFS on macOS.
    Directory {
        name: String,
        path: PathBuf,
    },
    // Stratafs is a fresh volume in the stratafs store directory, mounted with the installed FSKit module.
    Stratafs {
        name: String,
        store: PathBuf,
        mount_point: PathBuf,
    },
    // Juicefs is a JuiceFS volume kept in a store directory, with SQLite metadata and its blobs in the same
    // directory, mounted with the juicefs binary.
    Juicefs {
        name: String,
        binary: PathBuf,
        store: PathBuf,
        mount_point: PathBuf,
    },
}

fn five() -> u32 {
    5
}

fn one() -> u32 {
    1
}

fn large() -> String {
    "large".to_owned()
}

fn hundred_thousand() -> u32 {
    100_000
}

impl Config {
    pub fn read(file: &Path) -> Result<Self, SuiteError> {
        let text = std::fs::read_to_string(file).map_err(|source| SuiteError::Io {
            action: format!("reading {}", file.display()),
            source,
        })?;
        let mut config: Config = toml::from_str(&text)
            .map_err(|error| SuiteError::Config(format!("{}: {error}", file.display())))?;
        // Paths become absolute, since the workloads hand them to tools as URLs and from other directories.
        let base =
            std::path::absolute(file.parent().unwrap_or(Path::new("."))).map_err(|source| {
                SuiteError::Io {
                    action: format!("resolving the directory of {}", file.display()),
                    source,
                }
            })?;
        let resolve = |path: &mut PathBuf| *path = base.join(&*path);
        resolve(&mut config.results);
        resolve(&mut config.inputs);
        for target in &mut config.targets {
            match target {
                TargetConfig::Directory { path, .. } => resolve(path),
                TargetConfig::Stratafs {
                    store, mount_point, ..
                } => {
                    resolve(store);
                    resolve(mount_point);
                }
                TargetConfig::Juicefs {
                    binary,
                    store,
                    mount_point,
                    ..
                } => {
                    resolve(binary);
                    resolve(store);
                    resolve(mount_point);
                }
            }
        }
        Ok(config)
    }
}

impl TargetConfig {
    pub fn name(&self) -> &str {
        match self {
            TargetConfig::Directory { name, .. }
            | TargetConfig::Stratafs { name, .. }
            | TargetConfig::Juicefs { name, .. } => name,
        }
    }
}
