use std::path::Path;

use crate::suite::SuiteError;
use crate::suite::config::Config;
use crate::suite::inputs::{Inputs, REPOSITORIES};
use crate::suite::quoted;

// Workload is one thing the suite measures on a file system, by name.
pub struct Workload {
    pub name: String,
    pub load: Load,
}

pub enum Load {
    // Timed is a shell command hyperfine times. setup runs once before the runs, and prepare before each one,
    // untimed.
    Timed {
        setup: Option<String>,
        prepare: Option<String>,
        command: String,
    },
    // Fio is a fio job, given as fio's arguments. The suite adds where its files go and where its results go.
    Fio {
        arguments: Vec<String>,
    },
}

// selected are the workloads the config names, or every one when it names none, run in the directory work.
pub fn selected(
    config: &Config,
    work: &Path,
    inputs: &Inputs,
) -> Result<Vec<Workload>, SuiteError> {
    if !REPOSITORIES.iter().any(|(size, _)| *size == config.tree) {
        return Err(SuiteError::Config(format!(
            "tree is {}, which is not a repository size: small, medium or large",
            config.tree
        )));
    }
    let all = all(work, inputs, &config.tree, config.files);
    let Some(names) = &config.workloads else {
        return Ok(all);
    };
    if let Some(unknown) = names
        .iter()
        .find(|name| !all.iter().any(|workload| &workload.name == *name))
    {
        let known: Vec<&str> = all.iter().map(|workload| workload.name.as_str()).collect();
        return Err(SuiteError::Config(format!(
            "there is no workload named {unknown}; the workloads are {}",
            known.join(", ")
        )));
    }
    Ok(all
        .into_iter()
        .filter(|workload| names.contains(&workload.name))
        .collect())
}

fn all(work: &Path, inputs: &Inputs, tree: &str, files: u32) -> Vec<Workload> {
    let at = |name: &str| quoted(&work.join(name));
    let clone = |size: &str, into: &str| {
        format!(
            "git clone --quiet --no-local file://{} {}",
            inputs.repository(size).display(),
            at(into)
        )
    };
    let unpack = |size: &str, into: &str| {
        format!(
            "rm -rf {into} && mkdir -p {into} && tar -xf {} -C {into}",
            quoted(&inputs.archive(size)),
            into = at(into)
        )
    };
    let timed =
        |name: String, setup: Option<String>, prepare: Option<String>, command: String| Workload {
            name,
            load: Load::Timed {
                setup,
                prepare,
                command,
            },
        };
    let fio = |name: &str, arguments: &[&str]| Workload {
        name: name.to_owned(),
        load: Load::Fio {
            arguments: [format!("--name={name}")]
                .into_iter()
                .chain(arguments.iter().map(|argument| (*argument).to_owned()))
                .collect(),
        },
    };
    let nrfiles = format!("--nrfiles={files}");
    let mut workloads = Vec::new();
    for (size, _) in REPOSITORIES {
        // Cloning writes the repository and checks out every file.
        workloads.push(timed(
            format!("clone-{size}"),
            None,
            Some(format!("rm -rf {}", at(&format!("clone-{size}")))),
            clone(size, &format!("clone-{size}")),
        ));
        // Status stats every file of a checkout.
        workloads.push(timed(
            format!("status-{size}"),
            Some(format!(
                "rm -rf {} && {}",
                at(&format!("status-{size}")),
                clone(size, &format!("status-{size}"))
            )),
            None,
            format!(
                "git -C {} status --porcelain",
                at(&format!("status-{size}"))
            ),
        ));
    }
    let build_setup = format!(
        "rm -rf {} && {}",
        at("build"),
        clone("small", "build/source")
    );
    let build = format!(
        "cargo build --release --offline --quiet --manifest-path {} --target-dir {}",
        quoted(&work.join("build/source/Cargo.toml")),
        at("build/target")
    );
    workloads.extend([
        // Searching reads every file of a checkout.
        timed(
            format!("search-{tree}"),
            Some(format!(
                "rm -rf {} && {}",
                at("search"),
                clone(tree, "search")
            )),
            None,
            format!("rg --count-matches TODO {}", at("search")),
        ),
        // A clean build reads the sources and writes the build's output.
        timed(
            "build-small".to_owned(),
            Some(build_setup.clone()),
            Some(format!("rm -rf {}", at("build/target"))),
            build.clone(),
        ),
        // A build with nothing to do stats the sources and the earlier output.
        timed(
            "rebuild-small".to_owned(),
            Some(format!("{build_setup} && {build}")),
            None,
            build,
        ),
        // Unpacking writes many files, as an install or an output bundle does.
        timed(
            format!("unpack-{tree}"),
            None,
            Some(format!("rm -rf {}", at("unpack"))),
            unpack(tree, "unpack"),
        ),
        // Copying reads one tree and writes another on the same file system.
        timed(
            format!("copy-{tree}"),
            Some(unpack(tree, "copy-source")),
            Some(format!(
                "rm -rf {} && mkdir {}",
                at("copy-target"),
                at("copy-target")
            )),
            format!(
                "tar -cf - -C {} . | tar -xf - -C {}",
                at("copy-source"),
                at("copy-target")
            ),
        ),
        // Removing deletes every file of a tree.
        timed(
            format!("remove-{tree}"),
            None,
            Some(unpack(tree, "remove")),
            format!("rm -rf {}", at("remove")),
        ),
        fio(
            "sequential-write",
            // Flushing every 256 writes puts the flushes inside fio's timing, which a flush only at the end
            // would not be.
            &[
                "--rw=write",
                "--bs=1M",
                "--size=4G",
                "--ioengine=psync",
                "--fsync=256",
            ],
        ),
        fio(
            "sequential-read",
            &[
                "--rw=read",
                "--bs=1M",
                "--size=4G",
                "--ioengine=psync",
                "--invalidate=1",
            ],
        ),
        fio(
            "random-read",
            &[
                "--rw=randread",
                "--bs=4k",
                "--size=1G",
                "--ioengine=psync",
                "--invalidate=1",
                "--runtime=30",
                "--time_based",
            ],
        ),
        fio(
            "random-write",
            &[
                "--rw=randwrite",
                "--bs=4k",
                "--size=1G",
                "--ioengine=psync",
                "--fsync=32",
                "--runtime=30",
                "--time_based",
            ],
        ),
        fio(
            "create-files",
            &[
                "--ioengine=filecreate",
                nrfiles.as_str(),
                "--filesize=4k",
                "--rw=write",
                "--bs=4k",
            ],
        ),
        fio(
            "stat-files",
            &[
                "--ioengine=filestat",
                nrfiles.as_str(),
                "--filesize=4k",
                "--rw=read",
                "--bs=4k",
            ],
        ),
        fio(
            "delete-files",
            &[
                "--ioengine=filedelete",
                nrfiles.as_str(),
                "--filesize=4k",
                "--rw=write",
                "--bs=4k",
            ],
        ),
    ]);
    workloads
}
