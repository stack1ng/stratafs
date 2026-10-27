// The benchmark suite: the same workloads, timed on each file system a config names, with the load coming from
// hyperfine, fio and ordinary tools. It only needs a directory on each file system, so everything but the
// targets that mount something runs on any platform.

pub mod config;
pub mod inputs;
pub mod report;
pub mod targets;
pub mod workloads;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::suite::config::Config;
use crate::suite::inputs::Inputs;
use crate::suite::targets::Target;
use crate::suite::workloads::{Load, Workload};

#[derive(Debug, thiserror::Error)]
pub enum SuiteError {
    #[error("{0}")]
    Config(String),
    #[error("{action}")]
    Io {
        action: String,
        #[source]
        source: std::io::Error,
    },
    #[error("`{command}` failed: {status}")]
    Command { command: String, status: String },
}

// run measures every workload the config selects on every target it names, and returns the results directory.
pub fn run(config: &Config) -> Result<PathBuf, SuiteError> {
    let inputs = Inputs::prepare(&config.inputs)?;
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_secs();
    let results = config.results.join(since_epoch.to_string());
    make_directory(&results)?;
    report::write_environment(&results, config)?;
    for target_config in &config.targets {
        let target = targets::set_up(target_config)?;
        let measured = measure(config, &inputs, &target, &results.join(&target.name));
        let torn_down = target.tear_down();
        measured?;
        torn_down?;
    }
    report::write_summary(&results, config)?;
    Ok(results)
}

fn measure(
    config: &Config,
    inputs: &Inputs,
    target: &Target,
    results: &Path,
) -> Result<(), SuiteError> {
    make_directory(results)?;
    let work = target.directory.join("stratafs-bench");
    for workload in workloads::selected(config, &work, inputs)? {
        println!("{}: {}", target.name, workload.name);
        let output = results.join(format!("{}.json", workload.name));
        let log = results.join(format!("{}.log", workload.name));
        let measured = match &workload.load {
            Load::Timed { .. } => time(config, target, &workload, &output, &log),
            Load::Fio { arguments } => fio(config, target, &work, arguments, &output, &log),
        };
        // A failed workload is recorded, and the run goes on to the next one.
        if let Err(error) = measured {
            println!("{}: {} failed: {error}", target.name, workload.name);
            let failure = results.join(format!("{}.failed", workload.name));
            std::fs::write(&failure, error.to_string()).map_err(|source| SuiteError::Io {
                action: format!("writing {}", failure.display()),
                source,
            })?;
        }
    }
    shell(&format!("rm -rf {}", quoted(&work)))
}

fn time(
    config: &Config,
    target: &Target,
    workload: &Workload,
    output: &Path,
    log: &Path,
) -> Result<(), SuiteError> {
    let Load::Timed {
        setup,
        prepare,
        command,
    } = &workload.load
    else {
        unreachable!("time only times timed workloads")
    };
    let prepare = match (config.clear_caches, prepare) {
        (true, Some(prepare)) => Some(format!("{} && {prepare}", target.clear_caches_command())),
        (true, None) => Some(target.clear_caches_command()),
        (false, prepare) => prepare.clone(),
    };
    // Each command's errors go to the workload's log, since hyperfine discards what commands print.
    let logged = |command: &str| format!("{{ {command}; }} 2>> {}", quoted(log));
    let mut hyperfine = Command::new("hyperfine");
    hyperfine
        .args(["--style", "basic", "--runs"])
        .arg(config.runs.to_string())
        .arg("--warmup")
        .arg(
            if config.clear_caches {
                0
            } else {
                config.warmup
            }
            .to_string(),
        )
        .arg("--export-json")
        .arg(output);
    if let Some(setup) = setup {
        hyperfine.arg("--setup").arg(logged(setup));
    }
    if let Some(prepare) = prepare {
        hyperfine.arg("--prepare").arg(logged(&prepare));
    }
    hyperfine.arg(logged(command));
    execute(&mut hyperfine)
}

fn fio(
    config: &Config,
    target: &Target,
    work: &Path,
    arguments: &[String],
    output: &Path,
    log: &Path,
) -> Result<(), SuiteError> {
    let directory = work.join("fio");
    make_directory(&directory)?;
    if config.clear_caches {
        target.clear_caches()?;
    }
    let errors = std::fs::File::create(log).map_err(|source| SuiteError::Io {
        action: format!("making {}", log.display()),
        source,
    })?;
    execute(
        Command::new("fio")
            .arg("--output-format=json")
            .arg(format!("--output={}", output.display()))
            .arg(format!("--directory={}", directory.display()))
            .args(arguments)
            .stderr(errors),
    )?;
    shell(&format!("rm -rf {}", quoted(&directory)))
}

pub(crate) fn make_directory(directory: &Path) -> Result<(), SuiteError> {
    std::fs::create_dir_all(directory).map_err(|source| SuiteError::Io {
        action: format!("making {}", directory.display()),
        source,
    })
}

pub(crate) fn shell(command: &str) -> Result<(), SuiteError> {
    execute(Command::new("/bin/sh").args(["-c", command]))
}

pub(crate) fn execute(command: &mut Command) -> Result<(), SuiteError> {
    let status = command.status().map_err(|source| SuiteError::Io {
        action: format!("starting {command:?}"),
        source,
    })?;
    if status.success() {
        return Ok(());
    }
    Err(SuiteError::Command {
        command: format!("{command:?}"),
        status: status.to_string(),
    })
}

// quoted is a path as one shell word.
pub(crate) fn quoted(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', r"'\''"))
}
