use std::fmt::Write;
use std::path::Path;
use std::process::Command;

use serde_json::Value;

use crate::suite::SuiteError;
use crate::suite::config::Config;
use crate::{BenchFsConfig, BenchTreeConfig};
use stratafs_fs_engine::FsConfig;
use stratafs_tree::tree::TreeConfig;

// write_environment records what the results were measured on: the machine, the system, the stratafs build's
// constants and the config.
pub fn write_environment(results: &Path, config: &Config) -> Result<(), SuiteError> {
    let mut environment = String::new();
    for (label, program, arguments) in [
        ("machine", "sysctl", &["-n", "hw.model"][..]),
        ("cpu", "sysctl", &["-n", "machdep.cpu.brand_string"]),
        ("memory bytes", "sysctl", &["-n", "hw.memsize"]),
        ("system", "sw_vers", &[]),
    ] {
        let output = Command::new(program)
            .args(arguments)
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
            .unwrap_or_else(|error| format!("unavailable: {error}"));
        writeln!(environment, "{label}: {output}").expect("writing to a string succeeds");
    }
    writeln!(
        environment,
        "stratafs build: node capacity {}, merge below {}%, largest inline entry {}",
        BenchTreeConfig::NODE_CAPACITY,
        BenchTreeConfig::MERGE_BELOW_PERCENT,
        BenchFsConfig::LARGEST_INLINE_ENTRY,
    )
    .expect("writing to a string succeeds");
    writeln!(
        environment,
        "runs: {}, clear caches: {}",
        config.runs, config.clear_caches
    )
    .expect("writing to a string succeeds");
    write(&results.join("environment.txt"), &environment)
}

// write_summary turns every target's results into one table per workload, writes it next to them and prints it.
pub fn write_summary(results: &Path, config: &Config) -> Result<(), SuiteError> {
    let mut workloads: Vec<String> = Vec::new();
    for target in &config.targets {
        let Ok(entries) = std::fs::read_dir(results.join(target.name())) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.path();
            if let Some(stem) = name.file_stem().and_then(|stem| stem.to_str())
                && name
                    .extension()
                    .is_some_and(|extension| extension == "json" || extension == "failed")
                && !workloads.iter().any(|known| known == stem)
            {
                workloads.push(stem.to_owned());
            }
        }
    }
    workloads.sort();
    let mut summary = String::from("| workload |");
    for target in &config.targets {
        write!(summary, " {} |", target.name()).expect("writing to a string succeeds");
    }
    summary.push_str("\n|---|");
    summary.push_str(&"---|".repeat(config.targets.len()));
    summary.push('\n');
    for workload in &workloads {
        write!(summary, "| {workload} |").expect("writing to a string succeeds");
        for target in &config.targets {
            let file = results.join(target.name()).join(format!("{workload}.json"));
            write!(summary, " {} |", cell(&file)).expect("writing to a string succeeds");
        }
        summary.push('\n');
    }
    println!("{summary}");
    write(&results.join("summary.md"), &summary)
}

// cell is one result as the table shows it: a timed workload's median and spread, or a fio job's throughput,
// operations per second and 99th percentile latency.
fn cell(file: &Path) -> String {
    if file.with_extension("failed").exists() {
        return format!(
            "failed, see {}",
            file.with_extension("log")
                .file_name()
                .expect("a result has a file name")
                .to_string_lossy()
        );
    }
    let Ok(text) = std::fs::read_to_string(file) else {
        return "not run".to_owned();
    };
    let Ok(json) = serde_json::from_str::<Value>(&text) else {
        return "unreadable".to_owned();
    };
    if let Some(result) = json["results"].get(0) {
        return format!(
            "{:.3} s ± {:.3}",
            result["median"].as_f64().unwrap_or(f64::NAN),
            result["stddev"].as_f64().unwrap_or(f64::NAN)
        );
    }
    let Some(job) = json["jobs"].get(0) else {
        return "unreadable".to_owned();
    };
    let side = if job["write"]["io_bytes"].as_u64().unwrap_or(0) > 0 {
        &job["write"]
    } else {
        &job["read"]
    };
    format!(
        "{:.0} MiB/s, {:.0} op/s, p99 {:.0} µs",
        side["bw_bytes"].as_f64().unwrap_or(0.0) / (1024.0 * 1024.0),
        side["iops"].as_f64().unwrap_or(0.0),
        side["clat_ns"]["percentile"]["99.000000"]
            .as_f64()
            .unwrap_or(f64::NAN)
            / 1000.0,
    )
}

fn write(file: &Path, text: &str) -> Result<(), SuiteError> {
    std::fs::write(file, text).map_err(|source| SuiteError::Io {
        action: format!("writing {}", file.display()),
        source,
    })
}
