// xtask runs the repository's own build tasks: `cargo xtask <task>`.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
struct Xtask {
    #[command(subcommand)]
    task: Task,
}

#[derive(Subcommand)]
enum Task {
    #[command(
        about = "Build the benchmark FSKit module and its create command with one configuration, then install the module for FSKit"
    )]
    Bundle(Configuration),
    #[command(
        about = "Build JuiceFS 1.4.2 for the benchmark, with the go-m1cpu fix its release lacks for M5 Macs, into target/bench/juicefs"
    )]
    Juicefs,
}

// Configuration is the build's tuning constants. Each one left out keeps stratafs-bench's default.
#[derive(Args)]
struct Configuration {
    #[arg(long, help = "The tree's node capacity in bytes, at most 65535")]
    node_capacity: Option<u32>,
    #[arg(long, help = "The fill below which a tree node merges, in percent")]
    merge_below_percent: Option<u8>,
    #[arg(long, help = "The largest entry the engine keeps inline, in bytes")]
    largest_inline_entry: Option<u32>,
}

const LSREGISTER: &str = "/System/Library/Frameworks/CoreServices.framework/Versions/Current/Frameworks/LaunchServices.framework/Versions/Current/Support/lsregister";

fn main() -> ExitCode {
    let result = match Xtask::parse().task {
        Task::Bundle(configuration) => bundle(&configuration),
        Task::Juicefs => juicefs(),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn bundle(configuration: &Configuration) -> Result<(), String> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask sits in the workspace root");
    let mut build = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
    build.current_dir(workspace).args([
        "build",
        "--release",
        "--package",
        "stratafs-bench",
        "--bins",
    ]);
    for (variable, value) in [
        (
            "STRATAFS_BENCH_NODE_CAPACITY",
            configuration.node_capacity.map(|value| value.to_string()),
        ),
        (
            "STRATAFS_BENCH_MERGE_BELOW_PERCENT",
            configuration
                .merge_below_percent
                .map(|value| value.to_string()),
        ),
        (
            "STRATAFS_BENCH_LARGEST_INLINE_ENTRY",
            configuration
                .largest_inline_entry
                .map(|value| value.to_string()),
        ),
    ] {
        match value {
            Some(value) => build.env(variable, value),
            None => build.env_remove(variable),
        };
    }
    run(&mut build)?;

    let built = workspace.join("target/release");
    let sources = workspace.join("crates/stratafs-bench/bundle");
    let app = workspace.join("target/bench/StrataBench.app");
    let appex = app.join("Contents/Extensions/StrataBench.appex");
    if app.exists() {
        std::fs::remove_dir_all(&app)
            .map_err(|error| format!("removing {}: {error}", app.display()))?;
    }
    for (from, to) in [
        (
            built.join("stratafs-bench-extension"),
            appex.join("Contents/MacOS/stratafs-bench-extension"),
        ),
        (
            sources.join("extension/Info.plist"),
            appex.join("Contents/Info.plist"),
        ),
        (
            built.join("stratafs-bench-host"),
            app.join("Contents/MacOS/stratafs-bench-host"),
        ),
        (
            sources.join("host/Info.plist"),
            app.join("Contents/Info.plist"),
        ),
    ] {
        copy(&from, &to)?;
    }
    // The module is signed before the app around it, each with the sandbox FSKit requires.
    let entitlements = sources.join("sandbox.entitlements");
    for signed in [&appex, &app] {
        run(Command::new("codesign")
            .args([
                "--force",
                "--sign",
                "-",
                "--timestamp=none",
                "--options",
                "runtime",
            ])
            .arg("--entitlements")
            .arg(&entitlements)
            .arg("--generate-entitlement-der")
            .arg(signed))?;
    }
    run(Command::new(LSREGISTER)
        .args(["-f", "-R", "-trusted"])
        .arg(&app))?;
    println!(
        "installed {}\nThe first time, enable \"stratafs benchmark\" in System Settings > General > Login Items & Extensions > File System Extensions.\nCreate volumes with {}",
        app.display(),
        built.join("stratafs-bench").display(),
    );
    Ok(())
}

// juicefs builds JuiceFS 1.4.2 with go-m1cpu 0.2.4, the first version that reads M5 Pro and Max processors
// instead of crashing at startup, as the 1.4.2 release does. It needs Go, which the nix shell has.
fn juicefs() -> Result<(), String> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask sits in the workspace root");
    let source = workspace.join("target/bench/juicefs-source");
    if !source.exists() {
        run(Command::new("git")
            .args(["clone", "--quiet", "--depth", "1", "--branch", "v1.4.2"])
            .arg("https://github.com/juicedata/juicefs.git")
            .arg(&source))?;
    }
    run(Command::new("go")
        .current_dir(&source)
        .args(["get", "github.com/shoenig/go-m1cpu@v0.2.4"]))?;
    let binary = workspace.join("target/bench/juicefs");
    run(Command::new("go")
        .current_dir(&source)
        .args(["build", "-o"])
        .arg(&binary)
        .arg("."))?;
    run(Command::new(&binary).arg("version"))
}

fn copy(from: &Path, to: &PathBuf) -> Result<(), String> {
    let directory = to.parent().expect("a bundle file sits in a directory");
    std::fs::create_dir_all(directory)
        .map_err(|error| format!("making {}: {error}", directory.display()))?;
    std::fs::copy(from, to)
        .map(|_| ())
        .map_err(|error| format!("copying {} to {}: {error}", from.display(), to.display()))
}

fn run(command: &mut Command) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|error| format!("starting {command:?}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{command:?} exited with {status}"))
    }
}
