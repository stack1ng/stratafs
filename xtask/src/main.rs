// xtask runs the repository's own build tasks: `cargo xtask <task>`.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::SystemTime;

use clap::{Args, Parser, Subcommand};
use plist::{Data, Date, Dictionary, Value};
use serde::Deserialize;
use sha1::{Digest, Sha1};

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
    #[arg(long, help = "The store: redb, sqlite or mdbx")]
    store: Option<String>,
    #[arg(long, help = "How the mdbx store syncs: safe or utterly")]
    mdbx_mode: Option<String>,
    #[arg(
        long,
        help = "How many bytes the mdbx store writes between its durable points in safe mode"
    )]
    mdbx_sync_bytes: Option<u64>,
    #[arg(
        long,
        help = "The most the engine makes commits durable against: none, software or power"
    )]
    durability: Option<String>,
}

const LSREGISTER: &str = "/System/Library/Frameworks/CoreServices.framework/Versions/Current/Frameworks/LaunchServices.framework/Versions/Current/Support/lsregister";

// PROFILES is where Xcode keeps the provisioning profiles it makes, under the home directory.
const PROFILES: &str = "Library/Developer/Xcode/UserData/Provisioning Profiles";

// Profile is the part of a provisioning profile the bundle uses.
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Profile {
    team_identifier: Vec<String>,
    expiration_date: Date,
    entitlements: Dictionary,
    developer_certificates: Vec<Data>,
    provisioned_devices: Option<Vec<String>>,
    provisions_all_devices: Option<bool>,
}

// Hardware is the part of system_profiler's hardware report the bundle uses.
#[derive(Deserialize)]
struct Hardware {
    #[serde(rename = "SPHardwareDataType")]
    reports: Vec<HardwareReport>,
}

#[derive(Deserialize)]
struct HardwareReport {
    #[serde(rename = "provisioning_UDID")]
    provisioning_udid: String,
}

// Signing is what the module is signed with: a signing identity in the keychain, by its certificate's SHA-1, a
// provisioning profile that lets that identity sign the module on this Mac, and the entitlements from that profile
// the module's signature must carry.
struct Signing {
    identity: String,
    profile: PathBuf,
    entitlements: Dictionary,
}

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
    let sources = workspace.join("crates/stratafs-bench/bundle");
    // FSKit lets the module invalidate what the kernel caches only when a provisioning profile grants it FSKit's
    // module entitlement, so the profile is found before anything is built.
    let info: Dictionary = plist::from_file(sources.join("extension/Info.plist"))
        .map_err(|error| format!("reading the module's Info.plist: {error}"))?;
    let module = info
        .get("CFBundleIdentifier")
        .and_then(Value::as_string)
        .ok_or("the module's Info.plist has no CFBundleIdentifier")?;
    let signing = signing(module)?;
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
        ("STRATAFS_BENCH_STORE", configuration.store.clone()),
        (
            "STRATAFS_BENCH_DURABILITY",
            configuration.durability.clone(),
        ),
        ("STRATAFS_BENCH_MDBX_MODE", configuration.mdbx_mode.clone()),
        (
            "STRATAFS_BENCH_MDBX_SYNC_BYTES",
            configuration.mdbx_sync_bytes.map(|value| value.to_string()),
        ),
    ] {
        match value {
            Some(value) => build.env(variable, value),
            None => build.env_remove(variable),
        };
    }
    run(&mut build)?;

    let built = workspace.join("target/release");
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
    copy(
        &signing.profile,
        &appex.join("Contents/embedded.provisionprofile"),
    )?;
    let module_entitlements = workspace.join("target/bench/extension.entitlements");
    let mut entitlements: Dictionary = plist::from_file(sources.join("extension.entitlements"))
        .map_err(|error| format!("reading extension.entitlements: {error}"))?;
    entitlements.extend(signing.entitlements);
    plist::to_file_xml(&module_entitlements, &entitlements)
        .map_err(|error| format!("writing {}: {error}", module_entitlements.display()))?;
    // The module is signed before the app around it, each with the sandbox FSKit requires.
    for (signed, entitlements) in [
        (&appex, module_entitlements),
        (&app, sources.join("sandbox.entitlements")),
    ] {
        run(Command::new("codesign")
            .args([
                "--force",
                "--sign",
                &signing.identity,
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

// signing finds Xcode's provisioning profile for the module on this Mac and a signing identity in the keychain
// that it lets sign the module, keeping the profile that expires last.
fn signing(module: &str) -> Result<Signing, String> {
    let identities = String::from_utf8(output(Command::new("security").args([
        "find-identity",
        "-v",
        "-p",
        "codesigning",
    ]))?)
    .map_err(|error| format!("reading the signing identities: {error}"))?;
    let identities: Vec<&str> = identities
        .lines()
        .filter_map(|line| line.split_whitespace().nth(1))
        .filter(|hash| hash.len() == 40)
        .collect();
    let hardware: Hardware = serde_json::from_slice(&output(
        Command::new("system_profiler").args(["SPHardwareDataType", "-json"]),
    )?)
    .map_err(|error| format!("reading the hardware report: {error}"))?;
    let [
        HardwareReport {
            provisioning_udid: device,
        },
    ] = &hardware.reports[..]
    else {
        return Err("the hardware report does not describe one Mac".to_owned());
    };
    let profiles = std::env::home_dir()
        .ok_or("there is no home directory")?
        .join(PROFILES);
    let entries = std::fs::read_dir(&profiles)
        .map_err(|error| format!("reading {}: {error}", profiles.display()))?;
    let mut found: Option<(SystemTime, Signing)> = None;
    for entry in entries {
        let path = entry
            .map_err(|error| format!("reading {}: {error}", profiles.display()))?
            .path();
        if path
            .extension()
            .is_none_or(|extension| extension != "provisionprofile")
        {
            continue;
        }
        let decoded = output(
            Command::new("security")
                .args(["cms", "-D", "-i"])
                .arg(&path),
        )?;
        let profile: Profile = plist::from_bytes(&decoded)
            .map_err(|error| format!("reading {}: {error}", path.display()))?;
        let [team] = &profile.team_identifier[..] else {
            continue;
        };
        let application = format!("{team}.{module}");
        let expires = SystemTime::from(profile.expiration_date);
        let entitlements = &profile.entitlements;
        let runs_here = profile.provisions_all_devices == Some(true)
            || profile
                .provisioned_devices
                .is_some_and(|devices| devices.contains(device));
        if entitlements.get("com.apple.application-identifier")
            != Some(&Value::from(application.clone()))
            || entitlements.get("com.apple.developer.fskit.fsmodule") != Some(&Value::Boolean(true))
            || !runs_here
            || expires <= SystemTime::now()
            || found.as_ref().is_some_and(|(kept, _)| *kept >= expires)
        {
            continue;
        }
        let Some(identity) = profile
            .developer_certificates
            .iter()
            .find_map(|certificate| {
                let hash = hex::encode_upper(Sha1::digest(certificate));
                identities.contains(&hash.as_str()).then_some(hash)
            })
        else {
            continue;
        };
        let entitlements = Dictionary::from_iter([
            (
                "com.apple.application-identifier".to_owned(),
                Value::String(application),
            ),
            (
                "com.apple.developer.team-identifier".to_owned(),
                Value::String(team.clone()),
            ),
        ]);
        found = Some((
            expires,
            Signing {
                identity,
                profile: path,
                entitlements,
            },
        ));
    }
    found.map(|(_, signing)| signing).ok_or_else(|| {
        format!(
            "{} has no unexpired provisioning profile for {module} on this Mac with FSKit's module entitlement and a signing identity in the keychain; make one in Xcode",
            profiles.display()
        )
    })
}

fn copy(from: &Path, to: &PathBuf) -> Result<(), String> {
    let directory = to.parent().expect("a bundle file sits in a directory");
    std::fs::create_dir_all(directory)
        .map_err(|error| format!("making {}: {error}", directory.display()))?;
    std::fs::copy(from, to)
        .map(|_| ())
        .map_err(|error| format!("copying {} to {}: {error}", from.display(), to.display()))
}

// output runs command and returns what it printed.
fn output(command: &mut Command) -> Result<Vec<u8>, String> {
    let output = command
        .output()
        .map_err(|error| format!("starting {command:?}: {error}"))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "{command:?} exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
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
