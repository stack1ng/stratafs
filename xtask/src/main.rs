// xtask runs the repository's own build tasks: `cargo xtask <task>`.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
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
        about = "Build stratafs-local and its FSKit module with one configuration, then install the module for FSKit"
    )]
    Bundle(Configuration),
    #[command(
        about = "Build JuiceFS 1.4.2 for the benchmark, with the go-m1cpu fix its release lacks for M5 Macs, into target/bench/juicefs"
    )]
    Juicefs,
    #[command(
        about = "Build and test stratafs-fuse and stratafs-local on Linux, in a Docker container that can mount FUSE volumes; extra arguments go to cargo test"
    )]
    FuseTest {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
    #[command(
        about = "Build stratafs-local and stratafs-e2e on Linux, in the Docker container stratafs-fuse's tests use, and run stratafs-e2e there with the arguments"
    )]
    E2e {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        arguments: Vec<String>,
    },
}

// Configuration is the build's tuning constants. Each one left out keeps stratafs-local's default.
#[derive(Args)]
struct Configuration {
    #[arg(long, help = "The tree's node capacity in bytes, at most 65535")]
    node_capacity: Option<u32>,
    #[arg(long, help = "The fill below which a tree node merges, in percent")]
    merge_below_percent: Option<u8>,
    #[arg(long, help = "The largest entry the engine keeps inline, in bytes")]
    largest_inline_entry: Option<u32>,
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
        Task::FuseTest { arguments } => fuse_test(&arguments),
        Task::E2e { arguments } => e2e(&arguments),
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
    let sources = workspace.join("crates/stratafs-local/bundle");
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
        "stratafs-local",
        "--bins",
    ]);
    for (variable, value) in [
        (
            "STRATAFS_LOCAL_NODE_CAPACITY",
            configuration.node_capacity.map(|value| value.to_string()),
        ),
        (
            "STRATAFS_LOCAL_MERGE_BELOW_PERCENT",
            configuration
                .merge_below_percent
                .map(|value| value.to_string()),
        ),
        (
            "STRATAFS_LOCAL_LARGEST_INLINE_ENTRY",
            configuration
                .largest_inline_entry
                .map(|value| value.to_string()),
        ),
        (
            "STRATAFS_LOCAL_DURABILITY",
            configuration.durability.clone(),
        ),
        ("STRATAFS_LOCAL_MDBX_MODE", configuration.mdbx_mode.clone()),
        (
            "STRATAFS_LOCAL_MDBX_SYNC_BYTES",
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
    // LaunchServices drops the app's registration when the app is gone or broken for long, or is registered again
    // while a volume is mounted or a module process lives, and the module then stays disabled until it is enabled in
    // System Settings. So the app is updated in place, each file swapped in whole, and signed again at once.
    let mounted = output(&mut Command::new("/sbin/mount"))?;
    if String::from_utf8_lossy(&mounted).contains("(stratafs") {
        return Err("a stratafs volume is mounted; unmount it before bundling".to_owned());
    }
    if Command::new("/usr/bin/pgrep")
        .args(["-f", "stratafs-local-module"])
        .output()
        .is_ok_and(|found| found.status.success())
    {
        return Err(
            "a stratafs-local-module process is still running; wait for it to exit, or stop it, before bundling"
                .to_owned(),
        );
    }
    for (from, to) in [
        (
            built.join("stratafs-local-module"),
            appex.join("Contents/MacOS/stratafs-local-module"),
        ),
        (
            sources.join("extension/Info.plist"),
            appex.join("Contents/Info.plist"),
        ),
        (
            built.join("stratafs-local-host"),
            app.join("Contents/MacOS/stratafs-local-host"),
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
        built.join("stratafs-local").display(),
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

// FUSE_IMAGE is the image stratafs-fuse is tested in, and the FUSE_* volumes keep the sources, cargo's downloads and the
// Linux build between runs, apart from the repository and its macOS target directory.
const FUSE_IMAGE: &str = "stratafs-fuse-test";
const FUSE_SOURCES: &str = "stratafs-fuse-sources";
const FUSE_CARGO_HOME: &str = "stratafs-fuse-cargo";
const FUSE_TARGET: &str = "stratafs-fuse-target";

// FUSE_LEFT_OUT are the workspace's paths the container has no use for, by tar's exclude patterns.
const FUSE_LEFT_OUT: [&str; 4] = ["./target", "./.git", "./.direnv", "node_modules"];

// fuse_test runs stratafs-fuse's tests in the Linux container.
fn fuse_test(arguments: &[String]) -> Result<(), String> {
    let mut command = vec![
        "cargo",
        "test",
        "--locked",
        "--package",
        "stratafs-fuse",
        "--package",
        "stratafs-local",
    ];
    command.extend(arguments.iter().map(String::as_str));
    in_linux_container(&command)
}

// e2e builds stratafs-fuse and stratafs-e2e in the Linux container, and runs stratafs-e2e there with arguments.
fn e2e(arguments: &[String]) -> Result<(), String> {
    let build = "cargo build --release --locked --package stratafs-local --package stratafs-e2e";
    let run = format!("{build} && exec /target/release/stratafs-e2e \"$@\"");
    let mut command = vec!["sh", "-c", &run, "stratafs-e2e"];
    command.extend(arguments.iter().map(String::as_str));
    in_linux_container(&command)
}

// in_linux_container builds the test image, copies the workspace's sources into a volume, and runs command in a
// container with /dev/fuse and the right to mount. The copy writes only files whose contents changed, so cargo rebuilds
// only those, and it needs nothing of Docker's file sharing.
fn in_linux_container(command: &[&str]) -> Result<(), String> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask sits in the workspace root");
    run(Command::new("docker")
        .current_dir(workspace)
        .args(["build", "--tag", FUSE_IMAGE, "--file"])
        .arg("crates/stratafs-fuse/Dockerfile")
        .arg("."))?;
    let mut archive = Command::new("tar");
    archive
        .current_dir(workspace)
        // Apple's tar would add each file's extended attributes as files of their own.
        .env("COPYFILE_DISABLE", "1")
        .args(["--create", "--no-xattrs", "--file", "-"]);
    for left_out in FUSE_LEFT_OUT {
        archive.arg(format!("--exclude={left_out}"));
    }
    let mut archiving = archive
        .arg(".")
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|error| format!("starting {archive:?}: {error}"))?;
    let sources = archiving.stdout.take().expect("tar's output is piped");
    let copied = run(Command::new("docker")
        .args(["run", "--rm", "--interactive", "--volume"])
        .arg(format!("{FUSE_SOURCES}:/stratafs"))
        .arg(FUSE_IMAGE)
        .args([
            "sh",
            "-c",
            "mkdir /copied && tar -x -C /copied && rsync --recursive --links --checksum --delete /copied/ /stratafs/",
        ])
        .stdin(sources));
    let archived = archiving
        .wait()
        .map_err(|error| format!("waiting for {archive:?}: {error}"))?;
    copied?;
    if !archived.success() {
        return Err(format!("{archive:?} exited with {archived}"));
    }
    run(Command::new("docker")
        .args([
            "run",
            "--rm",
            "--device",
            "/dev/fuse",
            "--cap-add",
            "SYS_ADMIN",
            "--security-opt",
            "apparmor=unconfined",
            "--env",
            "CARGO_TARGET_DIR=/target",
            "--volume",
        ])
        .arg(format!("{FUSE_SOURCES}:/stratafs"))
        .arg("--volume")
        .arg(format!("{FUSE_CARGO_HOME}:/root/.cargo"))
        .arg("--volume")
        .arg(format!("{FUSE_TARGET}:/target"))
        .arg(FUSE_IMAGE)
        .args(command))
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

// copy puts a copy of from at to, swapping it in whole, so to is never missing or half written.
fn copy(from: &Path, to: &PathBuf) -> Result<(), String> {
    let directory = to.parent().expect("a bundle file sits in a directory");
    std::fs::create_dir_all(directory)
        .map_err(|error| format!("making {}: {error}", directory.display()))?;
    let copied = PathBuf::from(format!("{}.copying", to.display()));
    std::fs::copy(from, &copied).map_err(|error| {
        format!(
            "copying {} to {}: {error}",
            from.display(),
            copied.display()
        )
    })?;
    std::fs::rename(&copied, to)
        .map_err(|error| format!("moving {} to {}: {error}", copied.display(), to.display()))
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
