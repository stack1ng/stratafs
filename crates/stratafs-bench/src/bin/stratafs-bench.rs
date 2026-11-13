use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(about = "Benchmark stratafs against other file systems")]
struct Command {
    #[arg(help = "The config file, such as crates/stratafs-bench/bench.toml")]
    config: PathBuf,
}

fn main() -> ExitCode {
    let config = Command::parse().config;
    let ran = stratafs_bench::suite::config::Config::read(&config)
        .and_then(|config| stratafs_bench::suite::run(&config));
    match ran {
        Ok(results) => {
            println!("results are in {}", results.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", error_chain(&error));
            ExitCode::FAILURE
        }
    }
}

// error_chain is an error with each of its sources, the way a person needs to read it.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut chain = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        source = cause.source();
    }
    chain
}
