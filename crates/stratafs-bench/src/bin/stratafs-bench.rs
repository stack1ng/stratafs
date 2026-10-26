use std::path::PathBuf;
use std::process::ExitCode;

use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures_lite::future::block_on;

#[derive(Parser)]
#[command(about = "Create stratafs volumes, and benchmark stratafs against other file systems")]
struct Command {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    #[command(
        about = "Make a new, empty volume in a store directory, making the store first if it has none, and print the volume's id. Nothing may have the store mounted."
    )]
    Create {
        #[arg(help = "The store directory")]
        store: PathBuf,
    },
    #[command(
        about = "Run the benchmark suite a config file describes, and print the summary of its results"
    )]
    Run {
        #[arg(help = "The config file, such as crates/stratafs-bench/bench.toml")]
        config: PathBuf,
    },
}

fn main() -> ExitCode {
    match Command::parse().action {
        Action::Create { store } => match block_on(stratafs_bench::create_volume(&store)) {
            Ok(volume) => {
                println!("{}", hex::encode(Bytes::from(volume)));
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{}", error_chain(&error));
                ExitCode::FAILURE
            }
        },
        Action::Run { config } => {
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
