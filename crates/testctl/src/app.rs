//! CLI routing keeps catalog validation separate from sealed run attempts.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::catalog::Repository;
use crate::run_error::AppError;
use crate::runner::{run_pack, run_scenario};

/// Parses process arguments and returns whether every requested check passed.
pub fn run_cli() -> Result<bool, AppError> {
    run(Cli::parse())
}

#[derive(Debug, Parser)]
#[command(
    name = "testctl",
    version,
    about = "black-box correctness runner for Kafka clients"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Validates every scenario, pack, subject, and contract manifest.
    Validate {
        /// Repository root.
        #[arg(long, default_value = ".")]
        root: PathBuf,
    },
    /// Runs one scenario against one packaged subject.
    Run {
        /// Repository root.
        #[arg(long, default_value = ".")]
        root: PathBuf,
        /// Repository-relative scenario manifest.
        #[arg(long)]
        scenario: PathBuf,
        /// Repository-relative subject manifest.
        #[arg(long)]
        subject: PathBuf,
        /// Repository-relative or absolute evidence directory.
        #[arg(long, default_value = "evidence")]
        evidence_dir: PathBuf,
    },
    /// Runs every scenario in one ordered pack.
    RunPack {
        /// Repository root.
        #[arg(long, default_value = ".")]
        root: PathBuf,
        /// Repository-relative pack manifest.
        #[arg(long)]
        pack: PathBuf,
        /// Repository-relative subject manifest.
        #[arg(long)]
        subject: PathBuf,
        /// Repository-relative or absolute evidence directory.
        #[arg(long, default_value = "evidence")]
        evidence_dir: PathBuf,
    },
}

fn run(cli: Cli) -> Result<bool, AppError> {
    match cli.command {
        Command::Validate { root } => {
            let repository = Repository::open(&root)?;
            let summary = repository.validate_all()?;
            println!(
                "validated {} scenarios, {} packs, {} subjects, {} environments, {} qualifications, and {} contracts",
                summary.scenarios,
                summary.packs,
                summary.subjects,
                summary.environments,
                summary.qualifications,
                summary.contracts
            );
            Ok(true)
        }
        Command::Run {
            root,
            scenario,
            subject,
            evidence_dir,
        } => {
            let repository = Repository::open(&root)?;
            let run = run_scenario(&repository, &scenario, &subject, &evidence_dir)?;
            println!("{:?} {}", run.verdict.status, run.path.display());
            Ok(run.verdict.is_passed())
        }
        Command::RunPack {
            root,
            pack,
            subject,
            evidence_dir,
        } => {
            let repository = Repository::open(&root)?;
            let runs = run_pack(&repository, &pack, &subject, &evidence_dir)?;
            let mut passed = true;
            for run in runs {
                println!("{:?} {}", run.verdict.status, run.path.display());
                passed &= run.verdict.is_passed();
            }
            Ok(passed)
        }
    }
}
