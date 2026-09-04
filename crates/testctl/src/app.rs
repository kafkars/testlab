//! CLI routing keeps catalog validation separate from sealed run attempts.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

use crate::candidate::prepare_kafkars;
use crate::catalog::Repository;
use crate::qualification::run_qualification;
use crate::qualification_merge::aggregate_qualification;
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
    /// Runs the private external real-cluster network proxy protocol.
    #[command(hide = true)]
    NetworkProxyWorker {
        /// One broker route encoded as ORDINAL|LISTEN|UPSTREAM.
        #[arg(long, required = true)]
        route: Vec<String>,
    },
    /// Runs the private external Kafka adversary worker protocol.
    #[command(hide = true)]
    AdversaryWorker {
        /// Sole baseline topic exposed by the adversary.
        #[arg(long)]
        topic: String,
    },
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
        /// Repository-relative environment manifest.
        #[arg(long, default_value = "clusters/model-broker.toml")]
        environment: PathBuf,
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
        /// Repository-relative environment manifest.
        #[arg(long, default_value = "clusters/model-broker.toml")]
        environment: PathBuf,
        /// Repository-relative or absolute evidence directory.
        #[arg(long, default_value = "evidence")]
        evidence_dir: PathBuf,
    },
    /// Runs every cell in one reviewed qualification manifest.
    Qualify {
        /// Repository root.
        #[arg(long, default_value = ".")]
        root: PathBuf,
        /// Repository-relative qualification manifest.
        #[arg(long)]
        qualification: PathBuf,
        /// Repository-relative packaged subject manifest.
        #[arg(long)]
        subject: PathBuf,
        /// Repository-relative or absolute evidence directory.
        #[arg(long, default_value = "evidence")]
        evidence_dir: PathBuf,
        /// Execute only this cell, under a distinct shard qualification identity.
        #[arg(long)]
        cell: Option<String>,
    },
    /// Packages Kafkars public crates, builds the external adapter, and qualifies them.
    QualifyKafkars {
        /// Repository root.
        #[arg(long, default_value = ".")]
        root: PathBuf,
        /// Kafkars source checkout to package.
        #[arg(long)]
        kafkars_root: PathBuf,
        /// Repository-relative qualification manifest.
        #[arg(long, default_value = "qualifications/kafkars-pr.toml")]
        qualification: PathBuf,
        /// Repository-relative or absolute evidence directory.
        #[arg(long, default_value = "target/kafkars-candidate-evidence")]
        evidence_dir: PathBuf,
        /// Permit Cargo to package an uncommitted Kafkars checkout.
        #[arg(long)]
        allow_dirty: bool,
        /// Execute only this cell, under a distinct shard qualification identity.
        #[arg(long)]
        cell: Option<String>,
    },
    /// Verify all expected cell shards and seal one complete qualification.
    AggregateQualification {
        #[arg(long, default_value = ".")]
        root: PathBuf,
        #[arg(long)]
        qualification: PathBuf,
        /// Sealed shard roots; repeat once for every expected cell.
        #[arg(long, required = true)]
        shard: Vec<PathBuf>,
        #[arg(long, default_value = "evidence")]
        evidence_dir: PathBuf,
    },
}

fn run(cli: Cli) -> Result<bool, AppError> {
    match cli.command {
        Command::NetworkProxyWorker { route } => {
            testlab_environment::run_network_proxy_worker(&route)
                .map_err(|error| AppError::Catalog(error.to_string()))?;
            Ok(true)
        }
        Command::AdversaryWorker { topic } => {
            testlab_environment::run_adversary_worker(&topic)
                .map_err(|error| AppError::Catalog(error.to_string()))?;
            Ok(true)
        }
        Command::Validate { root } => validate_catalog(&root),
        Command::Run {
            root,
            scenario,
            subject,
            environment,
            evidence_dir,
        } => {
            let repository = Repository::open(&root)?;
            let run = run_scenario(
                &repository,
                &scenario,
                &subject,
                &environment,
                &evidence_dir,
            )?;
            println!("{:?} {}", run.verdict.status, run.path.display());
            Ok(run.verdict.is_passed())
        }
        Command::RunPack {
            root,
            pack,
            subject,
            environment,
            evidence_dir,
        } => {
            let repository = Repository::open(&root)?;
            let runs = run_pack(&repository, &pack, &subject, &environment, &evidence_dir)?;
            let mut passed = true;
            for run in runs {
                println!("{:?} {}", run.verdict.status, run.path.display());
                passed &= run.verdict.is_passed();
            }
            Ok(passed)
        }
        Command::Qualify {
            root,
            qualification,
            subject,
            evidence_dir,
            cell,
        } => {
            let repository = Repository::open(&root)?;
            let run = run_qualification(
                &repository,
                &qualification,
                &subject,
                &evidence_dir,
                cell.as_deref(),
            )?;
            println!("{:?} {}", run.status, run.path.display());
            Ok(run.status == testlab_schema::VerdictStatus::Passed)
        }
        Command::QualifyKafkars {
            root,
            kafkars_root,
            qualification,
            evidence_dir,
            allow_dirty,
            cell,
        } => {
            let repository = Repository::open(&root)?;
            let (_, manifest) = repository.load_qualification(&qualification)?;
            crate::qualification::select_qualification(&manifest, cell.as_deref())?;
            let candidate = prepare_kafkars(&repository, &kafkars_root, allow_dirty)?;
            eprintln!("prepared {}", candidate.directory.display());
            let run = run_qualification(
                &repository,
                &qualification,
                &candidate.subject_path,
                &evidence_dir,
                cell.as_deref(),
            )?;
            println!("{:?} {}", run.status, run.path.display());
            Ok(run.status == testlab_schema::VerdictStatus::Passed)
        }
        Command::AggregateQualification {
            root,
            qualification,
            shard,
            evidence_dir,
        } => {
            let repository = Repository::open(&root)?;
            let run = aggregate_qualification(&repository, &qualification, &shard, &evidence_dir)?;
            println!("{:?} {}", run.status, run.path.display());
            Ok(run.status == testlab_schema::VerdictStatus::Passed)
        }
    }
}

fn validate_catalog(root: &Path) -> Result<bool, AppError> {
    let repository = Repository::open(root)?;
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
