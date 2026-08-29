//! Adversary process completion becomes one terminal operation and bounded artifacts.

use std::process::ExitStatus;
use std::time::{Duration, Instant};

use testlab_schema::{
    EnvironmentOperation, EnvironmentOperationId, EnvironmentOperationKind,
    EnvironmentOperationStatus,
};

use crate::compose_types::{ComposeArtifact, ComposeFailure, ComposePhase};

#[derive(Debug)]
pub(crate) enum WaitOutcome {
    Exited(ExitStatus),
    Failed(String),
    TimedOut(Option<ExitStatus>),
}

pub(crate) struct PhaseInput {
    pub(crate) operation_id: EnvironmentOperationId,
    pub(crate) program: String,
    pub(crate) args: Vec<String>,
    pub(crate) started_unix_ms: u64,
    pub(crate) elapsed: Duration,
    pub(crate) wait: WaitOutcome,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
    pub(crate) failure: Option<String>,
}

pub(crate) fn phase(input: PhaseInput) -> ComposePhase {
    let elapsed_ms = u64::try_from(input.elapsed.as_millis()).unwrap_or(u64::MAX);
    let (status, exit_code) = status(&input.wait, input.failure.is_some());
    let stdout_name = "protocol-adversary.jsonl".to_owned();
    let stderr_name = "protocol-adversary.stderr.txt".to_owned();
    let operation = EnvironmentOperation {
        id: input.operation_id,
        kind: EnvironmentOperationKind::ProtocolAdversary,
        program: input.program,
        args: input.args,
        started_unix_ms: input.started_unix_ms,
        completed_unix_ms: input.started_unix_ms.saturating_add(elapsed_ms),
        status,
        exit_code,
        stdout_artifact: Some(stdout_name.clone()),
        stderr_artifact: Some(stderr_name.clone()),
        diagnostic: input.failure.clone(),
    };
    ComposePhase {
        operations: vec![operation],
        artifacts: vec![
            ComposeArtifact {
                name: stdout_name,
                bytes: input.stdout,
            },
            ComposeArtifact {
                name: stderr_name,
                bytes: input.stderr,
            },
        ],
        failure: input
            .failure
            .map(|diagnostic| ComposeFailure::new("protocol_adversary_failed", diagnostic)),
    }
}

pub(crate) fn wait_failure(wait: &WaitOutcome, stderr: &[u8]) -> Option<String> {
    match wait {
        WaitOutcome::Exited(status) if status.success() => None,
        WaitOutcome::Exited(status) => Some(format!(
            "protocol adversary exited with {status}; stderr: {}",
            String::from_utf8_lossy(stderr)
        )),
        WaitOutcome::Failed(error) => Some(format!("wait for protocol adversary: {error}")),
        WaitOutcome::TimedOut(_) => {
            Some("protocol adversary exceeded its shutdown bound".to_owned())
        }
    }
}

pub(crate) fn failed_spawn(
    operation_id: EnvironmentOperationId,
    program: String,
    topic: String,
    started_unix_ms: u64,
    diagnostic: String,
) -> ComposePhase {
    phase(PhaseInput {
        operation_id,
        program,
        args: vec!["adversary-worker".to_owned(), "--topic".to_owned(), topic],
        started_unix_ms,
        elapsed: Duration::ZERO,
        wait: WaitOutcome::Failed(diagnostic.clone()),
        stdout: Vec::new(),
        stderr: Vec::new(),
        failure: Some(diagnostic),
    })
}

pub(crate) fn remaining(deadline: Instant) -> Result<Duration, ComposeFailure> {
    deadline
        .checked_duration_since(Instant::now())
        .ok_or_else(|| {
            ComposeFailure::new(
                "adversary_event_timeout",
                "adversary event deadline elapsed",
            )
        })
}

fn status(wait: &WaitOutcome, failed: bool) -> (EnvironmentOperationStatus, Option<i32>) {
    match wait {
        WaitOutcome::Exited(status) if status.success() && !failed => {
            (EnvironmentOperationStatus::Succeeded, status.code())
        }
        WaitOutcome::Exited(status) => (EnvironmentOperationStatus::Failed, status.code()),
        WaitOutcome::Failed(_) => (EnvironmentOperationStatus::Failed, None),
        WaitOutcome::TimedOut(status) => (
            EnvironmentOperationStatus::TimedOut,
            status.as_ref().and_then(ExitStatus::code),
        ),
    }
}
