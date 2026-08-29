//! Proxy shutdown retains one terminal operation and bounded process streams.

use std::process::ExitStatus;
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus};

use crate::compose_types::{ComposeArtifact, ComposePhase};
use crate::network_proxy_process_io::{NetworkProcessMessage, NetworkProcessReaders};
use crate::network_proxy_process_types::{
    NetworkProxyFinish, NetworkProxyProcessRequest, RunningNetworkProxy,
};

impl RunningNetworkProxy {
    /// Closes control input, waits for the child, and retains bounded streams.
    pub fn finish(mut self, timeout: Duration) -> NetworkProxyFinish {
        self.stdin.take();
        let wait = self.wait_child(timeout);
        let readers = self.readers.take().map(NetworkProcessReaders::join);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut reader_error = None;
        if let Some(readers) = readers {
            for message in readers.messages {
                self.record_message(message);
            }
            match readers.stdout {
                Ok(bytes) => stdout = bytes,
                Err(error) => reader_error = Some(error),
            }
            match readers.stderr {
                Ok(bytes) => stderr = bytes,
                Err(error) if reader_error.is_none() => reader_error = Some(error),
                Err(_) => {}
            }
        }
        let (status, exit_code, mut diagnostic) = wait_status(wait);
        if diagnostic.is_none() {
            diagnostic = self.fatal.clone().or(reader_error);
        }
        let status = if diagnostic.is_some() && status == EnvironmentOperationStatus::Succeeded {
            EnvironmentOperationStatus::Failed
        } else {
            status
        };
        let mut phase = ComposePhase::default();
        phase.artifacts.extend([
            ComposeArtifact {
                name: "network-proxy.jsonl".to_owned(),
                bytes: stdout,
            },
            ComposeArtifact {
                name: "network-proxy.stderr.txt".to_owned(),
                bytes: stderr,
            },
        ]);
        phase.operations.push(EnvironmentOperation {
            id: self.operation_id,
            kind: EnvironmentOperationKind::NetworkProxy,
            program: self.program,
            args: self.args,
            started_unix_ms: self.started_unix_ms,
            completed_unix_ms: completed(self.started_unix_ms, self.started.elapsed()),
            status,
            exit_code,
            stdout_artifact: Some("network-proxy.jsonl".to_owned()),
            stderr_artifact: Some("network-proxy.stderr.txt".to_owned()),
            diagnostic: diagnostic.clone(),
        });
        if let Some(diagnostic) = diagnostic {
            phase.fail("network_proxy_failed", diagnostic);
        }
        NetworkProxyFinish {
            phase,
            observations: self.observations,
        }
    }

    fn record_message(&mut self, message: NetworkProcessMessage) {
        match message {
            NetworkProcessMessage::Event(event) => {
                let _ = self.record_event(event);
            }
            NetworkProcessMessage::Error(error) => self.fatal = Some(error),
            NetworkProcessMessage::Eof => {}
        }
    }

    fn wait_child(&mut self, timeout: Duration) -> WaitOutcome {
        let deadline = Instant::now().checked_add(timeout);
        loop {
            let Some(child) = self.child.as_mut() else {
                return WaitOutcome::Failed("network proxy child was unavailable".to_owned());
            };
            match child.try_wait() {
                Ok(Some(status)) => return WaitOutcome::Exited(status),
                Ok(None) if deadline.is_some_and(|value| Instant::now() < value) => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    let kill = child.kill().err().map(|error| error.to_string());
                    let status = child.wait().ok();
                    return WaitOutcome::TimedOut(status, kill);
                }
                Err(error) => return WaitOutcome::Failed(error.to_string()),
            }
        }
    }
}

enum WaitOutcome {
    Exited(ExitStatus),
    TimedOut(Option<ExitStatus>, Option<String>),
    Failed(String),
}

fn wait_status(wait: WaitOutcome) -> (EnvironmentOperationStatus, Option<i32>, Option<String>) {
    match wait {
        WaitOutcome::Exited(status) if status.success() => {
            (EnvironmentOperationStatus::Succeeded, status.code(), None)
        }
        WaitOutcome::Exited(status) => (
            EnvironmentOperationStatus::Failed,
            status.code(),
            Some(format!("network proxy exited with {status}")),
        ),
        WaitOutcome::TimedOut(status, kill) => (
            EnvironmentOperationStatus::TimedOut,
            status.and_then(|value| value.code()),
            Some(format!("network proxy timed out; kill error: {kill:?}")),
        ),
        WaitOutcome::Failed(error) => (
            EnvironmentOperationStatus::Failed,
            None,
            Some(format!("wait for network proxy: {error}")),
        ),
    }
}

pub(super) fn failed_start(
    request: &NetworkProxyProcessRequest<'_>,
    error: String,
) -> ComposePhase {
    let mut phase = ComposePhase::default();
    phase.operations.push(EnvironmentOperation {
        id: request.operation_id.clone(),
        kind: EnvironmentOperationKind::NetworkProxy,
        program: request.program.display().to_string(),
        args: vec!["network-proxy-worker".to_owned()],
        started_unix_ms: request.started_unix_ms,
        completed_unix_ms: request.started_unix_ms,
        status: EnvironmentOperationStatus::Failed,
        exit_code: None,
        stdout_artifact: None,
        stderr_artifact: None,
        diagnostic: Some(error.clone()),
    });
    phase.fail("network_proxy_start_failed", error);
    phase
}

fn completed(started: u64, elapsed: Duration) -> u64 {
    started.saturating_add(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}
