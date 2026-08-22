//! Terminal supervision returns bounded evidence for every owned command.

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{
    EnvironmentOperation, EnvironmentOperationId, EnvironmentOperationKind,
    EnvironmentOperationStatus,
};

use crate::terminal_capture::{join_reader, spawn_reader};

const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// One non-secret terminal command owned by an environment driver.
pub struct TerminalRequest {
    /// Stable operation identity supplied before process creation.
    pub id: EnvironmentOperationId,
    /// Semantic operation class.
    pub kind: EnvironmentOperationKind,
    /// Executable name or path.
    pub program: String,
    /// Exact non-secret arguments.
    pub args: Vec<String>,
    /// Command working directory.
    pub current_directory: PathBuf,
    /// Environment values passed to the child but excluded from operation evidence.
    pub environment: Vec<(String, String)>,
    /// Diagnostic start time owned by testctl.
    pub started_unix_ms: u64,
    /// Maximum monotonic command duration.
    pub timeout: Duration,
    /// Sealed stdout artifact name.
    pub stdout_artifact: Option<String>,
    /// Sealed stderr artifact name.
    pub stderr_artifact: Option<String>,
}

impl Debug for TerminalRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalRequest")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("program", &self.program)
            .field("args", &self.args)
            .field("current_directory", &self.current_directory)
            .field(
                "environment_names",
                &self
                    .environment
                    .iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
            )
            .field("started_unix_ms", &self.started_unix_ms)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// Bounded terminal streams plus their correlated operation record.
pub struct TerminalOutput {
    /// Terminal operation evidence.
    pub operation: EnvironmentOperation,
    /// Bounded stdout bytes.
    pub stdout: Vec<u8>,
    /// Bounded stderr bytes.
    pub stderr: Vec<u8>,
}

impl TerminalOutput {
    /// Returns whether the child exited successfully and both streams settled.
    pub fn succeeded(&self) -> bool {
        self.operation.status == EnvironmentOperationStatus::Succeeded
    }
}

impl Debug for TerminalOutput {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalOutput")
            .field("operation", &self.operation)
            .field("stdout_bytes", &self.stdout.len())
            .field("stderr_bytes", &self.stderr.len())
            .finish()
    }
}

/// Runs one terminal command under a monotonic timeout and bounded stream capture.
pub fn run_terminal(request: TerminalRequest) -> TerminalOutput {
    let started = Instant::now();
    let mut command = Command::new(&request.program);
    command
        .args(&request.args)
        .current_dir(&request.current_directory)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in &request.environment {
        command.env(name, value);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return failed(request, started.elapsed(), None, &error.to_string()),
    };
    let pipes = (child.stdout.take(), child.stderr.take());
    let (Some(stdout), Some(stderr)) = pipes else {
        let _ = child.kill();
        let _ = child.wait();
        return failed(
            request,
            started.elapsed(),
            None,
            "terminal child pipes were unavailable",
        );
    };
    let stdout_reader = spawn_reader("testlab-terminal-stdout", stdout);
    let stderr_reader = spawn_reader("testlab-terminal-stderr", stderr);
    let (stdout_reader, stderr_reader) = match (stdout_reader, stderr_reader) {
        (Ok(stdout_reader), Ok(stderr_reader)) => (stdout_reader, stderr_reader),
        (stdout_reader, stderr_reader) => {
            let _ = child.kill();
            let _ = child.wait();
            let diagnostic = format!(
                "failed to start terminal readers: stdout={:?}, stderr={:?}",
                stdout_reader.err(),
                stderr_reader.err()
            );
            return failed(request, started.elapsed(), None, &diagnostic);
        }
    };
    let wait = wait_for_child(&mut child, request.timeout, started);
    let stdout = join_reader(stdout_reader, "stdout");
    let stderr = join_reader(stderr_reader, "stderr");
    finish(request, started.elapsed(), wait, stdout, stderr)
}

#[derive(Debug)]
enum WaitResult {
    Exited(ExitStatus),
    Failed(String),
    TimedOut(Option<ExitStatus>, Option<String>),
}

fn wait_for_child(
    child: &mut std::process::Child,
    timeout: Duration,
    started: Instant,
) -> WaitResult {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return WaitResult::Exited(status),
            Ok(None) if started.elapsed() < timeout => {
                thread::sleep(POLL_INTERVAL.min(timeout.saturating_sub(started.elapsed())));
            }
            Ok(None) => {
                let kill_error = child.kill().err().map(|error| error.to_string());
                let status = child.wait().ok();
                return WaitResult::TimedOut(status, kill_error);
            }
            Err(error) => return WaitResult::Failed(error.to_string()),
        }
    }
}

fn finish(
    request: TerminalRequest,
    elapsed: Duration,
    wait: WaitResult,
    stdout: Result<Vec<u8>, String>,
    stderr: Result<Vec<u8>, String>,
) -> TerminalOutput {
    let (stdout_bytes, stdout_error) = settle_reader(stdout);
    let (stderr_bytes, stderr_error) = settle_reader(stderr);
    let (status, exit_code, diagnostic) = terminal_status(
        wait,
        stdout_error.as_deref(),
        stderr_error.as_deref(),
        &stderr_bytes,
    );
    TerminalOutput {
        operation: operation(request, elapsed, status, exit_code, diagnostic),
        stdout: stdout_bytes,
        stderr: stderr_bytes,
    }
}

fn settle_reader(result: Result<Vec<u8>, String>) -> (Vec<u8>, Option<String>) {
    match result {
        Ok(bytes) => (bytes, None),
        Err(error) => (Vec::new(), Some(error)),
    }
}

fn terminal_status(
    wait: WaitResult,
    stdout_error: Option<&str>,
    stderr_error: Option<&str>,
    stderr_bytes: &[u8],
) -> (EnvironmentOperationStatus, Option<i32>, Option<String>) {
    let reader_error = stdout_error.or(stderr_error);
    match wait {
        WaitResult::Exited(status) if status.success() && reader_error.is_none() => {
            (EnvironmentOperationStatus::Succeeded, status.code(), None)
        }
        WaitResult::Exited(status) => (
            EnvironmentOperationStatus::Failed,
            status.code(),
            Some(reader_error.map_or_else(
                || {
                    bounded(&format!(
                        "terminal exited with {status}; stderr: {}",
                        String::from_utf8_lossy(stderr_bytes)
                    ))
                },
                ToOwned::to_owned,
            )),
        ),
        WaitResult::Failed(error) => (
            EnvironmentOperationStatus::Failed,
            None,
            Some(bounded(&format!("failed to wait for terminal: {error}"))),
        ),
        WaitResult::TimedOut(status, kill_error) => (
            EnvironmentOperationStatus::TimedOut,
            status.as_ref().and_then(ExitStatus::code),
            Some(bounded(&format!(
                "terminal exceeded timeout; kill error: {kill_error:?}"
            ))),
        ),
    }
}

fn failed(
    request: TerminalRequest,
    elapsed: Duration,
    exit_code: Option<i32>,
    diagnostic: &str,
) -> TerminalOutput {
    TerminalOutput {
        operation: operation(
            request,
            elapsed,
            EnvironmentOperationStatus::Failed,
            exit_code,
            Some(bounded(diagnostic)),
        ),
        stdout: Vec::new(),
        stderr: Vec::new(),
    }
}

fn operation(
    request: TerminalRequest,
    elapsed: Duration,
    status: EnvironmentOperationStatus,
    exit_code: Option<i32>,
    diagnostic: Option<String>,
) -> EnvironmentOperation {
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    EnvironmentOperation {
        id: request.id,
        kind: request.kind,
        program: request.program,
        args: request.args,
        started_unix_ms: request.started_unix_ms,
        completed_unix_ms: request.started_unix_ms.saturating_add(elapsed_ms),
        status,
        exit_code,
        stdout_artifact: request.stdout_artifact,
        stderr_artifact: request.stderr_artifact,
        diagnostic,
    }
}

fn bounded(value: &str) -> String {
    const MAX_DIAGNOSTIC_BYTES: usize = 4096;
    if value.len() <= MAX_DIAGNOSTIC_BYTES {
        return value.to_owned();
    }
    let mut end = MAX_DIAGNOSTIC_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}
