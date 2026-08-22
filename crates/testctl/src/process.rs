//! Adapter process ownership enforces bounded protocol I/O and clean termination.

use std::env;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

use testlab_schema::{AdapterEventEnvelope, CommandEnvelope, SubjectManifest};

use crate::process_io::ProcessReaders;
use crate::run_error::RunFailure;
use crate::time::Deadline;

const MAX_PROTOCOL_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct AdapterProcess {
    child: Option<Child>,
    stdin: Option<BufWriter<ChildStdin>>,
    readers: ProcessReaders,
}

impl AdapterProcess {
    pub(crate) fn spawn(
        repository_root: &Path,
        subject: &SubjectManifest,
    ) -> Result<Self, RunFailure> {
        let executable = resolve_executable(repository_root, &subject.command)?;
        let working_directory =
            resolve_working_directory(repository_root, subject.working_directory.as_deref())?;
        let mut command = Command::new(&executable);
        command
            .args(&subject.args)
            .current_dir(&working_directory)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear();
        configure_environment(&mut command, subject)?;
        let mut child = command.spawn().map_err(|error| {
            RunFailure::harness(
                "subject_spawn_failed",
                format!(
                    "failed to spawn {} in {}: {error}",
                    executable.display(),
                    working_directory.display()
                ),
            )
        })?;
        let pipes = (child.stdin.take(), child.stdout.take(), child.stderr.take());
        let (Some(stdin), Some(stdout), Some(stderr)) = pipes else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(RunFailure::harness(
                "subject_pipe_failed",
                "one or more subject process pipes were unavailable",
            ));
        };
        let readers = match crate::process_io::start(stdout, stderr) {
            Ok(readers) => readers,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        };
        Ok(Self {
            child: Some(child),
            stdin: Some(BufWriter::new(stdin)),
            readers,
        })
    }

    pub(crate) fn send(&mut self, command: &CommandEnvelope) -> Result<(), RunFailure> {
        let encoded = serde_json::to_vec(command).map_err(|error| {
            RunFailure::harness(
                "command_encode_failed",
                format!("failed to encode adapter command: {error}"),
            )
        })?;
        if encoded.len() > MAX_PROTOCOL_BYTES {
            return Err(RunFailure::harness(
                "command_too_large",
                "adapter command exceeded the 4 MiB protocol bound",
            ));
        }
        let writer = self.stdin.as_mut().ok_or_else(|| {
            RunFailure::harness("subject_stdin_closed", "adapter stdin was already closed")
        })?;
        writer
            .write_all(&encoded)
            .map_err(|error| write_failure(&error))?;
        writer
            .write_all(b"\n")
            .map_err(|error| write_failure(&error))?;
        writer.flush().map_err(|error| write_failure(&error))
    }

    pub(crate) fn receive(
        &self,
        deadline: Deadline,
    ) -> Result<Option<AdapterEventEnvelope>, RunFailure> {
        let remaining = deadline.remaining()?;
        match self.readers.events.recv_timeout(remaining) {
            Ok(ProcessMessage::Event(event)) => Ok(Some(event)),
            Ok(ProcessMessage::Error(diagnostic)) => {
                Err(RunFailure::protocol("adapter_output_invalid", diagnostic))
            }
            Ok(ProcessMessage::Eof) => Ok(None),
            Err(RecvTimeoutError::Timeout) => Err(RunFailure::harness(
                "scenario_timeout",
                "adapter event wait exceeded the scenario deadline",
            )),
            Err(RecvTimeoutError::Disconnected) => Err(RunFailure::harness(
                "subject_reader_lost",
                "adapter stdout reader disconnected without EOF",
            )),
        }
    }

    pub(crate) fn close_input(&mut self) -> Result<(), RunFailure> {
        if let Some(mut stdin) = self.stdin.take() {
            stdin.flush().map_err(|error| write_failure(&error))?;
        }
        Ok(())
    }

    pub(crate) fn wait_success(&mut self, deadline: Deadline) -> Result<String, RunFailure> {
        self.close_input()?;
        let status = loop {
            let child = self.child.as_mut().ok_or_else(|| {
                RunFailure::harness(
                    "subject_ownership_lost",
                    "adapter child process was unavailable",
                )
            })?;
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {
                    let remaining = deadline.remaining()?;
                    thread::sleep(remaining.min(Duration::from_millis(5)));
                }
                Err(error) => {
                    return Err(RunFailure::harness(
                        "subject_wait_failed",
                        format!("failed to wait for adapter: {error}"),
                    ));
                }
            }
        };
        drop(self.child.take());
        let stderr = match self.readers.stderr.recv_timeout(deadline.remaining()?) {
            Ok(stderr) => stderr,
            Err(RecvTimeoutError::Timeout) => {
                return Err(RunFailure::harness(
                    "subject_stderr_timeout",
                    "adapter stderr capture did not settle before the deadline",
                ));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(RunFailure::harness(
                    "subject_stderr_lost",
                    "adapter stderr reader disconnected without a result",
                ));
            }
        };
        if status.success() {
            Ok(stderr)
        } else {
            Err(RunFailure::harness(
                "subject_exit_failed",
                format!("adapter exited with {status}; stderr: {stderr}"),
            ))
        }
    }

    fn terminate(&mut self) {
        drop(self.stdin.take());
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for AdapterProcess {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[derive(Debug)]
pub(crate) enum ProcessMessage {
    Event(AdapterEventEnvelope),
    Error(String),
    Eof,
}

fn resolve_executable(root: &Path, command: &str) -> Result<PathBuf, RunFailure> {
    let configured = Path::new(command);
    let candidate = if configured.is_absolute() {
        configured.to_path_buf()
    } else {
        root.join(configured)
    };
    let executable = fs::canonicalize(&candidate).map_err(|error| {
        RunFailure::harness(
            "subject_executable_missing",
            format!("failed to resolve {}: {error}", candidate.display()),
        )
    })?;
    if !configured.is_absolute() && !executable.starts_with(root) {
        return Err(RunFailure::harness(
            "subject_executable_escaped",
            format!("relative subject executable escaped repository root: {command}"),
        ));
    }
    Ok(executable)
}

fn resolve_working_directory(root: &Path, configured: Option<&str>) -> Result<PathBuf, RunFailure> {
    let candidate = configured.map_or_else(|| root.to_path_buf(), |value| root.join(value));
    let directory = fs::canonicalize(&candidate).map_err(|error| {
        RunFailure::harness(
            "subject_working_directory_missing",
            format!("failed to resolve {}: {error}", candidate.display()),
        )
    })?;
    if !directory.starts_with(root) {
        return Err(RunFailure::harness(
            "subject_working_directory_escaped",
            format!(
                "subject working directory escaped repository root: {}",
                directory.display()
            ),
        ));
    }
    Ok(directory)
}

fn configure_environment(
    command: &mut Command,
    subject: &SubjectManifest,
) -> Result<(), RunFailure> {
    #[cfg(windows)]
    for name in ["SYSTEMROOT", "WINDIR"] {
        if let Some(value) = env::var_os(name) {
            command.env(name, value);
        }
    }
    for (name, value) in &subject.environment {
        command.env(name, value);
    }
    for name in &subject.pass_environment {
        let value = env::var_os(name).ok_or_else(|| {
            RunFailure::harness(
                "subject_environment_missing",
                format!("required pass-through environment variable {name} is unset"),
            )
        })?;
        command.env(name, value);
    }
    Ok(())
}

fn write_failure(error: &std::io::Error) -> RunFailure {
    RunFailure::harness(
        "subject_write_failed",
        format!("failed to write adapter command: {error}"),
    )
}
