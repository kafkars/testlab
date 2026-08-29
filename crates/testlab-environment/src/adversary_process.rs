//! Parent supervision turns one external adversary process into sealed environment evidence.

use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{
    ADVERSARY_PROTOCOL_VERSION, AdversaryControlEnvelope, AdversaryEvent,
    ProtocolAdversaryObservation, ProtocolFaultAction,
};

use crate::adversary_process_evidence::{
    PhaseInput, WaitOutcome, failed_spawn, phase, remaining, wait_failure,
};
use crate::adversary_process_io::{ProcessMessage, ProcessReaders};
use crate::adversary_process_types::{AdversaryProcessRequest, RunningAdversary};
use crate::compose_types::{ComposeFailure, ComposePhase};

impl RunningAdversary {
    /// Starts the child and waits for its versioned ready event.
    pub fn start(
        request: AdversaryProcessRequest<'_>,
        timeout: Duration,
    ) -> Result<Self, ComposePhase> {
        match Self::spawn(request) {
            Ok(mut process) => match process.wait_ready(timeout) {
                Ok(()) => Ok(process),
                Err(error) => Err(process.finish_failed(&error)),
            },
            Err((request, error)) => Err(failed_spawn(
                request.operation_id,
                request.program.display().to_string(),
                request.topic.to_owned(),
                request.started_unix_ms,
                error,
            )),
        }
    }

    /// Returns the bound loopback Kafka endpoint.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Arms one validated control and waits for its exact acknowledgement.
    pub fn arm(
        &mut self,
        control: &ProtocolFaultAction,
        timeout: Duration,
    ) -> Result<(), ComposeFailure> {
        let envelope = AdversaryControlEnvelope {
            protocol_version: ADVERSARY_PROTOCOL_VERSION,
            control: control.clone(),
        };
        let encoded = serde_json::to_vec(&envelope).map_err(|error| {
            ComposeFailure::new("adversary_control_encode_failed", error.to_string())
        })?;
        let writer = self.stdin.as_mut().ok_or_else(|| {
            ComposeFailure::new("adversary_control_closed", "adversary stdin was closed")
        })?;
        writer
            .write_all(&encoded)
            .and_then(|()| writer.write_all(b"\n"))
            .and_then(|()| writer.flush())
            .map_err(|error| {
                ComposeFailure::new("adversary_control_write_failed", error.to_string())
            })?;
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            ComposeFailure::new("adversary_control_deadline_invalid", "deadline overflowed")
        })?;
        loop {
            match self.receive(remaining(deadline)?)? {
                AdversaryEvent::Armed { operation_id, .. }
                    if operation_id == control.operation_id =>
                {
                    return Ok(());
                }
                AdversaryEvent::Armed { operation_id, .. } => {
                    return Err(ComposeFailure::new(
                        "adversary_control_mismatch",
                        format!("expected {}, received {operation_id}", control.operation_id),
                    ));
                }
                _ => {}
            }
        }
    }

    /// Closes control input, supervises shutdown, and returns terminal evidence.
    pub fn finish(&mut self, timeout: Duration) -> ComposePhase {
        drop(self.stdin.take());
        let wait = self.wait(timeout);
        self.drain_events();
        let streams = self.readers.take().map(ProcessReaders::join);
        let (stdout, stderr) = streams.unwrap_or_else(|| {
            (
                Err("adversary stdout reader ownership was lost".to_owned()),
                Err("adversary stderr reader ownership was lost".to_owned()),
            )
        });
        let stdout_error = stdout.as_ref().err().cloned();
        let stderr_error = stderr.as_ref().err().cloned();
        let stdout = stdout.unwrap_or_default();
        let stderr = stderr.unwrap_or_default();
        let failure = self
            .fatal
            .clone()
            .or(stdout_error)
            .or(stderr_error)
            .or_else(|| wait_failure(&wait, &stderr));
        self.phase(wait, stdout, stderr, failure)
    }

    /// Drains and returns currently available independent observations.
    pub fn take_observations(&mut self) -> Vec<ProtocolAdversaryObservation> {
        self.drain_events();
        std::mem::take(&mut self.observations)
    }

    fn spawn(
        request: AdversaryProcessRequest<'_>,
    ) -> Result<Self, (AdversaryProcessRequest<'_>, String)> {
        let args = vec![
            "adversary-worker".to_owned(),
            "--topic".to_owned(),
            request.topic.to_owned(),
        ];
        let mut command = Command::new(request.program);
        command
            .args(&args)
            .current_dir(request.repository_root)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|error| (request.clone(), format!("spawn adversary: {error}")))?;
        let pipes = (child.stdin.take(), child.stdout.take(), child.stderr.take());
        let (Some(stdin), Some(stdout), Some(stderr)) = pipes else {
            let _ignored = child.kill();
            let _ignored = child.wait();
            return Err((
                request,
                "adversary process pipes were unavailable".to_owned(),
            ));
        };
        let readers = ProcessReaders::start(stdout, stderr).map_err(|error| {
            let _ignored = child.kill();
            let _ignored = child.wait();
            (request.clone(), error)
        })?;
        Ok(Self {
            child: Some(child),
            stdin: Some(BufWriter::new(stdin)),
            readers: Some(readers),
            endpoint: String::new(),
            observations: Vec::new(),
            fatal: None,
            operation_id: request.operation_id,
            program: request.program.display().to_string(),
            args,
            started_unix_ms: request.started_unix_ms,
            started: Instant::now(),
        })
    }

    fn wait_ready(&mut self, timeout: Duration) -> Result<(), ComposeFailure> {
        match self.receive(timeout)? {
            AdversaryEvent::Ready {
                protocol_version,
                endpoint,
            } if protocol_version == ADVERSARY_PROTOCOL_VERSION => {
                self.endpoint = endpoint;
                Ok(())
            }
            event => Err(ComposeFailure::new(
                "adversary_ready_invalid",
                format!("expected versioned ready event, received {event:?}"),
            )),
        }
    }

    fn receive(&mut self, timeout: Duration) -> Result<AdversaryEvent, ComposeFailure> {
        let readers = self.readers.as_ref().ok_or_else(|| {
            ComposeFailure::new("adversary_reader_lost", "event reader was unavailable")
        })?;
        match readers.events.recv_timeout(timeout) {
            Ok(ProcessMessage::Event(event)) => {
                self.record_event(&event);
                Ok(event)
            }
            Ok(ProcessMessage::Error(error)) => {
                Err(ComposeFailure::new("adversary_output_invalid", error))
            }
            Ok(ProcessMessage::Eof) => Err(ComposeFailure::new(
                "adversary_eof",
                "adversary exited before the expected event",
            )),
            Err(RecvTimeoutError::Timeout) => Err(ComposeFailure::new(
                "adversary_event_timeout",
                "adversary event wait exceeded its deadline",
            )),
            Err(RecvTimeoutError::Disconnected) => Err(ComposeFailure::new(
                "adversary_reader_lost",
                "adversary event reader disconnected",
            )),
        }
    }

    fn record_event(&mut self, event: &AdversaryEvent) {
        match event {
            AdversaryEvent::Observation { observation, .. } => {
                self.observations.push(observation.clone());
            }
            AdversaryEvent::Fatal { diagnostic, .. } => self.fatal = Some(diagnostic.clone()),
            AdversaryEvent::Ready { .. } | AdversaryEvent::Armed { .. } => {}
        }
    }

    fn drain_events(&mut self) {
        loop {
            let message = self
                .readers
                .as_ref()
                .map(|readers| readers.events.try_recv());
            match message {
                Some(Ok(ProcessMessage::Event(event))) => self.record_event(&event),
                Some(Ok(ProcessMessage::Error(error))) => self.fatal = Some(error),
                Some(
                    Ok(ProcessMessage::Eof) | Err(TryRecvError::Disconnected | TryRecvError::Empty),
                )
                | None => return,
            }
        }
    }

    fn wait(&mut self, timeout: Duration) -> WaitOutcome {
        let deadline = Instant::now().checked_add(timeout);
        loop {
            let Some(child) = self.child.as_mut() else {
                return WaitOutcome::Failed("child ownership was lost".to_owned());
            };
            match child.try_wait() {
                Ok(Some(status)) => {
                    self.child.take();
                    return WaitOutcome::Exited(status);
                }
                Err(error) => return WaitOutcome::Failed(error.to_string()),
                Ok(None) if deadline.is_some_and(|deadline| Instant::now() >= deadline) => {
                    let _ignored = child.kill();
                    let status = child.wait().ok();
                    self.child.take();
                    return WaitOutcome::TimedOut(status);
                }
                Ok(None) => thread::sleep(Duration::from_millis(5)),
            }
        }
    }

    fn finish_failed(mut self, error: &ComposeFailure) -> ComposePhase {
        self.fatal = Some(error.to_string());
        self.finish(Duration::from_secs(1))
    }

    fn phase(
        &self,
        wait: WaitOutcome,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        failure: Option<String>,
    ) -> ComposePhase {
        phase(PhaseInput {
            operation_id: self.operation_id.clone(),
            program: self.program.clone(),
            args: self.args.clone(),
            started_unix_ms: self.started_unix_ms,
            elapsed: self.started.elapsed(),
            wait,
            stdout,
            stderr,
            failure,
        })
    }
}

impl Drop for RunningAdversary {
    fn drop(&mut self) {
        drop(self.stdin.take());
        if let Some(mut child) = self.child.take() {
            let _ignored = child.kill();
            let _ignored = child.wait();
        }
    }
}
