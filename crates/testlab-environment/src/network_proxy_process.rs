//! Parent supervision turns the proxy child into sealed environment evidence.

use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

use testlab_schema::{
    NETWORK_PROXY_PROTOCOL_VERSION, NetworkFaultState, NetworkProxyControl,
    NetworkProxyControlEnvelope, NetworkProxyEvent, NetworkProxyObservation,
};

use crate::compose_types::{ComposeFailure, ComposePhase};
use crate::network_proxy_process_finish::failed_start;
use crate::network_proxy_process_io::{NetworkProcessMessage, NetworkProcessReaders};
use crate::network_proxy_process_types::{NetworkProxyProcessRequest, RunningNetworkProxy};

impl RunningNetworkProxy {
    /// Starts the child and waits for its exact versioned route acknowledgement.
    pub fn start(
        request: &NetworkProxyProcessRequest<'_>,
        timeout: Duration,
    ) -> Result<Self, ComposePhase> {
        let mut process = match Self::spawn(request) {
            Ok(process) => process,
            Err(error) => return Err(failed_start(request, error)),
        };
        match process.wait_ready(timeout) {
            Ok(()) => Ok(process),
            Err(error) => {
                let mut finish = process.finish(timeout);
                finish.phase.fail("network_proxy_start_failed", error);
                Err(finish.phase)
            }
        }
    }

    /// Sends one exact control and waits for its matching acknowledgement.
    pub fn control(
        &mut self,
        control: &NetworkProxyControl,
        timeout: Duration,
    ) -> Result<(), ComposeFailure> {
        let envelope = NetworkProxyControlEnvelope {
            protocol_version: NETWORK_PROXY_PROTOCOL_VERSION,
            control: control.clone(),
        };
        let encoded = serde_json::to_vec(&envelope).map_err(|error| {
            ComposeFailure::new("network_proxy_control_encode_failed", error.to_string())
        })?;
        let writer = self.stdin.as_mut().ok_or_else(|| {
            ComposeFailure::new("network_proxy_control_closed", "network proxy stdin closed")
        })?;
        writer
            .write_all(&encoded)
            .and_then(|()| writer.write_all(b"\n"))
            .and_then(|()| writer.flush())
            .map_err(|error| {
                ComposeFailure::new("network_proxy_control_write_failed", error.to_string())
            })?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| ComposeFailure::new("network_proxy_deadline_invalid", "overflow"))?;
        let event = self.next_event(deadline)?;
        match matching_ack(control, event) {
            Ok(ControlAck::Applied) => Ok(()),
            Ok(ControlAck::Effect(observation)) => {
                self.observations.push(observation);
                Ok(())
            }
            Err(error) => {
                self.fatal = Some(error.to_string());
                Err(error)
            }
        }
    }

    /// Drains observations acknowledged since the prior call.
    pub fn take_observations(&mut self) -> Vec<NetworkProxyObservation> {
        std::mem::take(&mut self.observations)
    }

    fn spawn(request: &NetworkProxyProcessRequest<'_>) -> Result<Self, String> {
        let mut args = vec!["network-proxy-worker".to_owned()];
        for route in request.routes {
            args.extend([
                "--route".to_owned(),
                format!(
                    "{}|{}|{}",
                    route.broker_ordinal, route.listen_endpoint, route.upstream_endpoint
                ),
            ]);
        }
        let mut child = Command::new(request.program)
            .args(&args)
            .current_dir(request.repository_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("spawn network proxy: {error}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "network proxy stdin was unavailable".to_owned())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "network proxy stdout was unavailable".to_owned())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "network proxy stderr was unavailable".to_owned())?;
        let readers = NetworkProcessReaders::start(stdout, stderr)?;
        Ok(Self {
            child: Some(child),
            stdin: Some(BufWriter::new(stdin)),
            readers: Some(readers),
            routes: request.routes.to_vec(),
            observations: Vec::new(),
            fatal: None,
            operation_id: request.operation_id.clone(),
            program: request.program.display().to_string(),
            args,
            started_unix_ms: request.started_unix_ms,
            started: Instant::now(),
        })
    }

    fn wait_ready(&mut self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| "network proxy ready deadline overflowed".to_owned())?;
        loop {
            let event = self
                .next_event(deadline)
                .map_err(|error| error.to_string())?;
            match event {
                NetworkProxyEvent::Ready {
                    protocol_version,
                    routes,
                } if protocol_version == NETWORK_PROXY_PROTOCOL_VERSION
                    && routes == self.routes =>
                {
                    return Ok(());
                }
                other => self.record_event(other)?,
            }
        }
    }

    fn next_event(&mut self, deadline: Instant) -> Result<NetworkProxyEvent, ComposeFailure> {
        let result = self.receive_event(deadline);
        if let Err(error) = &result {
            self.fatal = Some(error.to_string());
        }
        result
    }

    fn receive_event(&self, deadline: Instant) -> Result<NetworkProxyEvent, ComposeFailure> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let readers = self.readers.as_ref().ok_or_else(|| {
            ComposeFailure::new(
                "network_proxy_reader_closed",
                "network proxy readers closed",
            )
        })?;
        match readers.events.recv_timeout(remaining) {
            Ok(NetworkProcessMessage::Event(event)) => Ok(event),
            Ok(NetworkProcessMessage::Error(error)) => {
                Err(ComposeFailure::new("network_proxy_protocol_failed", error))
            }
            Ok(NetworkProcessMessage::Eof) => Err(ComposeFailure::new(
                "network_proxy_eof",
                "network proxy exited before acknowledgement",
            )),
            Err(RecvTimeoutError::Timeout) => Err(ComposeFailure::new(
                "network_proxy_control_timeout",
                "network proxy acknowledgement timed out",
            )),
            Err(RecvTimeoutError::Disconnected) => Err(ComposeFailure::new(
                "network_proxy_reader_closed",
                "network proxy event reader disconnected",
            )),
        }
    }

    pub(crate) fn record_event(&mut self, event: NetworkProxyEvent) -> Result<(), String> {
        match event {
            NetworkProxyEvent::Fatal {
                protocol_version,
                code,
                diagnostic,
            } if protocol_version == NETWORK_PROXY_PROTOCOL_VERSION => {
                let fatal = format!("{code}: {diagnostic}");
                self.fatal = Some(fatal.clone());
                Err(fatal)
            }
            other => {
                let fatal = format!("unexpected network proxy event {other:?}");
                self.fatal = Some(fatal.clone());
                Err(fatal)
            }
        }
    }
}

fn matching_ack(
    control: &NetworkProxyControl,
    event: NetworkProxyEvent,
) -> Result<ControlAck, ComposeFailure> {
    let matched = match (control, event) {
        (
            NetworkProxyControl::AlterFault(action),
            NetworkProxyEvent::FaultApplied {
                protocol_version,
                operation_id,
            },
        ) if action.state == NetworkFaultState::Present
            && protocol_version == NETWORK_PROXY_PROTOCOL_VERSION
            && operation_id == action.operation_id =>
        {
            ControlAck::Applied
        }
        (
            NetworkProxyControl::AlterFault(action),
            NetworkProxyEvent::FaultRemoved {
                protocol_version,
                observation,
            },
        ) if action.state == NetworkFaultState::Absent
            && protocol_version == NETWORK_PROXY_PROTOCOL_VERSION
            && observation_id(&observation) == &action.operation_id =>
        {
            ControlAck::Effect(observation)
        }
        (
            NetworkProxyControl::CutConnections(action),
            NetworkProxyEvent::ConnectionsCut {
                protocol_version,
                observation,
            },
        ) if protocol_version == NETWORK_PROXY_PROTOCOL_VERSION
            && observation_id(&observation) == &action.operation_id =>
        {
            ControlAck::Effect(observation)
        }
        (
            _,
            NetworkProxyEvent::Fatal {
                code, diagnostic, ..
            },
        ) => {
            return Err(ComposeFailure::new(
                "network_proxy_control_failed",
                format!("{code}: {diagnostic}"),
            ));
        }
        (_, other) => {
            return Err(ComposeFailure::new(
                "network_proxy_protocol_failed",
                format!("unexpected proxy acknowledgement {other:?}"),
            ));
        }
    };
    Ok(matched)
}

enum ControlAck {
    Applied,
    Effect(NetworkProxyObservation),
}

fn observation_id(
    observation: &NetworkProxyObservation,
) -> &testlab_schema::EnvironmentOperationId {
    match observation {
        NetworkProxyObservation::FaultWindow(value) => &value.remove_operation_id,
        NetworkProxyObservation::ConnectionsCut(value) => &value.operation_id,
    }
}
