//! Protocol session state accepts late known events while rejecting foreign correlation.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandEnvelope, CommandId,
    PROTOCOL_VERSION,
};

use crate::process::AdapterProcess;
use crate::recorder::HistoryRecorder;
use crate::run_error::RunFailure;
use crate::runner_protocol::{EventDisposition, ExpectedEvent};
use crate::time::Deadline;

const MAX_EVENTS_PER_COMMAND: usize = 64;

#[derive(Debug, Default)]
pub(crate) struct ProtocolSession {
    next_command: u64,
    issued: BTreeMap<CommandId, ExpectedEvent>,
}

impl ProtocolSession {
    pub(crate) fn send_and_wait(
        &mut self,
        process: &mut AdapterProcess,
        recorder: &mut HistoryRecorder,
        deadline: Deadline,
        command: AdapterCommand,
        expected: &ExpectedEvent,
    ) -> Result<AdapterEventEnvelope, RunFailure> {
        let command_id = self.next_command_id()?;
        let envelope = CommandEnvelope::new(command_id.clone(), command);
        if self
            .issued
            .insert(command_id.clone(), expected.clone())
            .is_some()
        {
            return Err(RunFailure::harness(
                "command_id_reused",
                format!("generated command identity {command_id} was already issued"),
            ));
        }
        recorder.command(envelope.clone())?;
        process.send(&envelope)?;
        for _ in 0..MAX_EVENTS_PER_COMMAND {
            let event = process.receive(deadline)?.ok_or_else(|| {
                RunFailure::harness(
                    "subject_exited_early",
                    format!("adapter stdout closed while waiting for {expected:?}"),
                )
            })?;
            recorder.event(event.clone())?;
            let event_expected = self.expected_for(&event)?;
            reject_fatal(&event)?;
            let disposition = event_expected.classify(&event.event)?;
            if event.command_id != command_id {
                continue;
            }
            if disposition == EventDisposition::Complete {
                return Ok(event);
            }
        }
        Err(RunFailure::protocol(
            "event_limit_exceeded",
            format!(
                "adapter emitted more than {MAX_EVENTS_PER_COMMAND} events while command {command_id} was active"
            ),
        ))
    }

    pub(crate) fn drain_to_eof(
        &self,
        process: &AdapterProcess,
        recorder: &mut HistoryRecorder,
        deadline: Deadline,
    ) -> Result<(), RunFailure> {
        while let Some(event) = process.receive(deadline)? {
            recorder.event(event.clone())?;
            let event_expected = self.expected_for(&event)?;
            reject_fatal(&event)?;
            event_expected.classify(&event.event)?;
        }
        Ok(())
    }

    fn expected_for(&self, event: &AdapterEventEnvelope) -> Result<&ExpectedEvent, RunFailure> {
        if event.protocol_version != PROTOCOL_VERSION {
            return Err(RunFailure::protocol(
                "protocol_version_mismatch",
                format!(
                    "event used protocol {}, expected {PROTOCOL_VERSION}",
                    event.protocol_version
                ),
            ));
        }
        self.issued.get(&event.command_id).ok_or_else(|| {
            RunFailure::protocol(
                "command_id_unknown",
                format!(
                    "event references command {} that testctl never issued",
                    event.command_id
                ),
            )
        })
    }

    fn next_command_id(&mut self) -> Result<CommandId, RunFailure> {
        self.next_command = self.next_command.checked_add(1).ok_or_else(|| {
            RunFailure::harness(
                "command_id_overflow",
                "command identity counter exceeded u64",
            )
        })?;
        CommandId::new(format!("command-{:08}", self.next_command)).map_err(|error| {
            RunFailure::harness(
                "command_id_invalid",
                format!("generated command identity was invalid: {error}"),
            )
        })
    }
}

fn reject_fatal(event: &AdapterEventEnvelope) -> Result<(), RunFailure> {
    if let AdapterEvent::Fatal { code, diagnostic } = &event.event {
        Err(RunFailure::harness(
            "adapter_fatal",
            format!("adapter fatal {code}: {diagnostic}"),
        ))
    } else {
        Ok(())
    }
}
