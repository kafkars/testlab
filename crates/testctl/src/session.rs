//! One sequential adapter session executes protocol-v1 scenario actions.

use std::collections::BTreeSet;
use std::path::Path;

use testlab_broker::RunningBroker;
use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterEvent, AdapterEventEnvelope, PROTOCOL_VERSION, RunId,
    Scenario, ScenarioAction, SubjectManifest,
};

use crate::process::AdapterProcess;
use crate::protocol_session::ProtocolSession;
use crate::recorder::HistoryRecorder;
use crate::run_error::RunFailure;
use crate::runner_protocol::ExpectedEvent;
use crate::time::Deadline;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SessionRequest<'a> {
    pub(crate) repository_root: &'a Path,
    pub(crate) scenario: &'a Scenario,
    pub(crate) subject: &'a SubjectManifest,
    pub(crate) run_id: &'a RunId,
    pub(crate) deadline: Deadline,
    pub(crate) broker: &'a RunningBroker,
}

pub(crate) fn run_adapter_session(
    request: SessionRequest<'_>,
    recorder: &mut HistoryRecorder,
    adapter: &mut Option<AdapterDescriptor>,
) -> Result<(), RunFailure> {
    let SessionRequest {
        repository_root,
        scenario,
        subject,
        run_id,
        deadline,
        broker,
    } = request;
    let mut process = AdapterProcess::spawn(repository_root, subject)?;
    let mut protocol = ProtocolSession::default();
    let ready = protocol.send_and_wait(
        &mut process,
        recorder,
        deadline,
        AdapterCommand::Hello {
            run_id: run_id.clone(),
            scenario_id: scenario.id.clone(),
            broker_endpoint: broker.endpoint().to_owned(),
        },
        &ExpectedEvent::Ready,
    )?;
    let descriptor = descriptor_from(ready)?;
    verify_capabilities(scenario, &descriptor)?;
    *adapter = Some(descriptor);
    for step in &scenario.steps {
        execute_step(
            &mut process,
            recorder,
            broker,
            deadline,
            &mut protocol,
            &step.action,
        )?;
    }
    protocol.send_and_wait(
        &mut process,
        recorder,
        deadline,
        AdapterCommand::Finish,
        &ExpectedEvent::Finished,
    )?;
    process.close_input()?;
    protocol.drain_to_eof(&process, recorder, deadline)?;
    let _stderr = process.wait_success(deadline)?;
    Ok(())
}

fn execute_step(
    process: &mut AdapterProcess,
    recorder: &mut HistoryRecorder,
    broker: &RunningBroker,
    deadline: Deadline,
    protocol: &mut ProtocolSession,
    action: &ScenarioAction,
) -> Result<(), RunFailure> {
    let (command, expected) = match action {
        ScenarioAction::CreateClient { client_id } => (
            AdapterCommand::CreateClient {
                client_id: client_id.clone(),
            },
            ExpectedEvent::ClientCreated(client_id.clone()),
        ),
        ScenarioAction::CreateProducer {
            client_id,
            producer_id,
        } => (
            AdapterCommand::CreateProducer {
                client_id: client_id.clone(),
                producer_id: producer_id.clone(),
            },
            ExpectedEvent::ProducerCreated(producer_id.clone()),
        ),
        ScenarioAction::SetBrokerBehavior { behavior } => {
            broker.set_next_behavior(*behavior).map_err(|error| {
                RunFailure::harness(
                    "environment_control_failed",
                    format!("failed to control model broker: {error}"),
                )
            })?;
            recorder.broker_control(*behavior)?;
            return Ok(());
        }
        ScenarioAction::Send {
            producer_id,
            operation_id,
            record,
        } => (
            AdapterCommand::Send {
                producer_id: producer_id.clone(),
                operation_id: operation_id.clone(),
                record: record.clone(),
            },
            ExpectedEvent::SendSettled(operation_id.clone()),
        ),
        ScenarioAction::Flush { producer_id } => (
            AdapterCommand::Flush {
                producer_id: producer_id.clone(),
            },
            ExpectedEvent::FlushCompleted(producer_id.clone()),
        ),
        ScenarioAction::CloseProducer { producer_id } => (
            AdapterCommand::CloseProducer {
                producer_id: producer_id.clone(),
            },
            ExpectedEvent::ProducerClosed(producer_id.clone()),
        ),
        ScenarioAction::ShutdownClient { client_id } => (
            AdapterCommand::ShutdownClient {
                client_id: client_id.clone(),
            },
            ExpectedEvent::ClientShutdown(client_id.clone()),
        ),
    };
    protocol.send_and_wait(process, recorder, deadline, command, &expected)?;
    Ok(())
}

fn descriptor_from(event: AdapterEventEnvelope) -> Result<AdapterDescriptor, RunFailure> {
    match event.event {
        AdapterEvent::Ready { descriptor } => {
            if descriptor.protocol_version != PROTOCOL_VERSION {
                return Err(RunFailure::protocol(
                    "descriptor_version_mismatch",
                    format!(
                        "adapter descriptor used protocol {}, expected {PROTOCOL_VERSION}",
                        descriptor.protocol_version
                    ),
                ));
            }
            Ok(descriptor)
        }
        other => Err(RunFailure::protocol(
            "handshake_event_invalid",
            format!("expected ready event, received {other:?}"),
        )),
    }
}

fn verify_capabilities(scenario: &Scenario, adapter: &AdapterDescriptor) -> Result<(), RunFailure> {
    let missing = scenario
        .requires
        .difference(&adapter.capabilities)
        .copied()
        .collect::<BTreeSet<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(RunFailure::capability(format!(
            "adapter {} lacks required capabilities {missing:?}",
            adapter.id
        )))
    }
}
