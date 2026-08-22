//! One sequential adapter session executes protocol-v9 scenario actions.

use std::collections::BTreeSet;
use std::path::Path;

use testlab_broker::RunningBroker;
use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterEvent, AdapterEventEnvelope, AdapterSecurity,
    PROTOCOL_VERSION, RunId, Scenario, ScenarioAction, SubjectManifest,
};

use crate::process::AdapterProcess;
use crate::protocol_session::ProtocolSession;
use crate::recorder::HistoryRecorder;
use crate::run_error::RunFailure;
use crate::runner_protocol::ExpectedEvent;
use crate::time::Deadline;

#[derive(Clone, Debug)]
pub(crate) struct SessionRequest<'a> {
    pub(crate) repository_root: &'a Path,
    pub(crate) scenario: &'a Scenario,
    pub(crate) subject: &'a SubjectManifest,
    pub(crate) run_id: &'a RunId,
    pub(crate) deadline: Deadline,
    pub(crate) broker_endpoints: &'a [String],
    pub(crate) security: AdapterSecurity,
    pub(crate) adapter_environment: &'a [(String, String)],
    pub(crate) model_broker: Option<&'a RunningBroker>,
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
        broker_endpoints,
        security,
        adapter_environment,
        model_broker,
    } = request;
    let mut process = AdapterProcess::spawn(repository_root, subject, adapter_environment)?;
    let mut protocol = ProtocolSession::default();
    let ready = protocol.send_and_wait(
        &mut process,
        recorder,
        deadline,
        AdapterCommand::Hello {
            run_id: run_id.clone(),
            scenario_id: scenario.id.clone(),
            broker_endpoints: broker_endpoints.to_vec(),
            security,
        },
        &ExpectedEvent::Ready,
    )?;
    let descriptor = descriptor_from(ready)?;
    verify_capabilities(scenario, &descriptor)?;
    *adapter = Some(descriptor);
    for step in &scenario.steps {
        let outcome = execute_step(
            &mut process,
            recorder,
            model_broker,
            deadline,
            &mut protocol,
            &step.action,
        )?;
        if outcome == StepOutcome::ClientFailed {
            return settle_process(&mut process, &protocol, recorder, deadline);
        }
    }
    let finish = protocol.send_and_wait(
        &mut process,
        recorder,
        deadline,
        AdapterCommand::Finish,
        &ExpectedEvent::Finished,
    )?;
    if matches!(finish.event, AdapterEvent::CommandFailed { .. }) {
        return settle_process(&mut process, &protocol, recorder, deadline);
    }
    settle_process(&mut process, &protocol, recorder, deadline)
}

fn settle_process(
    process: &mut AdapterProcess,
    protocol: &ProtocolSession,
    recorder: &mut HistoryRecorder,
    deadline: Deadline,
) -> Result<(), RunFailure> {
    process.close_input()?;
    protocol.drain_to_eof(process, recorder, deadline)?;
    let _stderr = process.wait_success(deadline)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StepOutcome {
    Continue,
    ClientFailed,
}

fn execute_step(
    process: &mut AdapterProcess,
    recorder: &mut HistoryRecorder,
    model_broker: Option<&RunningBroker>,
    deadline: Deadline,
    protocol: &mut ProtocolSession,
    action: &ScenarioAction,
) -> Result<StepOutcome, RunFailure> {
    if let ScenarioAction::SetBrokerBehavior { behavior } = action {
        let Some(broker) = model_broker else {
            return Err(RunFailure::harness(
                "environment_control_unsupported",
                "scenario requested model-broker control from a real Kafka environment",
            ));
        };
        broker.set_next_behavior(*behavior).map_err(|error| {
            RunFailure::harness(
                "environment_control_failed",
                format!("failed to control model broker: {error}"),
            )
        })?;
        recorder.broker_control(*behavior)?;
        return Ok(StepOutcome::Continue);
    }
    let (command, expected) = crate::session_command::translate(action).ok_or_else(|| {
        RunFailure::harness(
            "action_translation_failed",
            "non-environment scenario action had no adapter translation",
        )
    })?;
    let event = protocol.send_and_wait(process, recorder, deadline, command, &expected)?;
    if matches!(event.event, AdapterEvent::CommandFailed { .. }) {
        Ok(StepOutcome::ClientFailed)
    } else {
        Ok(StepOutcome::Continue)
    }
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
