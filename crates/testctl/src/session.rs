//! One sequential adapter session executes current-protocol scenario actions.

use std::collections::BTreeSet;
use std::path::Path;

use testlab_broker::RunningBroker;
use testlab_environment::{ComposeArtifact, DockerComposeEnvironment, RunningAdversary};
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

pub(crate) struct SessionRequest<'a> {
    pub(crate) repository_root: &'a Path,
    pub(crate) scenario: &'a Scenario,
    pub(crate) subject: &'a SubjectManifest,
    pub(crate) run_id: &'a RunId,
    pub(crate) deadline: Deadline,
    pub(crate) broker_endpoints: &'a [String],
    pub(crate) security: AdapterSecurity,
    pub(crate) adapter_environment: &'a [(String, String)],
    pub(crate) environment: SessionEnvironment<'a>,
}

pub(crate) enum SessionEnvironment<'a> {
    Model(&'a RunningBroker),
    Adversary(&'a mut RunningAdversary),
    Compose {
        controller: &'a mut DockerComposeEnvironment,
        artifacts: &'a mut Vec<ComposeArtifact>,
    },
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
        mut environment,
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
            &mut environment,
            deadline,
            &mut protocol,
            scenario,
            &step.action,
        )?;
        match outcome {
            StepOutcome::Continue => {}
            StepOutcome::ClientFailed => {
                return settle_process(&mut process, &protocol, recorder, deadline);
            }
            StepOutcome::ScenarioFailed => {
                return abort_and_settle(&mut process, &mut protocol, recorder, deadline);
            }
        }
    }
    finish_and_settle(&mut process, &mut protocol, recorder, deadline)
}

fn abort_and_settle(
    process: &mut AdapterProcess,
    protocol: &mut ProtocolSession,
    recorder: &mut HistoryRecorder,
    deadline: Deadline,
) -> Result<(), RunFailure> {
    let (command, expected) = scenario_failure_settlement();
    protocol.send_and_wait(process, recorder, deadline, command, &expected)?;
    settle_process(process, protocol, recorder, deadline)
}

pub(super) fn scenario_failure_settlement() -> (AdapterCommand, ExpectedEvent) {
    (AdapterCommand::Abort, ExpectedEvent::Aborted)
}

fn finish_and_settle(
    process: &mut AdapterProcess,
    protocol: &mut ProtocolSession,
    recorder: &mut HistoryRecorder,
    deadline: Deadline,
) -> Result<(), RunFailure> {
    let finish = protocol.send_and_wait(
        process,
        recorder,
        deadline,
        AdapterCommand::Finish,
        &ExpectedEvent::Finished,
    )?;
    if matches!(finish.event, AdapterEvent::CommandFailed { .. }) {
        return settle_process(process, protocol, recorder, deadline);
    }
    settle_process(process, protocol, recorder, deadline)
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
pub(crate) enum StepOutcome {
    Continue,
    ClientFailed,
    ScenarioFailed,
}

fn execute_step(
    process: &mut AdapterProcess,
    recorder: &mut HistoryRecorder,
    environment: &mut SessionEnvironment<'_>,
    deadline: Deadline,
    protocol: &mut ProtocolSession,
    scenario: &Scenario,
    action: &ScenarioAction,
) -> Result<StepOutcome, RunFailure> {
    if let Some(result) =
        crate::session_environment_control::execute(environment, recorder, deadline, action)
    {
        return result;
    }
    let translated = crate::session_command_concurrent::translate(action, scenario)
        .or_else(|| crate::session_command::translate(action));
    let (command, expected) = translated.ok_or_else(|| {
        RunFailure::harness(
            "action_translation_failed",
            "non-environment scenario action had no adapter translation",
        )
    })?;
    let observation_command = command.clone();
    let event = protocol.send_and_wait(process, recorder, deadline, command, &expected)?;
    if matches!(event.event, AdapterEvent::CommandFailed { .. }) {
        if testlab_schema::expected_client_error(action).is_some() {
            if expects_admin_failure(action) {
                observe_admin_action(
                    environment,
                    recorder,
                    deadline,
                    action,
                    &observation_command,
                )?;
            }
            return Ok(StepOutcome::Continue);
        }
        Ok(StepOutcome::ClientFailed)
    } else if crate::session_share::receive_succeeded(scenario, action, &event.event) == Some(false)
    {
        Ok(StepOutcome::ScenarioFailed)
    } else {
        observe_admin_action(
            environment,
            recorder,
            deadline,
            action,
            &observation_command,
        )?;
        Ok(StepOutcome::Continue)
    }
}

pub(super) fn expects_admin_failure(action: &ScenarioAction) -> bool {
    testlab_schema::expected_admin_error(action).is_some()
}

fn observe_admin_action(
    environment: &mut SessionEnvironment<'_>,
    recorder: &mut HistoryRecorder,
    deadline: Deadline,
    action: &ScenarioAction,
    command: &AdapterCommand,
) -> Result<(), RunFailure> {
    let SessionEnvironment::Compose {
        controller,
        artifacts,
    } = environment
    else {
        return Ok(());
    };
    let snapshot = controller.observe_admin(action, command, deadline.remaining()?);
    let testlab_environment::ComposeObservation {
        phase,
        observations,
        state_observations,
    } = snapshot;
    crate::runner_environment::record_phase(phase, recorder, artifacts)?;
    if !observations.is_empty() {
        return Err(RunFailure::harness(
            "admin_observation_shape_invalid",
            "admin state observation unexpectedly returned record observations",
        ));
    }
    for observation in state_observations {
        recorder.state_observation(observation)?;
    }
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
