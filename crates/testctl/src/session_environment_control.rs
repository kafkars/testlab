//! Environment actions stay outside the packaged client protocol boundary.

use std::time::Duration;

use testlab_schema::ScenarioAction;

use crate::recorder::HistoryRecorder;
use crate::run_error::RunFailure;
use crate::session::{SessionEnvironment, StepOutcome};
use crate::time::Deadline;

pub(crate) fn execute(
    environment: &mut SessionEnvironment<'_>,
    recorder: &mut HistoryRecorder,
    deadline: Deadline,
    action: &ScenarioAction,
) -> Option<Result<StepOutcome, RunFailure>> {
    let result = match action {
        ScenarioAction::SetBrokerBehavior { behavior } => {
            control_model(environment, recorder, *behavior)
        }
        ScenarioAction::ArmProtocolFault(control) => {
            control_adversary(environment, recorder, deadline, control)
        }
        ScenarioAction::AlterNetworkFault(action) => control_network_proxy(
            environment,
            recorder,
            deadline,
            testlab_schema::NetworkProxyControl::AlterFault(action.clone()),
            action.timeout_ms,
        ),
        ScenarioAction::CutNetworkConnections(action) => control_network_proxy(
            environment,
            recorder,
            deadline,
            testlab_schema::NetworkProxyControl::CutConnections(action.clone()),
            action.timeout_ms,
        ),
        ScenarioAction::RestartBroker {
            broker_ordinal,
            timeout_ms,
        } => control_compose(
            environment,
            recorder,
            deadline,
            *timeout_ms,
            |controller, timeout| controller.restart_broker(*broker_ordinal, timeout),
        ),
        ScenarioAction::StopBroker {
            broker_ordinal,
            timeout_ms,
        } => control_compose(
            environment,
            recorder,
            deadline,
            *timeout_ms,
            |controller, timeout| controller.stop_broker(*broker_ordinal, timeout),
        ),
        ScenarioAction::StartBroker {
            broker_ordinal,
            timeout_ms,
        } => control_compose(
            environment,
            recorder,
            deadline,
            *timeout_ms,
            |controller, timeout| controller.start_broker(*broker_ordinal, timeout),
        ),
        ScenarioAction::StopBrokerRole { target, timeout_ms } => control_compose(
            environment,
            recorder,
            deadline,
            *timeout_ms,
            |controller, timeout| controller.stop_broker_role(target, timeout),
        ),
        ScenarioAction::RestoreBrokerRole { target, timeout_ms } => control_compose(
            environment,
            recorder,
            deadline,
            *timeout_ms,
            |controller, timeout| controller.restore_broker_role(target, timeout),
        ),
        ScenarioAction::AlterBrokerPolicy(action) => control_compose(
            environment,
            recorder,
            deadline,
            action.timeout_ms,
            |controller, timeout| controller.alter_broker_policy(action, timeout),
        ),
        _ => return None,
    };
    Some(result)
}

fn control_network_proxy(
    environment: &mut SessionEnvironment<'_>,
    recorder: &mut HistoryRecorder,
    deadline: Deadline,
    control: testlab_schema::NetworkProxyControl,
    timeout_ms: u64,
) -> Result<StepOutcome, RunFailure> {
    let SessionEnvironment::Compose { controller, .. } = environment else {
        return Err(RunFailure::harness(
            "environment_control_unsupported",
            "scenario requested a network fault from a non-Compose environment",
        ));
    };
    let timeout = Duration::from_millis(timeout_ms).min(deadline.remaining()?);
    let observations = controller
        .control_network_proxy(&control, timeout)
        .map_err(|error| RunFailure::harness(error.code(), error.diagnostic()))?;
    recorder.network_proxy_control(control)?;
    for observation in observations {
        recorder.network_proxy_observation(observation)?;
    }
    Ok(StepOutcome::Continue)
}

fn control_adversary(
    environment: &mut SessionEnvironment<'_>,
    recorder: &mut HistoryRecorder,
    deadline: Deadline,
    control: &testlab_schema::ProtocolFaultAction,
) -> Result<StepOutcome, RunFailure> {
    let SessionEnvironment::Adversary(controller) = environment else {
        return Err(RunFailure::harness(
            "environment_control_unsupported",
            "scenario requested a protocol fault from a non-adversary environment",
        ));
    };
    controller
        .arm(control, deadline.remaining()?)
        .map_err(|error| RunFailure::harness("environment_control_failed", error.to_string()))?;
    recorder.adversary_control(control.clone())?;
    for observation in controller.take_observations() {
        recorder.adversary_observation(observation)?;
    }
    Ok(StepOutcome::Continue)
}

fn control_model(
    environment: &SessionEnvironment<'_>,
    recorder: &mut HistoryRecorder,
    behavior: testlab_schema::BrokerBehavior,
) -> Result<StepOutcome, RunFailure> {
    let SessionEnvironment::Model(broker) = environment else {
        return Err(RunFailure::harness(
            "environment_control_unsupported",
            "scenario requested model control from a real Kafka environment",
        ));
    };
    broker.set_next_behavior(behavior).map_err(|error| {
        RunFailure::harness(
            "environment_control_failed",
            format!("failed to control model broker: {error}"),
        )
    })?;
    recorder.broker_control(behavior)?;
    Ok(StepOutcome::Continue)
}

fn control_compose(
    environment: &mut SessionEnvironment<'_>,
    recorder: &mut HistoryRecorder,
    deadline: Deadline,
    timeout_ms: u64,
    operation: impl FnOnce(
        &mut testlab_environment::DockerComposeEnvironment,
        Duration,
    ) -> testlab_environment::ComposePhase,
) -> Result<StepOutcome, RunFailure> {
    let SessionEnvironment::Compose {
        controller,
        artifacts,
    } = environment
    else {
        return Err(RunFailure::harness(
            "environment_control_unsupported",
            "scenario requested Compose control from the model environment",
        ));
    };
    let timeout = Duration::from_millis(timeout_ms).min(deadline.remaining()?);
    let phase = operation(controller, timeout);
    crate::runner_environment::record_phase(phase, recorder, artifacts)?;
    Ok(StepOutcome::Continue)
}
