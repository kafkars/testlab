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
        ScenarioAction::StopPartitionLeader {
            topic,
            partition,
            timeout_ms,
        } => control_compose(
            environment,
            recorder,
            deadline,
            *timeout_ms,
            |controller, timeout| controller.stop_partition_leader(topic, *partition, timeout),
        ),
        ScenarioAction::RestorePartitionLeader {
            topic,
            partition,
            timeout_ms,
        } => control_compose(
            environment,
            recorder,
            deadline,
            *timeout_ms,
            |controller, timeout| controller.restore_partition_leader(topic, *partition, timeout),
        ),
        _ => return None,
    };
    Some(result)
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
