//! Group receives retain independently selected public observation and checkpoint methods.

#[cfg(test)]
#[path = "group_checkpoint_method_test.rs"]
mod checkpoint_method_tests;

use testlab_schema::{
    AdapterCommand, GroupCheckpointMethod, GroupConsumerReceiveMethod, OperationId, Scenario,
    ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    verify_immediate(scenario, index, violations);
    verify_into_checkpoint(scenario, index, violations);
}

fn verify_immediate(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::GroupReceive {
            method: GroupConsumerReceiveMethod::TryTakeBatch,
            receive_id,
            ..
        } = &step.action
        else {
            continue;
        };
        verify_exact(
            &step.action,
            receive_id,
            "CONS-018",
            "try_take_batch observer",
            index,
            violations,
        );
    }
}

fn verify_into_checkpoint(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        let ScenarioAction::GroupReceive {
            checkpoint_method: GroupCheckpointMethod::IntoCheckpoint,
            receive_id,
            ..
        } = &step.action
        else {
            continue;
        };
        verify_exact(
            &step.action,
            receive_id,
            "CONS-025",
            "into_checkpoint conversion",
            index,
            violations,
        );
    }
}

fn verify_exact(
    action: &ScenarioAction,
    receive_id: &OperationId,
    contract: &str,
    selection: &str,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let expected = command(action);
    let commands = index
        .commands
        .iter()
        .filter(|(_, _, command)| {
            matches!(command, AdapterCommand::GroupReceive { receive_id: actual, .. } if actual == receive_id)
        })
        .collect::<Vec<_>>();
    if commands.len() == 1 && commands[0].2 == expected {
        return;
    }
    violations.push(violation(
        contract,
        format!(
            "group receive {receive_id} selected {selection} but observed {} matching command(s) without one exact request",
            commands.len()
        ),
        Some(receive_id.clone()),
        commands
            .iter()
            .map(|(sequence, ..)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn command(action: &ScenarioAction) -> AdapterCommand {
    let ScenarioAction::GroupReceive {
        consumer_id,
        method,
        checkpoint_method,
        receive_id,
        processing_acknowledgement_delay_ms,
        processed_record_count,
        timeout_ms,
        ..
    } = action
    else {
        unreachable!("selected action is one group receive")
    };
    AdapterCommand::GroupReceive {
        consumer_id: consumer_id.clone(),
        method: *method,
        checkpoint_method: *checkpoint_method,
        receive_id: receive_id.clone(),
        processing_acknowledgement_delay_ms: *processing_acknowledgement_delay_ms,
        processed_record_count: *processed_record_count,
        timeout_ms: *timeout_ms,
    }
}
