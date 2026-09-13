//! Hosted group receives preserve every exact public batch request.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let expected = scenario
        .steps
        .iter()
        .filter_map(|step| command(&step.action))
        .collect::<Vec<_>>();
    if expected.is_empty() {
        return;
    }
    let actual = index
        .commands
        .iter()
        .filter(|(_, _, command)| matches!(command, AdapterCommand::GroupReceive { .. }))
        .collect::<Vec<_>>();
    let exact = actual.len() == expected.len()
        && actual
            .iter()
            .zip(&expected)
            .all(|((_, _, actual), expected)| *actual == *expected);
    if exact {
        return;
    }
    violations.push(violation(
        "CONS-032",
        format!(
            "group receive expected {} exact ordered consumer, observer, checkpoint, processing, identity, and timeout command(s)",
            expected.len()
        ),
        None,
        actual
            .into_iter()
            .map(|(sequence, _, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn command(action: &ScenarioAction) -> Option<AdapterCommand> {
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
        return None;
    };
    Some(AdapterCommand::GroupReceive {
        consumer_id: consumer_id.clone(),
        method: *method,
        checkpoint_method: *checkpoint_method,
        receive_id: receive_id.clone(),
        processing_acknowledgement_delay_ms: *processing_acknowledgement_delay_ms,
        processed_record_count: *processed_record_count,
        timeout_ms: *timeout_ms,
    })
}

#[cfg(test)]
#[path = "group_receive_command_test.rs"]
mod tests;
