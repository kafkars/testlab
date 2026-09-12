//! Group assignment evidence retains the exact public transition observer.

#[cfg(test)]
#[path = "group_consumer_event_method_test.rs"]
mod tests;

use testlab_schema::{AdapterCommand, AdapterEvent, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::ObserveGroupAssignments(action) = &step.action else {
            continue;
        };
        let commands = index
            .commands
            .iter()
            .filter_map(|(sequence, _, command)| match command {
                AdapterCommand::ObserveGroupAssignments(command)
                    if command.operation_id == action.operation_id =>
                {
                    Some((*sequence, command))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let events = index
            .adapter_events
            .iter()
            .filter_map(|(sequence, envelope)| match &envelope.event {
                AdapterEvent::GroupAssignmentsObserved(observation)
                    if observation.operation_id == action.operation_id =>
                {
                    Some((*sequence, observation))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let command_exact = matches!(commands.as_slice(), [(sequence, command)]
            if command.consumer_ids == action.consumer_ids
                && command.method == action.method
                && command.timeout_ms == action.timeout_ms
                && events.first().is_some_and(|(event_sequence, _)| sequence < event_sequence));
        let event_exact = matches!(events.as_slice(), [(_, observation)]
            if observation.method == action.method);
        if command_exact && event_exact {
            continue;
        }
        violations.push(violation(
            "CONS-026",
            format!(
                "group assignment observation {} expected one ordered exact {:?} command and public event, observed {} command(s) and {} event(s)",
                action.operation_id,
                action.method,
                commands.len(),
                events.len()
            ),
            Some(action.operation_id.clone()),
            commands
                .iter()
                .map(|(sequence, _)| format!("history:{sequence}"))
                .chain(events.iter().map(|(sequence, _)| format!("history:{sequence}")))
                .collect(),
        ));
    }
}
