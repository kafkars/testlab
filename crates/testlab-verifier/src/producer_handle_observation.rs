//! Successful ordinary producer construction proves the selected public handle policy.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, AdapterEvent, ClientId, CommandId, ProducerId, Scenario, ScenarioAction,
    Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

struct ExpectedCreation {
    command: AdapterCommand,
    producer_id: ProducerId,
    selected_delivery_timeout_ms: Option<u64>,
}

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let mut command_cursor = 0;
    for expected in creations(scenario) {
        let Some(relative) = index.commands[command_cursor..]
            .iter()
            .position(|(_, _, command)| command == &expected.command)
        else {
            continue;
        };
        command_cursor += relative;
        let (command_sequence, command_id, _) = &index.commands[command_cursor];
        command_cursor += 1;
        verify_creation(&expected, *command_sequence, command_id, index, violations);
    }
}

fn verify_creation(
    expected: &ExpectedCreation,
    command_sequence: u64,
    command_id: &CommandId,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let events = index
        .adapter_events
        .iter()
        .filter(|(_, envelope)| &envelope.command_id == command_id)
        .filter_map(|(sequence, envelope)| match &envelope.event {
            AdapterEvent::ProducerCreated(observation) => Some((*sequence, observation)),
            _ => None,
        })
        .collect::<Vec<_>>();
    let exact = events.len() == 1
        && events.first().is_some_and(|(event_sequence, observation)| {
            *event_sequence > command_sequence
                && observation.producer_id == expected.producer_id
                && observation.selected_delivery_timeout_ms > 0
                && expected
                    .selected_delivery_timeout_ms
                    .is_none_or(|timeout| observation.selected_delivery_timeout_ms == timeout)
        });
    if exact {
        return;
    }
    let mut evidence = vec![format!("history:{command_sequence}")];
    evidence.extend(
        events
            .iter()
            .map(|(sequence, _)| format!("history:{sequence}")),
    );
    violations.push(violation(
        "PROD-023",
        format!(
            "producer {} expected one later exact nonzero selected delivery-timeout observation",
            expected.producer_id
        ),
        None,
        evidence,
    ));
}

fn creations(scenario: &Scenario) -> Vec<ExpectedCreation> {
    let mut policies = BTreeMap::<ClientId, Option<u64>>::new();
    let mut creations = Vec::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateClient(action) if action.expected_error_code.is_none() => {
                policies.insert(action.client_id.clone(), None);
            }
            ScenarioAction::CreateConfiguredClient(action) => {
                policies.insert(
                    action.client_id.clone(),
                    Some(action.configuration.delivery_timeout_ms),
                );
            }
            ScenarioAction::CreateAssignedConsumerClient(action) => {
                policies.insert(action.client_id.clone(), None);
            }
            ScenarioAction::CreateProducer {
                client_id,
                producer_id,
                ownership,
                delivery_timeout_ms,
            } => creations.push(ExpectedCreation {
                command: AdapterCommand::CreateProducer {
                    client_id: client_id.clone(),
                    producer_id: producer_id.clone(),
                    ownership: *ownership,
                    delivery_timeout_ms: *delivery_timeout_ms,
                },
                producer_id: producer_id.clone(),
                selected_delivery_timeout_ms: delivery_timeout_ms
                    .or_else(|| policies.get(client_id).copied().flatten()),
            }),
            ScenarioAction::ShutdownClient { client_id } => {
                policies.remove(client_id);
            }
            _ => {}
        }
    }
    creations
}

#[cfg(test)]
#[path = "producer_handle_observation_test.rs"]
mod tests;
