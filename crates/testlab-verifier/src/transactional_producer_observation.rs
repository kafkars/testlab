//! Transactional producer creation proves public identity and owner activity.

use testlab_schema::{
    AdapterCommand, AdapterEvent, ProducerId, Scenario, ScenarioAction, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

struct ExpectedCreation {
    command: AdapterCommand,
    producer_id: ProducerId,
    transactional_id: String,
    expects_success: bool,
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
        if expected.expects_success {
            verify_creation(&expected, *command_sequence, command_id, index, violations);
        }
    }
}

fn verify_creation(
    expected: &ExpectedCreation,
    command_sequence: u64,
    command_id: &testlab_schema::CommandId,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let events = index
        .adapter_events
        .iter()
        .filter(|(_, envelope)| &envelope.command_id == command_id)
        .filter_map(|(sequence, envelope)| match &envelope.event {
            AdapterEvent::TransactionalProducerCreated(observation) => {
                Some((*sequence, observation))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let exact = events.len() == 1
        && events.first().is_some_and(|(event_sequence, observation)| {
            *event_sequence > command_sequence
                && observation.producer_id == expected.producer_id
                && observation.transactional_id == expected.transactional_id
                && observation.kafka_producer_id >= 0
                && observation.kafka_producer_epoch >= 0
                && observation.active
        });
    if !exact {
        fail(
            "TXN-013",
            format!(
                "transactional producer {} expected one later exact active public identity observation",
                expected.producer_id
            ),
            command_sequence,
            events.iter().map(|(sequence, _)| *sequence),
            violations,
        );
    }
}

fn fail(
    contract_id: &str,
    message: String,
    command_sequence: u64,
    event_sequences: impl Iterator<Item = u64>,
    violations: &mut Vec<Violation>,
) {
    violations.push(violation(
        contract_id,
        message,
        None,
        std::iter::once(command_sequence)
            .chain(event_sequences)
            .map(|sequence| format!("history:{sequence}"))
            .collect(),
    ));
}

fn creations(scenario: &Scenario) -> Vec<ExpectedCreation> {
    scenario
        .steps
        .iter()
        .filter_map(|step| expected_creation(&step.action))
        .collect()
}

fn expected_creation(action: &ScenarioAction) -> Option<ExpectedCreation> {
    match action {
        ScenarioAction::CreateTransactionalProducer {
            client_id,
            producer_id,
            transactional_id,
            transaction_timeout_ms,
            initialization_timeout_ms,
            expected_error_code,
        } => Some(ExpectedCreation {
            command: AdapterCommand::CreateTransactionalProducer {
                client_id: client_id.clone(),
                producer_id: producer_id.clone(),
                transactional_id: transactional_id.clone(),
                transaction_timeout_ms: *transaction_timeout_ms,
                initialization_timeout_ms: *initialization_timeout_ms,
            },
            producer_id: producer_id.clone(),
            transactional_id: transactional_id.clone(),
            expects_success: expected_error_code.is_none(),
        }),
        ScenarioAction::FenceTransaction {
            fence_method,
            producer_id,
            transaction_id,
            operation,
            replacement_client_id,
            replacement_producer_id,
            transactional_id,
            transaction_timeout_ms,
            initialization_timeout_ms,
            timeout_ms,
        } => Some(ExpectedCreation {
            command: AdapterCommand::FenceTransaction {
                fence_method: *fence_method,
                producer_id: producer_id.clone(),
                transaction_id: transaction_id.clone(),
                operation: operation.clone(),
                replacement_client_id: replacement_client_id.clone(),
                replacement_producer_id: replacement_producer_id.clone(),
                transactional_id: transactional_id.clone(),
                transaction_timeout_ms: *transaction_timeout_ms,
                initialization_timeout_ms: *initialization_timeout_ms,
                timeout_ms: *timeout_ms,
            },
            producer_id: replacement_producer_id.clone(),
            transactional_id: transactional_id.clone(),
            expects_success: true,
        }),
        _ => None,
    }
}

#[cfg(test)]
#[path = "transactional_producer_observation_test.rs"]
mod tests;
