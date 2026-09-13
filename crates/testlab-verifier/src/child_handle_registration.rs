//! Ordinary producer and assigned-consumer handles require exact creation commands.

use testlab_schema::{AdapterCommand, ConsumerId, ProducerId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateProducer { producer_id, .. } => {
                let Some(expected) = producer_command(&step.action) else {
                    unreachable!("producer action was matched above");
                };
                verify_one(
                    &expected,
                    index.commands.iter().filter(|(_, _, command)| {
                        producer_creation_id(command) == Some(producer_id)
                    }),
                    "PROD-020",
                    format!(
                        "producer {producer_id} expected one exact client, producer, and child-ownership creation command"
                    ),
                    same_producer_owner,
                    violations,
                );
            }
            ScenarioAction::CreateAssignedConsumer { consumer_id, .. } => {
                let Some(expected) = assigned_command(&step.action) else {
                    unreachable!("assigned-consumer action was matched above");
                };
                verify_one(
                    &expected,
                    index.commands.iter().filter(|(_, _, command)| {
                        consumer_creation_id(command) == Some(consumer_id)
                    }),
                    "CONS-029",
                    format!(
                        "assigned consumer {consumer_id} expected one exact client, consumer, and child-ownership creation command"
                    ),
                    same_command,
                    violations,
                );
            }
            _ => {}
        }
    }
}

fn verify_one<'a>(
    expected: &AdapterCommand,
    relevant: impl Iterator<Item = &'a (u64, testlab_schema::CommandId, AdapterCommand)>,
    contract: &str,
    message: String,
    matches: impl Fn(&AdapterCommand, &AdapterCommand) -> bool,
    violations: &mut Vec<Violation>,
) {
    let relevant = relevant.collect::<Vec<_>>();
    if relevant.len() == 1
        && relevant
            .iter()
            .all(|(_, _, actual)| matches(actual, expected))
    {
        return;
    }
    violations.push(violation(
        contract,
        message,
        None,
        relevant
            .into_iter()
            .map(|(sequence, _, _)| format!("history:{sequence}"))
            .collect(),
    ));
}

fn same_producer_owner(actual: &AdapterCommand, expected: &AdapterCommand) -> bool {
    matches!(
        (actual, expected),
        (
            AdapterCommand::CreateProducer {
                client_id: actual_client,
                producer_id: actual_producer,
                ownership: actual_ownership,
                ..
            },
            AdapterCommand::CreateProducer {
                client_id: expected_client,
                producer_id: expected_producer,
                ownership: expected_ownership,
                ..
            }
        ) if actual_client == expected_client
            && actual_producer == expected_producer
            && actual_ownership == expected_ownership
    )
}

fn same_command(actual: &AdapterCommand, expected: &AdapterCommand) -> bool {
    actual == expected
}

fn producer_command(action: &ScenarioAction) -> Option<AdapterCommand> {
    let ScenarioAction::CreateProducer {
        client_id,
        producer_id,
        ownership,
        delivery_timeout_ms,
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::CreateProducer {
        client_id: client_id.clone(),
        producer_id: producer_id.clone(),
        ownership: *ownership,
        delivery_timeout_ms: *delivery_timeout_ms,
    })
}

fn assigned_command(action: &ScenarioAction) -> Option<AdapterCommand> {
    let ScenarioAction::CreateAssignedConsumer {
        client_id,
        consumer_id,
        ownership,
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::CreateAssignedConsumer {
        client_id: client_id.clone(),
        consumer_id: consumer_id.clone(),
        ownership: *ownership,
    })
}

fn producer_creation_id(command: &AdapterCommand) -> Option<&ProducerId> {
    match command {
        AdapterCommand::CreateProducer { producer_id, .. }
        | AdapterCommand::CreateTransactionalProducer { producer_id, .. }
        | AdapterCommand::FenceTransaction {
            replacement_producer_id: producer_id,
            ..
        } => Some(producer_id),
        _ => None,
    }
}

fn consumer_creation_id(command: &AdapterCommand) -> Option<&ConsumerId> {
    match command {
        AdapterCommand::CreateAssignedConsumer { consumer_id, .. }
        | AdapterCommand::CreateGroupConsumer { consumer_id, .. }
        | AdapterCommand::CreateShareConsumer { consumer_id, .. } => Some(consumer_id),
        _ => None,
    }
}

#[cfg(test)]
#[path = "child_handle_registration_test.rs"]
mod tests;
