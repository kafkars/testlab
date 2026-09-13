//! Transactional producer initialization requires exact caller-selected construction input.

use std::collections::BTreeMap;

use testlab_schema::{AdapterCommand, ProducerId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    let mut expected = BTreeMap::<ProducerId, Vec<AdapterCommand>>::new();
    for step in &scenario.steps {
        if let Some((producer_id, command)) = command(&step.action) {
            expected.entry(producer_id).or_default().push(command);
        }
    }
    for (producer_id, commands) in expected {
        let actual = index
            .commands
            .iter()
            .filter(|(_, _, command)| creation_producer_id(command) == Some(&producer_id))
            .collect::<Vec<_>>();
        let exact = actual.len() == commands.len()
            && actual
                .iter()
                .zip(&commands)
                .all(|((_, _, actual), expected)| *actual == *expected);
        if exact {
            continue;
        }
        violations.push(violation(
            "TXN-011",
            format!(
                "transactional producer {producer_id} expected {} exact ordered client, identity, transaction-timeout, and initialization-deadline command(s)",
                commands.len()
            ),
            None,
            actual
                .into_iter()
                .map(|(sequence, _, _)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}

fn command(action: &ScenarioAction) -> Option<(ProducerId, AdapterCommand)> {
    let ScenarioAction::CreateTransactionalProducer {
        client_id,
        producer_id,
        transactional_id,
        transaction_timeout_ms,
        initialization_timeout_ms,
        ..
    } = action
    else {
        return None;
    };
    Some((
        producer_id.clone(),
        AdapterCommand::CreateTransactionalProducer {
            client_id: client_id.clone(),
            producer_id: producer_id.clone(),
            transactional_id: transactional_id.clone(),
            transaction_timeout_ms: *transaction_timeout_ms,
            initialization_timeout_ms: *initialization_timeout_ms,
        },
    ))
}

fn creation_producer_id(command: &AdapterCommand) -> Option<&ProducerId> {
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

#[cfg(test)]
#[path = "transactional_producer_registration_test.rs"]
mod tests;
