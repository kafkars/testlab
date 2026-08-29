//! Public client failures are semantic evidence, not harness invalidity.

use testlab_schema::{Scenario, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_client_failures(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    for failure in &index.command_failures {
        if is_expected_failure(scenario, index, failure) {
            continue;
        }
        violations.push(violation(
            "CLIENT-001",
            format!(
                "public client command failed with code {}: {}",
                failure.code, failure.diagnostic
            ),
            None,
            vec![format!("history:{}", failure.history_sequence)],
        ));
    }
}

fn is_expected_failure(
    scenario: &Scenario,
    index: &HistoryIndex,
    failure: &crate::index::IndexedCommandFailure,
) -> bool {
    scenario.steps.iter().any(|step| {
        let Some(expected_code) = testlab_schema::expected_client_error(&step.action) else {
            return false;
        };
        if let Some((_, _)) = testlab_schema::expected_admin_error(&step.action) {
            return expected_code == failure.code
                && index
                    .admin_command_failures(&step.action)
                    .iter()
                    .any(|candidate| candidate.history_sequence == failure.history_sequence);
        }
        expected_code == failure.code
            && index.commands.iter().any(|(_, command_id, command)| {
                command_id == &failure.command_id && command_matches(&step.action, command)
            })
    })
}

fn command_matches(
    action: &testlab_schema::ScenarioAction,
    command: &testlab_schema::AdapterCommand,
) -> bool {
    match (action, command) {
        (
            testlab_schema::ScenarioAction::GroupReceive {
                consumer_id,
                receive_id,
                timeout_ms,
                ..
            },
            testlab_schema::AdapterCommand::GroupReceive {
                consumer_id: actual_consumer,
                receive_id: actual_receive,
                timeout_ms: actual_timeout,
            },
        ) => {
            consumer_id == actual_consumer
                && receive_id == actual_receive
                && timeout_ms == actual_timeout
        }
        (
            testlab_schema::ScenarioAction::CreateTransactionalProducer {
                client_id,
                producer_id,
                transactional_id,
                transaction_timeout_ms,
                initialization_timeout_ms,
                ..
            },
            testlab_schema::AdapterCommand::CreateTransactionalProducer {
                client_id: actual_client,
                producer_id: actual_producer,
                transactional_id: actual_transactional,
                transaction_timeout_ms: actual_transaction_timeout,
                initialization_timeout_ms: actual_initialization_timeout,
            },
        ) => {
            client_id == actual_client
                && producer_id == actual_producer
                && transactional_id == actual_transactional
                && transaction_timeout_ms == actual_transaction_timeout
                && initialization_timeout_ms == actual_initialization_timeout
        }
        _ => false,
    }
}
