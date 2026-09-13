//! Transaction execution preserves every exact public command in scenario order.

#[cfg(test)]
#[path = "transaction_command_test.rs"]
mod tests;

use testlab_schema::{
    AdapterCommand, Scenario, ScenarioAction, TransactionalTransformCommand, Violation,
};

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
        .filter(|(_, _, command)| is_transaction(command))
        .collect::<Vec<_>>();
    let exact = actual.len() == expected.len()
        && actual
            .iter()
            .zip(&expected)
            .all(|((_, _, actual), expected)| *actual == expected);
    if exact {
        return;
    }
    violations.push(violation(
        "TXN-012",
        format!(
            "transaction execution expected {} exact ordered execute, transform, and fence command(s)",
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
    match action {
        ScenarioAction::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            method,
            disposition,
            topic_identity_operation_id,
            timeout_ms,
        } => Some(AdapterCommand::ExecuteTransaction {
            producer_id: producer_id.clone(),
            transaction_id: transaction_id.clone(),
            operations: operations.clone(),
            method: *method,
            disposition: *disposition,
            validate_topic_uuids: topic_identity_operation_id.is_some(),
            timeout_ms: *timeout_ms,
        }),
        ScenarioAction::ExecuteTransactionalTransform(action) => Some(
            AdapterCommand::ExecuteTransactionalTransform(TransactionalTransformCommand {
                producer_id: action.producer_id.clone(),
                consumer_id: action.consumer_id.clone(),
                transaction_id: action.transaction_id.clone(),
                operations: action.operations.clone(),
                disposition: action.disposition,
                timeout_ms: action.timeout_ms,
            }),
        ),
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
        } => Some(AdapterCommand::FenceTransaction {
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
        }),
        _ => None,
    }
}

fn is_transaction(command: &AdapterCommand) -> bool {
    matches!(
        command,
        AdapterCommand::ExecuteTransaction { .. }
            | AdapterCommand::ExecuteTransactionalTransform(_)
            | AdapterCommand::FenceTransaction { .. }
    )
}
