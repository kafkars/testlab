//! Transaction recovery binds exact post-control commands to complete dispositions.

#[cfg(test)]
#[path = "network_transaction_recovery_test.rs"]
mod tests;

use testlab_schema::{
    AdapterCommand, BatchRecord, EnvironmentOperationId, NetworkFaultState, OperationId, Scenario,
    ScenarioAction, TerminalStatus, TransactionDisposition, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.commands.is_empty() {
        return;
    }
    for (position, step) in scenario.steps.iter().enumerate() {
        let Some(control_id) = recovery_control_id(&step.action) else {
            continue;
        };
        let end = scenario.steps[position + 1..]
            .iter()
            .position(|step| network_control_id(&step.action).is_some())
            .map_or(scenario.steps.len(), |relative| position + 1 + relative);
        let after = control_sequence(index, control_id);
        let before = scenario
            .steps
            .get(end)
            .and_then(|step| network_control_id(&step.action))
            .and_then(|operation_id| control_sequence(index, operation_id))
            .unwrap_or(u64::MAX);
        for action in scenario.steps[position + 1..end]
            .iter()
            .map(|step| &step.action)
            .filter(|action| transaction_fields(action).is_some())
        {
            if exact_recovery(action, index, after, before) {
                continue;
            }
            let transaction_id = transaction_fields(action).map(|fields| fields.0.clone());
            violations.push(violation(
                "NET-006",
                format!(
                    "network control {control_id} expected exact post-control transaction {} to stage and complete before the next network control",
                    transaction_id
                        .as_ref()
                        .map_or("unknown", OperationId::as_str)
                ),
                transaction_id,
                recovery_references(action, index, after),
            ));
        }
    }
}

fn exact_recovery(
    action: &ScenarioAction,
    index: &HistoryIndex,
    after: Option<u64>,
    before: u64,
) -> bool {
    let Some(expected) = transaction_command(action) else {
        return false;
    };
    let commands = index
        .commands
        .iter()
        .filter_map(|(sequence, _, command)| (command == &expected).then_some(*sequence))
        .collect::<Vec<_>>();
    let [command] = commands.as_slice() else {
        return false;
    };
    let Some((transaction_id, operations, disposition)) = transaction_fields(action) else {
        return false;
    };
    let Some([completion]) = index
        .transactions_completed
        .get(transaction_id)
        .map(Vec::as_slice)
    else {
        return false;
    };
    after.is_some_and(|after| after < *command)
        && *command < completion.history_sequence
        && completion.history_sequence < before
        && completion.disposition == disposition
        && !operations.is_empty()
        && operations.iter().all(|operation| {
            exact_staging(
                &operation.operation_id,
                index,
                *command,
                completion.history_sequence,
            )
        })
}

fn exact_staging(
    operation_id: &OperationId,
    index: &HistoryIndex,
    command: u64,
    completion: u64,
) -> bool {
    let Some([accepted]) = index.accepted.get(operation_id).map(Vec::as_slice) else {
        return false;
    };
    let Some([terminal]) = index.terminals.get(operation_id).map(Vec::as_slice) else {
        return false;
    };
    index.rejected.get(operation_id).map_or(0, Vec::len) == 0
        && command < *accepted
        && *accepted < terminal.history_sequence
        && terminal.history_sequence < completion
        && terminal.status == TerminalStatus::TransactionStaged
        && terminal.code.is_none()
}

fn transaction_command(action: &ScenarioAction) -> Option<AdapterCommand> {
    let ScenarioAction::ExecuteTransaction {
        producer_id,
        transaction_id,
        operations,
        method,
        disposition,
        topic_identity_operation_id,
        timeout_ms,
    } = action
    else {
        return None;
    };
    Some(AdapterCommand::ExecuteTransaction {
        producer_id: producer_id.clone(),
        transaction_id: transaction_id.clone(),
        operations: operations.clone(),
        method: *method,
        disposition: *disposition,
        validate_topic_uuids: topic_identity_operation_id.is_some(),
        timeout_ms: *timeout_ms,
    })
}

fn transaction_fields(
    action: &ScenarioAction,
) -> Option<(&OperationId, &[BatchRecord], TransactionDisposition)> {
    match action {
        ScenarioAction::ExecuteTransaction {
            transaction_id,
            operations,
            disposition,
            ..
        } => Some((transaction_id, operations, *disposition)),
        _ => None,
    }
}

fn recovery_control_id(action: &ScenarioAction) -> Option<&EnvironmentOperationId> {
    match action {
        ScenarioAction::AlterNetworkFault(action) if action.state == NetworkFaultState::Absent => {
            Some(&action.operation_id)
        }
        ScenarioAction::CutNetworkConnections(action) => Some(&action.operation_id),
        _ => None,
    }
}

fn network_control_id(action: &ScenarioAction) -> Option<&EnvironmentOperationId> {
    match action {
        ScenarioAction::AlterNetworkFault(action) => Some(&action.operation_id),
        ScenarioAction::CutNetworkConnections(action) => Some(&action.operation_id),
        _ => None,
    }
}

fn control_sequence(index: &HistoryIndex, operation: &EnvironmentOperationId) -> Option<u64> {
    let mut values = index
        .network_proxy_controls
        .iter()
        .filter(|(_, control)| control.operation_id() == operation);
    let (sequence, _) = values.next()?;
    values.next().is_none().then_some(*sequence)
}

fn recovery_references(
    action: &ScenarioAction,
    index: &HistoryIndex,
    control: Option<u64>,
) -> Vec<String> {
    let mut references = control
        .map(|sequence| vec![format!("history:{sequence}")])
        .unwrap_or_default();
    if let Some(expected) = transaction_command(action) {
        references.extend(index.commands.iter().filter_map(|(sequence, _, command)| {
            (command == &expected).then(|| format!("history:{sequence}"))
        }));
    }
    if let Some((transaction_id, operations, _)) = transaction_fields(action) {
        for operation in operations {
            references.extend(
                index
                    .accepted
                    .get(&operation.operation_id)
                    .into_iter()
                    .flatten()
                    .map(|sequence| format!("history:{sequence}")),
            );
            references.extend(
                index
                    .terminals
                    .get(&operation.operation_id)
                    .into_iter()
                    .flatten()
                    .map(|terminal| format!("history:{}", terminal.history_sequence)),
            );
        }
        references.extend(
            index
                .transactions_completed
                .get(transaction_id)
                .into_iter()
                .flatten()
                .map(|completion| format!("history:{}", completion.history_sequence)),
        );
    }
    references
}
