//! Partition-leader recovery binds exact transactions to the replacement window.

#[cfg(test)]
#[path = "broker_role_transaction_progress_test.rs"]
mod tests;

use testlab_schema::{
    AdapterCommand, BatchRecord, BrokerRoleTarget, OperationId, Scenario, ScenarioAction,
    TerminalStatus, TransactionDisposition, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(
    scenario: &Scenario,
    stop_position: usize,
    target: &BrokerRoleTarget,
    index: &HistoryIndex,
    after_election: u64,
    before_restore: u64,
    violations: &mut Vec<Violation>,
) {
    if index.commands.is_empty() {
        return;
    }
    let BrokerRoleTarget::PartitionLeader { topic, partition } = target else {
        return;
    };
    let end = scenario.steps[stop_position + 1..]
        .iter()
        .position(|step| {
            matches!(&step.action, ScenarioAction::RestoreBrokerRole { target: actual, .. } if actual == target)
        })
        .map_or(scenario.steps.len(), |relative| {
            stop_position + 1 + relative
        });
    for action in scenario.steps[stop_position + 1..end]
        .iter()
        .map(|step| &step.action)
        .filter(|action| committed_target(action, topic, *partition))
    {
        if exact_progress(action, index, after_election, before_restore) {
            continue;
        }
        let transaction_id = fields(action).map(|fields| fields.0.clone());
        violations.push(violation(
            "FAULT-005",
            format!(
                "replacement leader for {topic}:{partition} expected exact committed transaction {} inside the replacement window",
                transaction_id
                    .as_ref()
                    .map_or("unknown", OperationId::as_str)
            ),
            transaction_id,
            references(action, index, after_election, before_restore),
        ));
    }
}

fn exact_progress(
    action: &ScenarioAction,
    index: &HistoryIndex,
    after_election: u64,
    before_restore: u64,
) -> bool {
    let Some(expected) = command(action) else {
        return false;
    };
    let commands = index
        .commands
        .iter()
        .filter_map(|(sequence, _, actual)| (actual == &expected).then_some(*sequence))
        .collect::<Vec<_>>();
    let [command] = commands.as_slice() else {
        return false;
    };
    let Some((transaction_id, operations, disposition)) = fields(action) else {
        return false;
    };
    let Some([completion]) = index
        .transactions_completed
        .get(transaction_id)
        .map(Vec::as_slice)
    else {
        return false;
    };
    after_election < *command
        && *command < completion.history_sequence
        && completion.history_sequence < before_restore
        && disposition == TransactionDisposition::Commit
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

fn committed_target(action: &ScenarioAction, topic: &str, partition: i32) -> bool {
    fields(action).is_some_and(|(_, operations, disposition)| {
        disposition == TransactionDisposition::Commit
            && operations.iter().any(|operation| {
                operation.record.topic == topic && operation.record.partition == partition
            })
    })
}

fn fields(
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

fn command(action: &ScenarioAction) -> Option<AdapterCommand> {
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

fn references(
    action: &ScenarioAction,
    index: &HistoryIndex,
    after_election: u64,
    before_restore: u64,
) -> Vec<String> {
    let mut result = vec![
        format!("history:{after_election}"),
        format!("history:{before_restore}"),
    ];
    if let Some(expected) = command(action) {
        result.extend(index.commands.iter().filter_map(|(sequence, _, actual)| {
            (actual == &expected).then(|| format!("history:{sequence}"))
        }));
    }
    if let Some((transaction_id, operations, _)) = fields(action) {
        for operation in operations {
            result.extend(
                index
                    .accepted
                    .get(&operation.operation_id)
                    .into_iter()
                    .flatten()
                    .map(|sequence| format!("history:{sequence}")),
            );
            result.extend(
                index
                    .terminals
                    .get(&operation.operation_id)
                    .into_iter()
                    .flatten()
                    .map(|terminal| format!("history:{}", terminal.history_sequence)),
            );
        }
        result.extend(
            index
                .transactions_completed
                .get(transaction_id)
                .into_iter()
                .flatten()
                .map(|completion| format!("history:{}", completion.history_sequence)),
        );
    }
    result
}
