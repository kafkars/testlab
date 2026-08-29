//! Transaction boundaries bind each staged set to one ordered public disposition.

use std::collections::BTreeMap;

use testlab_schema::{BatchRecord, OperationId, ProducerId, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_staging(
    transaction_id: &OperationId,
    operations: &[BatchRecord],
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let completion = exact_completion_sequence(transaction_id, index);
    for operation in operations {
        let accepted = index.accepted.get(&operation.operation_id);
        let rejected = index.rejected.get(&operation.operation_id);
        let terminals = index.terminals.get(&operation.operation_id);
        let exact = matches!(accepted.map(Vec::as_slice), Some([_]))
            && rejected.map_or(0, Vec::len) == 0
            && terminals.is_some_and(|values| {
                matches!(values.as_slice(), [terminal]
                    if terminal.status == testlab_schema::TerminalStatus::TransactionStaged
                        && terminal.code.is_none())
            });
        let ordered = exact
            && completion.is_some_and(|completion_sequence| {
                let accepted_sequence = accepted.and_then(|values| values.first()).copied();
                let terminal_sequence = terminals
                    .and_then(|values| values.first())
                    .map(|terminal| terminal.history_sequence);
                matches!((accepted_sequence, terminal_sequence), (Some(start), Some(end))
                    if start < end && end < completion_sequence)
            });
        if ordered {
            continue;
        }
        let mut evidence = accepted
            .into_iter()
            .flatten()
            .chain(rejected.into_iter().flatten())
            .map(|sequence| format!("history:{sequence}"))
            .collect::<Vec<_>>();
        evidence.extend(
            terminals
                .into_iter()
                .flatten()
                .map(|terminal| format!("history:{}", terminal.history_sequence)),
        );
        if let Some(sequence) = completion {
            evidence.push(format!("history:{sequence}"));
        }
        violations.push(violation(
            "TXN-004",
            format!(
                "transaction {transaction_id} did not accept and stage declared operation {} exactly once before completion",
                operation.operation_id
            ),
            Some(operation.operation_id.clone()),
            evidence,
        ));
    }
}

pub(crate) fn verify_successive(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let mut previous: BTreeMap<ProducerId, (OperationId, u64)> = BTreeMap::new();
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        let Some((producer_id, transaction_id, operations)) = action_fields(&step.action) else {
            continue;
        };
        let Some(completion) = exact_completion_sequence(transaction_id, index) else {
            continue;
        };
        let start = exact_start_sequence(operations, index);
        if let Some((prior_id, prior_completion)) = previous.get(producer_id) {
            let separated = start.is_some_and(|sequence| sequence > *prior_completion)
                && completion > *prior_completion;
            if !separated {
                let mut evidence = vec![format!("history:{prior_completion}")];
                if let Some(sequence) = start {
                    evidence.push(format!("history:{sequence}"));
                }
                evidence.push(format!("history:{completion}"));
                violations.push(violation(
                    "TXN-006",
                    format!(
                        "transaction {transaction_id} on producer {producer_id} overlapped prior disposition boundary {prior_id}"
                    ),
                    Some(transaction_id.clone()),
                    evidence,
                ));
            }
        }
        previous.insert(producer_id.clone(), (transaction_id.clone(), completion));
    }
}

fn action_fields(action: &ScenarioAction) -> Option<(&ProducerId, &OperationId, &[BatchRecord])> {
    match action {
        ScenarioAction::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            ..
        } => Some((producer_id, transaction_id, operations)),
        ScenarioAction::ExecuteTransactionalTransform(action) => Some((
            &action.producer_id,
            &action.transaction_id,
            &action.operations,
        )),
        _ => None,
    }
}

fn exact_completion_sequence(transaction_id: &OperationId, index: &HistoryIndex) -> Option<u64> {
    let [completion] = index.transactions_completed.get(transaction_id)?.as_slice() else {
        return None;
    };
    Some(completion.history_sequence)
}

fn exact_start_sequence(operations: &[BatchRecord], index: &HistoryIndex) -> Option<u64> {
    operations
        .iter()
        .map(|operation| {
            let [sequence] = index.accepted.get(&operation.operation_id)?.as_slice() else {
                return None;
            };
            Some(*sequence)
        })
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .min()
}
