//! Transaction verification joins public end disposition with read-committed truth.

use testlab_schema::{
    BrokerObservation, Scenario, ScenarioAction, TransactionDisposition, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify_transactions(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        let ScenarioAction::ExecuteTransaction {
            transaction_id,
            operations,
            disposition,
            ..
        } = &step.action
        else {
            continue;
        };
        if !index.action_issued(&step.action) {
            continue;
        }
        verify_completion(transaction_id, *disposition, index, violations);
        for operation in operations {
            let count = observations
                .iter()
                .filter(|value| value.operation_id == operation.operation_id)
                .count();
            let visible = match disposition {
                TransactionDisposition::Commit => count == 1,
                TransactionDisposition::Abort => count == 0,
            };
            if !visible {
                violations.push(violation(
                    "TXN-002",
                    format!(
                        "transaction {transaction_id} expected {disposition:?} visibility for operation {}, observed {count}",
                        operation.operation_id
                    ),
                    Some(operation.operation_id.clone()),
                    observations
                        .iter()
                        .filter(|value| value.operation_id == operation.operation_id)
                        .map(|value| format!("broker-observation:{}", value.observation))
                        .collect(),
                ));
            }
        }
    }
}

fn verify_completion(
    transaction_id: &testlab_schema::OperationId,
    disposition: TransactionDisposition,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let completions = index.transactions_completed.get(transaction_id);
    let exact = completions.is_some_and(|values| {
        values.len() == 1
            && values
                .first()
                .is_some_and(|value| value.disposition == disposition)
    });
    if !exact {
        violations.push(violation(
            "TXN-001",
            format!(
                "transaction {transaction_id} expected one {disposition:?} completion, observed {}",
                completions.map_or(0, Vec::len)
            ),
            Some(transaction_id.clone()),
            completions
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence))
                .collect(),
        ));
    }
}
