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
        if !index.action_issued(&step.action) {
            continue;
        }
        match &step.action {
            ScenarioAction::ExecuteTransaction {
                transaction_id,
                operations,
                disposition,
                ..
            } => {
                verify_completion(transaction_id, *disposition, index, violations);
                for operation in operations {
                    verify_visibility(
                        transaction_id,
                        &operation.operation_id,
                        *disposition,
                        observations,
                        violations,
                    );
                }
            }
            ScenarioAction::FenceTransaction {
                transaction_id,
                operation,
                ..
            } => verify_fence(
                transaction_id,
                &operation.operation_id,
                index,
                observations,
                violations,
            ),
            _ => {}
        }
    }
}

fn verify_visibility(
    transaction_id: &testlab_schema::OperationId,
    operation_id: &testlab_schema::OperationId,
    disposition: TransactionDisposition,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let matching = observations
        .iter()
        .filter(|value| value.operation_id == *operation_id)
        .collect::<Vec<_>>();
    let visible = match disposition {
        TransactionDisposition::Commit => matching.len() == 1,
        TransactionDisposition::Abort => matching.is_empty(),
    };
    if !visible {
        violations.push(violation(
            "TXN-002",
            format!(
                "transaction {transaction_id} expected {disposition:?} visibility for operation {operation_id}, observed {}",
                matching.len()
            ),
            Some(operation_id.clone()),
            matching
                .iter()
                .map(|value| format!("broker-observation:{}", value.observation))
                .collect(),
        ));
    }
}

fn verify_fence(
    transaction_id: &testlab_schema::OperationId,
    operation_id: &testlab_schema::OperationId,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let outcomes = index.transactions_fenced.get(transaction_id);
    let exact = outcomes.is_some_and(|values| {
        values.len() == 1
            && values
                .first()
                .is_some_and(|value| value.commit_error_code.as_deref() == Some("fenced"))
    });
    if !exact {
        let observed = outcomes
            .into_iter()
            .flatten()
            .map(|value| {
                value
                    .commit_error_code
                    .as_deref()
                    .unwrap_or("commit_succeeded")
            })
            .collect::<Vec<_>>();
        violations.push(violation(
            "TXN-003",
            format!(
                "transaction {transaction_id} expected one fenced commit result, observed {observed:?}"
            ),
            Some(operation_id.clone()),
            outcomes
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence))
                .collect(),
        ));
    }
    let leaked = observations
        .iter()
        .filter(|value| value.operation_id == *operation_id)
        .collect::<Vec<_>>();
    if !leaked.is_empty() {
        violations.push(violation(
            "TXN-003",
            format!(
                "fenced transaction {transaction_id} leaked operation {operation_id} to {} read-committed observation(s)",
                leaked.len()
            ),
            Some(operation_id.clone()),
            leaked
                .iter()
                .map(|value| format!("broker-observation:{}", value.observation))
                .collect(),
        ));
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
