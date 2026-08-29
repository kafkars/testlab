//! Transaction verification joins public end disposition with read-committed truth.

use testlab_schema::{
    BrokerObservation, Scenario, ScenarioAction, TransactionDisposition, Violation,
};

use crate::index::HistoryIndex;
use crate::support::violation;
use crate::verify_index::observations_by_operation;

pub(crate) fn verify_transactions(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    let observed = observations_by_operation(observations);
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
                crate::transaction_boundaries::verify_staging(
                    transaction_id,
                    operations,
                    index,
                    violations,
                );
                crate::transaction_records::verify(
                    transaction_id,
                    operations,
                    *disposition,
                    index,
                    &observed,
                    violations,
                );
            }
            ScenarioAction::ExecuteTransactionalTransform(action) => {
                verify_completion(
                    &action.transaction_id,
                    action.disposition,
                    index,
                    violations,
                );
                crate::transaction_boundaries::verify_staging(
                    &action.transaction_id,
                    &action.operations,
                    index,
                    violations,
                );
                crate::transaction_records::verify(
                    &action.transaction_id,
                    &action.operations,
                    action.disposition,
                    index,
                    &observed,
                    violations,
                );
                crate::transaction_offsets::verify(scenario, action, index, &observed, violations);
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
    crate::transaction_boundaries::verify_successive(scenario, index, violations);
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
                .is_some_and(|value| is_explicit_fence(value.commit_error_code.as_deref()))
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

fn is_explicit_fence(code: Option<&str>) -> bool {
    matches!(
        code,
        Some("fenced" | "fenced:broker_47" | "fenced:broker_90")
    )
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
