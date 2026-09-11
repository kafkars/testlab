//! Transaction discovery fixtures require initialized, closed transactional owners.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ProducerId, Scenario, ScenarioAction};

#[derive(Clone, Debug)]
struct Owner {
    transactional_id: String,
    closed: bool,
}

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut owners = BTreeMap::<ProducerId, Owner>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateTransactionalProducer {
                producer_id,
                transactional_id,
                expected_error_code: None,
                ..
            } => record(&mut owners, producer_id, transactional_id),
            ScenarioAction::FenceTransaction {
                replacement_producer_id,
                transactional_id,
                ..
            } => record(&mut owners, replacement_producer_id, transactional_id),
            ScenarioAction::CloseTransactionalProducer(action) => {
                if let Some(owner) = owners.get_mut(&action.producer_id) {
                    owner.closed = true;
                }
            }
            ScenarioAction::ListTransactions(action) => {
                let expected = action
                    .expected_transactions
                    .iter()
                    .map(|transaction| transaction.transactional_id.as_str())
                    .collect::<BTreeSet<_>>();
                let initialized = owners
                    .values()
                    .map(|owner| owner.transactional_id.as_str())
                    .collect::<BTreeSet<_>>();
                if expected != initialized {
                    problems.push(format!(
                        "admin operation {} must list every successfully initialized transactional_id exactly once",
                        action.operation_id
                    ));
                }
                require_closed(&action.operation_id, expected, &owners, problems);
            }
            ScenarioAction::DescribeTransactions(action) => require_closed(
                &action.operation_id,
                action
                    .transactions
                    .iter()
                    .map(|transaction| transaction.transactional_id.as_str()),
                &owners,
                problems,
            ),
            _ => {}
        }
    }
}

fn record(owners: &mut BTreeMap<ProducerId, Owner>, producer_id: &ProducerId, value: &str) {
    owners.insert(
        producer_id.clone(),
        Owner {
            transactional_id: value.to_owned(),
            closed: false,
        },
    );
}

fn require_closed<'a>(
    operation_id: &crate::OperationId,
    transactional_ids: impl IntoIterator<Item = &'a str>,
    owners: &BTreeMap<ProducerId, Owner>,
    problems: &mut Vec<String>,
) {
    for transactional_id in transactional_ids {
        let matching = owners
            .values()
            .filter(|owner| owner.transactional_id == transactional_id)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            problems.push(format!(
                "admin operation {operation_id} requires prior successful initialization of transactional_id {transactional_id}"
            ));
        } else if matching.iter().any(|owner| !owner.closed) {
            problems.push(format!(
                "admin operation {operation_id} requires every producer for transactional_id {transactional_id} to be closed"
            ));
        }
    }
}

#[cfg(test)]
#[path = "admin_transaction_transition_validation_test.rs"]
mod tests;
