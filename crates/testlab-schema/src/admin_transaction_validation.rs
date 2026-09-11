//! Transaction Admin validation bounds exact fixture expectations and selections.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_TRANSACTIONS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    match action {
        ScenarioAction::ListTransactions(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            validate_count(
                &action.operation_id,
                action.expected_transactions.len(),
                problems,
            );
            let mut prior: Option<&str> = None;
            for expected in &action.expected_transactions {
                validate_id(&action.operation_id, &expected.transactional_id, problems);
                validate_state(&action.operation_id, &expected.expected_state, problems);
                if prior
                    .is_some_and(|prior| prior.as_bytes() >= expected.transactional_id.as_bytes())
                {
                    problems.push(format!(
                        "admin operation {} expected_transactions must be unique transactional-ID byte order",
                        action.operation_id
                    ));
                }
                prior = Some(&expected.transactional_id);
            }
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
            true
        }
        ScenarioAction::DescribeTransactions(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            validate_count(&action.operation_id, action.transactions.len(), problems);
            let mut ids = BTreeSet::new();
            for expected in &action.transactions {
                validate_id(&action.operation_id, &expected.transactional_id, problems);
                validate_state(&action.operation_id, &expected.expected_state, problems);
                if !ids.insert(&expected.transactional_id) {
                    problems.push(format!(
                        "admin operation {} transactions contain a duplicate transactional_id",
                        action.operation_id
                    ));
                }
                if expected.expected_transaction_timeout_ms <= 0 {
                    problems.push(format!(
                        "admin operation {} expected_transaction_timeout_ms must be positive",
                        action.operation_id
                    ));
                }
                validate_topics(&action.operation_id, &expected.expected_topics, problems);
            }
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
            true
        }
        _ => false,
    }
}

fn validate_count(operation_id: &OperationId, count: usize, problems: &mut Vec<String>) {
    if !(1..=MAX_TRANSACTIONS).contains(&count) {
        problems.push(format!(
            "admin operation {operation_id} must select 1 to {MAX_TRANSACTIONS} transactions"
        ));
    }
}

fn validate_id(operation_id: &OperationId, value: &str, problems: &mut Vec<String>) {
    if value.is_empty() || value.len() > 249 || value.chars().any(char::is_whitespace) {
        problems.push(format!(
            "admin operation {operation_id} has invalid transactional_id"
        ));
    }
}

fn validate_state(operation_id: &OperationId, value: &str, problems: &mut Vec<String>) {
    if value.is_empty() || value.len() > 64 || value.chars().any(char::is_whitespace) {
        problems.push(format!(
            "admin operation {operation_id} has invalid expected transaction state"
        ));
    }
}

fn validate_topics(
    operation_id: &OperationId,
    topics: &[crate::TransactionTopicSnapshot],
    problems: &mut Vec<String>,
) {
    if topics.len() > 32 {
        problems.push(format!(
            "admin operation {operation_id} expected_topics exceeds 32 entries"
        ));
    }
    if topics
        .windows(2)
        .any(|pair| pair[0].topic.as_bytes() >= pair[1].topic.as_bytes())
    {
        problems.push(format!(
            "admin operation {operation_id} expected_topics must be unique topic byte order"
        ));
    }
    for topic in topics {
        if topic.topic.is_empty() || topic.topic.len() > 249 {
            problems.push(format!(
                "admin operation {operation_id} has invalid expected transaction topic"
            ));
        }
        if topic.partitions.is_empty()
            || topic.partitions.len() > 32
            || topic.partitions.iter().any(|partition| *partition < 0)
            || topic.partitions.windows(2).any(|pair| pair[0] >= pair[1])
        {
            problems.push(format!(
                "admin operation {operation_id} expected topic partitions must be unique ascending nonnegative values"
            ));
        }
    }
}

#[cfg(test)]
#[path = "admin_transaction_validation_test.rs"]
mod tests;
