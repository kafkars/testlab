//! Batched offset-listing validation owns ordered query shapes and shared identity.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_BATCH_QUERIES: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::ListOffsetsBatch(action) = action else {
        return false;
    };
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    if !(2..=MAX_BATCH_QUERIES).contains(&action.queries.len()) {
        problems.push(format!(
            "admin operation {} queries must contain 2 to {MAX_BATCH_QUERIES} entries",
            action.operation_id
        ));
    }
    validate_queries(action, problems);
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    true
}

fn validate_queries(action: &crate::ListOffsetsBatchAction, problems: &mut Vec<String>) {
    let mut identities = BTreeSet::new();
    for query in &action.queries {
        if query.topic.is_empty() || query.topic.len() > 249 {
            problems.push(format!(
                "admin operation {} has invalid batch offset topic",
                action.operation_id
            ));
        }
        if query.partition < 0 {
            problems.push(format!(
                "admin operation {} batch offset partition must be nonnegative",
                action.operation_id
            ));
        }
        if query.expected_offset < 0 {
            problems.push(format!(
                "admin operation {} batch expected_offset must be nonnegative",
                action.operation_id
            ));
        }
        if !identities.insert((query.topic.as_str(), query.partition)) {
            problems.push(format!(
                "admin operation {} batch offset topic-partitions must be unique",
                action.operation_id
            ));
        }
    }
}
