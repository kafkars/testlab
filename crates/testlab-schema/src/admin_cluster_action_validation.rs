//! Cluster-wide read actions require one live client and one bounded operation.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    if let ScenarioAction::ValidateFeatureUpdates(action) = action {
        validate_identity(
            &action.client_id,
            &action.operation_id,
            clients,
            operation_ids,
            problems,
        );
        validate_timeout(&action.operation_id, action.timeout_ms, problems);
        validate_feature_updates(action, problems);
        return true;
    }
    let (client_id, operation_id, timeout_ms) = match action {
        ScenarioAction::DescribeCluster(action) => {
            validate_fenced_brokers(action, problems);
            (&action.client_id, &action.operation_id, action.timeout_ms)
        }
        ScenarioAction::DescribeFeatures(action) => {
            (&action.client_id, &action.operation_id, action.timeout_ms)
        }
        ScenarioAction::DescribeMetadataQuorum(action) => {
            (&action.client_id, &action.operation_id, action.timeout_ms)
        }
        _ => return false,
    };
    validate_identity(client_id, operation_id, clients, operation_ids, problems);
    validate_timeout(operation_id, timeout_ms, problems);
    true
}

fn validate_fenced_brokers(action: &crate::DescribeClusterAction, problems: &mut Vec<String>) {
    if action.expected_fenced_broker_ids.len() > 100
        || action
            .expected_fenced_broker_ids
            .iter()
            .any(|broker| *broker < 0)
        || action
            .expected_fenced_broker_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        problems.push(format!(
            "admin operation {} expected_fenced_broker_ids must contain at most 100 strictly increasing nonnegative brokers",
            action.operation_id
        ));
    }
}

fn validate_feature_updates(
    action: &crate::ValidateFeatureUpdatesAction,
    problems: &mut Vec<String>,
) {
    if !(1..=32).contains(&action.updates.len()) {
        problems.push(format!(
            "admin operation {} updates must contain between 1 and 32 features",
            action.operation_id
        ));
    }
    if action.baseline_operation_id == action.operation_id {
        problems.push(format!(
            "admin operation {} cannot use itself as its feature baseline",
            action.operation_id
        ));
    }
    let mut names = BTreeSet::new();
    for update in &action.updates {
        if update.name.is_empty()
            || update.name.len() > 249
            || update
                .name
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            problems.push(format!(
                "admin operation {} has invalid feature name {:?}",
                action.operation_id, update.name
            ));
        }
        if !names.insert(update.name.as_str()) {
            problems.push(format!(
                "admin operation {} repeats feature {}",
                action.operation_id, update.name
            ));
        }
        let valid_level = match update.kind {
            crate::FeatureUpdateKind::Upgrade => update.max_version_level > 0,
            crate::FeatureUpdateKind::SafeDowngrade | crate::FeatureUpdateKind::UnsafeDowngrade => {
                update.max_version_level >= 0
            }
        };
        if !valid_level {
            problems.push(format!(
                "admin operation {} has invalid level {} for feature {}",
                action.operation_id, update.max_version_level, update.name
            ));
        }
    }
}
