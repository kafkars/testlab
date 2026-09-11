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
    let (client_id, operation_id, timeout_ms) = match action {
        ScenarioAction::DescribeCluster(action) => {
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
