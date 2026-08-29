//! Admin-action validation owns shared client, identity, name, and timeout rules.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ClientId, OperationId, ScenarioAction};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    if crate::admin_create_topics_batch_validation::validate(
        action,
        clients,
        operation_ids,
        problems,
    ) {
        return;
    }
    if crate::admin_delete_records_action_validation::validate(
        action,
        clients,
        operation_ids,
        problems,
    ) {
        return;
    }
    if crate::admin_topic_action_validation::validate(action, clients, operation_ids, problems) {
        return;
    }
    if crate::admin_config_action_validation::validate(action, clients, operation_ids, problems) {
        return;
    }
    crate::admin_group_action_validation::validate(action, clients, operation_ids, problems);
}

#[allow(
    clippy::too_many_arguments,
    reason = "shared admin resource validation keeps every explicit identity and bound visible"
)]
pub(super) fn validate_resource(
    client_id: &ClientId,
    operation_id: &OperationId,
    resource: &str,
    resource_kind: &str,
    maximum: usize,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    validate_identity(client_id, operation_id, clients, operation_ids, problems);
    if resource.is_empty() || resource.len() > maximum {
        problems.push(format!(
            "admin operation {operation_id} has invalid {resource_kind}"
        ));
    }
}

pub(super) fn validate_identity(
    client_id: &ClientId,
    operation_id: &OperationId,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    match clients.get(client_id) {
        Some(false) => {}
        Some(true) => problems.push(format!(
            "admin operation {operation_id} uses shut down client {client_id}"
        )),
        None => problems.push(format!(
            "admin operation {operation_id} uses missing client {client_id}"
        )),
    }
    if !operation_ids.insert(operation_id.clone()) {
        problems.push(format!("duplicate operation id {operation_id}"));
    }
}

pub(super) fn validate_timeout(
    operation_id: &OperationId,
    timeout_ms: u64,
    problems: &mut Vec<String>,
) {
    if !(100..=60_000).contains(&timeout_ms) {
        problems.push(format!(
            "admin operation {operation_id} timeout_ms must be between 100 and 60000"
        ));
    }
}
