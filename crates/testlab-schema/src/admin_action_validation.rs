//! Admin-action validation owns public topic intent and operation identity.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ClientId, OperationId, ScenarioAction};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    let ScenarioAction::CreateTopic {
        client_id,
        operation_id,
        topic,
        partitions,
        replication_factor,
        timeout_ms,
    } = action
    else {
        return;
    };
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
    if topic.is_empty() || topic.len() > 249 {
        problems.push(format!("admin operation {operation_id} has invalid topic"));
    }
    if !(1..=10_000).contains(partitions) {
        problems.push(format!(
            "admin operation {operation_id} partitions must be between 1 and 10000"
        ));
    }
    if !(1..=100).contains(replication_factor) {
        problems.push(format!(
            "admin operation {operation_id} replication_factor must be between 1 and 100"
        ));
    }
    if !(100..=60_000).contains(timeout_ms) {
        problems.push(format!(
            "admin operation {operation_id} timeout_ms must be between 100 and 60000"
        ));
    }
}
