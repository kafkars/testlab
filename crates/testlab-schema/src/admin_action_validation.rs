//! Admin-action validation owns public topic intent and operation identity.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ClientId, OperationId, ScenarioAction};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::CreateTopic {
            client_id,
            operation_id,
            topic,
            partitions,
            replication_factor,
            timeout_ms,
        } => {
            validate_common(
                client_id,
                operation_id,
                topic,
                clients,
                operation_ids,
                problems,
            );
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
            validate_timeout(operation_id, *timeout_ms, problems);
        }
        ScenarioAction::CreatePartitions {
            client_id,
            operation_id,
            topic,
            total_count,
            timeout_ms,
        } => {
            validate_common(
                client_id,
                operation_id,
                topic,
                clients,
                operation_ids,
                problems,
            );
            if !(1..=10_000).contains(total_count) {
                problems.push(format!(
                    "admin operation {operation_id} total_count must be between 1 and 10000"
                ));
            }
            validate_timeout(operation_id, *timeout_ms, problems);
        }
        _ => {}
    }
}

fn validate_common(
    client_id: &ClientId,
    operation_id: &OperationId,
    topic: &str,
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
    if topic.is_empty() || topic.len() > 249 {
        problems.push(format!("admin operation {operation_id} has invalid topic"));
    }
}

fn validate_timeout(operation_id: &OperationId, timeout_ms: u64, problems: &mut Vec<String>) {
    if !(100..=60_000).contains(&timeout_ms) {
        problems.push(format!(
            "admin operation {operation_id} timeout_ms must be between 100 and 60000"
        ));
    }
}
