//! Admin-action validation owns public topic intent and operation identity.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ClientId, OperationId, ScenarioAction};

const MAX_EXPECTED_PARTITIONS: usize = 10_000;
const MAX_REQUIRED_TOPICS: usize = 32;

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
        ScenarioAction::CreatePartitions(action) => {
            validate_common(
                &action.client_id,
                &action.operation_id,
                &action.topic,
                clients,
                operation_ids,
                problems,
            );
            if !(1..=10_000).contains(&action.total_count) {
                problems.push(format!(
                    "admin operation {} total_count must be between 1 and 10000",
                    action.operation_id
                ));
            }
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        action @ (ScenarioAction::DescribeTopic(_)
        | ScenarioAction::ListTopics(_)
        | ScenarioAction::ListOffsets(_)) => {
            validate_query(action, clients, operation_ids, problems);
        }
        _ => {}
    }
}

fn validate_query(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::DescribeTopic(action) => {
            validate_common(
                &action.client_id,
                &action.operation_id,
                &action.topic,
                clients,
                operation_ids,
                problems,
            );
            validate_expected_partitions(
                &action.operation_id,
                &action.expected_partitions,
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::ListTopics(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            validate_required_topics(&action.operation_id, &action.required_topics, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::ListOffsets(action) => {
            validate_common(
                &action.client_id,
                &action.operation_id,
                &action.topic,
                clients,
                operation_ids,
                problems,
            );
            if action.partition < 0 {
                problems.push(format!(
                    "admin operation {} partition must be nonnegative",
                    action.operation_id
                ));
            }
            if action.expected_offset < 0 {
                problems.push(format!(
                    "admin operation {} expected_offset must be nonnegative",
                    action.operation_id
                ));
            }
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
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
    validate_identity(client_id, operation_id, clients, operation_ids, problems);
    validate_topic(operation_id, topic, problems);
}

fn validate_identity(
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

fn validate_topic(operation_id: &OperationId, topic: &str, problems: &mut Vec<String>) {
    if topic.is_empty() || topic.len() > 249 {
        problems.push(format!("admin operation {operation_id} has invalid topic"));
    }
}

fn validate_expected_partitions(
    operation_id: &OperationId,
    expected_partitions: &[i32],
    problems: &mut Vec<String>,
) {
    if expected_partitions.is_empty() {
        problems.push(format!(
            "admin operation {operation_id} expected_partitions must not be empty"
        ));
    }
    if expected_partitions.len() > MAX_EXPECTED_PARTITIONS {
        problems.push(format!(
            "admin operation {operation_id} expected_partitions has {} entries, maximum is {MAX_EXPECTED_PARTITIONS}",
            expected_partitions.len()
        ));
    }
    if expected_partitions.iter().any(|partition| *partition < 0) {
        problems.push(format!(
            "admin operation {operation_id} expected_partitions must be nonnegative"
        ));
    }
    if expected_partitions
        .windows(2)
        .any(|partitions| partitions[0] >= partitions[1])
    {
        problems.push(format!(
            "admin operation {operation_id} expected_partitions must be sorted and unique"
        ));
    }
}

fn validate_required_topics(
    operation_id: &OperationId,
    required_topics: &[String],
    problems: &mut Vec<String>,
) {
    if required_topics.is_empty() {
        problems.push(format!(
            "admin operation {operation_id} required_topics must not be empty"
        ));
    }
    if required_topics.len() > MAX_REQUIRED_TOPICS {
        problems.push(format!(
            "admin operation {operation_id} required_topics has {} entries, maximum is {MAX_REQUIRED_TOPICS}",
            required_topics.len()
        ));
    }
    let mut unique = BTreeSet::new();
    for topic in required_topics {
        if topic.is_empty() || topic.len() > 249 {
            problems.push(format!(
                "admin operation {operation_id} required_topics contains an invalid topic"
            ));
        }
        if !unique.insert(topic.as_str()) {
            problems.push(format!(
                "admin operation {operation_id} required_topics contains a duplicate topic"
            ));
        }
    }
}

fn validate_timeout(operation_id: &OperationId, timeout_ms: u64, problems: &mut Vec<String>) {
    if !(100..=60_000).contains(&timeout_ms) {
        problems.push(format!(
            "admin operation {operation_id} timeout_ms must be between 100 and 60000"
        ));
    }
}
