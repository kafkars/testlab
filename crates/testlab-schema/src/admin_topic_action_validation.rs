//! Topic-admin validation owns topic shapes and declarative result expectations.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_EXPECTED_PARTITIONS: usize = 10_000;
const MAX_REQUIRED_TOPICS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    match action {
        ScenarioAction::CreateTopic(action) => {
            validate_create_topic(action, clients, operation_ids, problems);
        }
        ScenarioAction::CreatePartitions(action) => {
            common(
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
            crate::admin_validate_only_validation::validate_create_partitions(action, problems);
            validate_missing_topic_error(
                &action.operation_id,
                action.expected_error_code.as_deref(),
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DeleteTopic(action) => {
            common(
                &action.client_id,
                &action.operation_id,
                &action.topic,
                clients,
                operation_ids,
                problems,
            );
            validate_missing_topic_error(
                &action.operation_id,
                action.expected_error_code.as_deref(),
                problems,
            );
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DescribeTopic(action) => {
            validate_describe_topic(action, clients, operation_ids, problems);
        }
        ScenarioAction::ListTopics(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            required_topics(&action.operation_id, &action.required_topics, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::ListOffsets(action) => {
            validate_list_offsets(action, clients, operation_ids, problems);
        }
        _ => return false,
    }
    true
}

fn validate_describe_topic(
    action: &crate::DescribeTopicAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    common(
        &action.client_id,
        &action.operation_id,
        &action.topic,
        clients,
        operation_ids,
        problems,
    );
    validate_result_or_error(
        &action.operation_id,
        action.expected_partitions.as_deref(),
        action.expected_error_code.as_deref(),
        "expected_partitions",
        problems,
    );
    if let Some(partitions) = action.expected_partitions.as_deref() {
        expected_partitions(&action.operation_id, partitions, problems);
    }
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
}

fn validate_list_offsets(
    action: &crate::ListOffsetsAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    common(
        &action.client_id,
        &action.operation_id,
        &action.topic,
        clients,
        operation_ids,
        problems,
    );
    nonnegative_partition(&action.operation_id, action.partition, problems);
    validate_result_or_error(
        &action.operation_id,
        action.expected_offset.as_ref(),
        action.expected_error_code.as_deref(),
        "expected_offset",
        problems,
    );
    if action.expected_offset.is_some_and(|offset| offset < 0) {
        problems.push(format!(
            "admin operation {} expected_offset must be nonnegative",
            action.operation_id
        ));
    }
    if action.expected_error_code.is_some() && action.partition == 0 {
        problems.push(format!(
            "admin operation {} expected missing partition must query a positive partition",
            action.operation_id
        ));
    }
    validate_missing_topic_error(
        &action.operation_id,
        action.expected_error_code.as_deref(),
        problems,
    );
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
}

fn validate_result_or_error<T: ?Sized>(
    operation_id: &OperationId,
    expected_result: Option<&T>,
    expected_error_code: Option<&str>,
    result_name: &str,
    problems: &mut Vec<String>,
) {
    if expected_result.is_some() == expected_error_code.is_some() {
        problems.push(format!(
            "admin operation {operation_id} must declare exactly one of {result_name} or expected_error_code"
        ));
    }
}

fn validate_missing_topic_error(
    operation_id: &OperationId,
    error_code: Option<&str>,
    problems: &mut Vec<String>,
) {
    if error_code.is_some_and(|code| code != crate::UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE) {
        problems.push(format!(
            "admin operation {operation_id} missing topic must expect error code {}",
            crate::UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE
        ));
    }
}

fn validate_create_topic(
    action: &crate::CreateTopicAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    common(
        &action.client_id,
        &action.operation_id,
        &action.topic,
        clients,
        operation_ids,
        problems,
    );
    if !(1..=10_000).contains(&action.partitions) {
        problems.push(format!(
            "admin operation {} partitions must be between 1 and 10000",
            action.operation_id
        ));
    }
    if !(1..=100).contains(&action.replication_factor) {
        problems.push(format!(
            "admin operation {} replication_factor must be between 1 and 100",
            action.operation_id
        ));
    }
    if let Some(code) = action.expected_error_code.as_deref()
        && !matches!(
            code,
            crate::TOPIC_ALREADY_EXISTS_ERROR_CODE | crate::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE
        )
    {
        problems.push(format!(
            "admin operation {} create-topic failure has unsupported error code {code}; expected {} or {}",
            action.operation_id,
            crate::TOPIC_ALREADY_EXISTS_ERROR_CODE,
            crate::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE
        ));
    }
    crate::admin_validate_only_validation::validate_create_topic(action, problems);
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
}

fn common(
    client_id: &ClientId,
    operation_id: &OperationId,
    topic: &str,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    validate_resource(
        client_id,
        operation_id,
        topic,
        "topic",
        249,
        clients,
        operation_ids,
        problems,
    );
}

fn nonnegative_partition(operation_id: &OperationId, partition: i32, problems: &mut Vec<String>) {
    if partition < 0 {
        problems.push(format!(
            "admin operation {operation_id} partition must be nonnegative"
        ));
    }
}

fn expected_partitions(operation_id: &OperationId, values: &[i32], problems: &mut Vec<String>) {
    if values.is_empty() || values.len() > MAX_EXPECTED_PARTITIONS {
        problems.push(format!("admin operation {operation_id} expected_partitions must contain 1 to {MAX_EXPECTED_PARTITIONS} entries"));
    }
    if values.iter().any(|value| *value < 0) || values.windows(2).any(|pair| pair[0] >= pair[1]) {
        problems.push(format!("admin operation {operation_id} expected_partitions must be sorted unique nonnegative indices"));
    }
}

fn required_topics(operation_id: &OperationId, values: &[String], problems: &mut Vec<String>) {
    if values.is_empty() || values.len() > MAX_REQUIRED_TOPICS {
        problems.push(format!("admin operation {operation_id} required_topics must contain 1 to {MAX_REQUIRED_TOPICS} entries"));
    }
    let mut unique = BTreeSet::new();
    for topic in values {
        if topic.is_empty() || topic.len() > 249 || !unique.insert(topic) {
            problems.push(format!(
                "admin operation {operation_id} required_topics must contain unique valid topics"
            ));
        }
    }
}
