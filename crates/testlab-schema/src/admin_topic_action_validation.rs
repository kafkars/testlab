//! Topic-admin validation owns topic shapes and declarative result expectations.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

#[path = "admin_partition_replica_assignment_validation.rs"]
mod partition_replica_assignment;
#[path = "admin_topic_replica_assignment_validation.rs"]
mod replica_assignment;
#[path = "admin_topic_listing_validation.rs"]
mod topic_listing;
#[path = "admin_topic_pagination_validation.rs"]
mod topic_pagination;

const MAX_EXPECTED_PARTITIONS: usize = 10_000;

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
            partition_replica_assignment::validate(action, problems);
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
            topic_listing::validate(action, problems);
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
    topic_pagination::validate(action, problems);
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
    match (action.position, action.timestamp_millis) {
        (crate::AdminOffsetSelector::Timestamp, Some(timestamp)) if timestamp >= 0 => {}
        (crate::AdminOffsetSelector::Timestamp, Some(_)) => problems.push(format!(
            "admin operation {} timestamp_millis must be nonnegative",
            action.operation_id
        )),
        (crate::AdminOffsetSelector::Timestamp, None) => problems.push(format!(
            "admin operation {} timestamp selector requires timestamp_millis",
            action.operation_id
        )),
        (_, Some(_)) => problems.push(format!(
            "admin operation {} timestamp_millis requires the timestamp selector",
            action.operation_id
        )),
        (_, None) => {}
    }
    if action.expected_error_code.is_some() && action.partition == 0 {
        problems.push(format!(
            "admin operation {} expected missing partition must query a positive partition",
            action.operation_id
        ));
    }
    validate_missing_partition_error(
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

fn validate_missing_partition_error(
    operation_id: &OperationId,
    error_code: Option<&str>,
    problems: &mut Vec<String>,
) {
    if error_code.is_some_and(|code| code != crate::ROUTING_ERROR_CODE) {
        problems.push(format!(
            "admin operation {operation_id} absent partition must expect error code {}",
            crate::ROUTING_ERROR_CODE
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
    replica_assignment::validate(action, problems);
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
