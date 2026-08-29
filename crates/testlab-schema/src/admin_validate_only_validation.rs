//! Validate-only admin requests bind public validation to exact modeled preconditions.

use crate::{AlterTopicConfigAction, CreatePartitionsAction, CreateTopicAction};

const MAX_CONFIG_VALUE_BYTES: usize = 32 * 1024;

pub(crate) fn validate_create_topic(action: &CreateTopicAction, problems: &mut Vec<String>) {
    if action.validate_only && action.expected_error_code.is_some() {
        problems.push(format!(
            "admin operation {} validate_only cannot declare expected_error_code",
            action.operation_id
        ));
    }
}

pub(crate) fn validate_create_partitions(
    action: &CreatePartitionsAction,
    problems: &mut Vec<String>,
) {
    if action.validate_only != action.expected_current_count.is_some() {
        problems.push(format!(
            "admin operation {} must declare expected_current_count exactly when validate_only is true",
            action.operation_id
        ));
    }
    if action
        .expected_current_count
        .is_some_and(|count| count <= 0 || count >= action.total_count)
    {
        problems.push(format!(
            "admin operation {} expected_current_count must be positive and less than total_count",
            action.operation_id
        ));
    }
    if action.validate_only && action.expected_error_code.is_some() {
        problems.push(format!(
            "admin operation {} validate_only cannot declare expected_error_code",
            action.operation_id
        ));
    }
}

pub(crate) fn validate_alter_config(action: &AlterTopicConfigAction, problems: &mut Vec<String>) {
    if action.validate_only != action.expected_current_value.is_some() {
        problems.push(format!(
            "admin operation {} must declare expected_current_value exactly when validate_only is true",
            action.operation_id
        ));
    }
    if let Some(current) = action.expected_current_value.as_deref() {
        if current.is_empty() || current.len() > MAX_CONFIG_VALUE_BYTES {
            problems.push(format!(
                "admin operation {} expected_current_value must contain 1 to {MAX_CONFIG_VALUE_BYTES} bytes",
                action.operation_id
            ));
        }
        if current == action.value {
            problems.push(format!(
                "admin operation {} expected_current_value must differ from the requested value",
                action.operation_id
            ));
        }
    }
}

pub(crate) fn validate_partition_transition(
    action: &CreatePartitionsAction,
    actual_count: Option<i32>,
    problems: &mut Vec<String>,
) {
    let Some(expected_count) = action.expected_current_count else {
        problems.push(format!(
            "admin operation {} validate_only requires an expected current partition count",
            action.operation_id
        ));
        return;
    };
    if actual_count != Some(expected_count) {
        problems.push(format!(
            "admin operation {} validate_only requires a prior exact partition count of {expected_count} for {}",
            action.operation_id, action.topic
        ));
    }
}

pub(crate) fn validate_config_transition(
    action: &AlterTopicConfigAction,
    described_value: Option<&str>,
    problems: &mut Vec<String>,
) {
    let Some(expected_value) = action.expected_current_value.as_deref() else {
        problems.push(format!(
            "admin operation {} validate_only requires an expected current topic-configuration value",
            action.operation_id
        ));
        return;
    };
    if described_value != Some(expected_value) {
        problems.push(format!(
            "admin operation {} validate_only requires a prior exact topic-configuration value",
            action.operation_id
        ));
    }
}
