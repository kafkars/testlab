//! Topic-configuration action validation owns bounded names and values.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_CONFIG_NAME_BYTES: usize = 249;
const MAX_CONFIG_VALUE_BYTES: usize = 32 * 1024;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let (client_id, operation_id, topic, config_name, value, timeout_ms) = match action {
        ScenarioAction::DescribeTopicConfig(action) => (
            &action.client_id,
            &action.operation_id,
            action.topic.as_str(),
            action.config_name.as_str(),
            action.expected_value.as_str(),
            action.timeout_ms,
        ),
        ScenarioAction::AlterTopicConfig(action) => (
            &action.client_id,
            &action.operation_id,
            action.topic.as_str(),
            action.config_name.as_str(),
            action.value.as_str(),
            action.timeout_ms,
        ),
        _ => return false,
    };
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
    if config_name.is_empty() || config_name.len() > MAX_CONFIG_NAME_BYTES {
        problems.push(format!(
            "admin operation {operation_id} has invalid config_name"
        ));
    }
    if value.is_empty() || value.len() > MAX_CONFIG_VALUE_BYTES {
        problems.push(format!(
            "admin operation {operation_id} configuration value must contain 1 to {MAX_CONFIG_VALUE_BYTES} bytes"
        ));
    }
    if let ScenarioAction::AlterTopicConfig(action) = action {
        crate::admin_validate_only_validation::validate_alter_config(action, problems);
    }
    validate_timeout(operation_id, timeout_ms, problems);
    true
}
