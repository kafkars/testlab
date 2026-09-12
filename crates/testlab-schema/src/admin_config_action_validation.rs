//! Topic-configuration action validation owns bounded names and values.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_CONFIG_NAME_BYTES: usize = 249;
const MAX_CONFIG_VALUE_BYTES: usize = 32 * 1024;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    if let ScenarioAction::ListConfigResources(action) = action {
        validate_resource_listing(action, clients, operation_ids, problems);
        return true;
    }
    if let ScenarioAction::DescribeTopicConfigs(action) = action {
        validate_description_batch(action, clients, operation_ids, problems);
        return true;
    }
    if let ScenarioAction::AlterTopicConfigs(action) = action {
        validate_alteration_batch(action, clients, operation_ids, problems);
        return true;
    }
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

fn validate_resource_listing(
    action: &crate::ListConfigResourcesAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    if !(2..=32).contains(&action.required_resources.len()) {
        problems.push(format!(
            "admin operation {} required_resources must contain between 2 and 32 entries",
            action.operation_id
        ));
    }
    let mut resources = BTreeSet::new();
    if action.required_resources.iter().any(|name| {
        name.is_empty()
            || name.len() > 249
            || name.chars().any(char::is_whitespace)
            || !resources.insert(name.as_str())
    }) {
        problems.push(format!(
            "admin operation {} required_resources must contain unique valid names",
            action.operation_id
        ));
    }
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
}

fn validate_description_batch(
    action: &crate::DescribeTopicConfigsAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    if !(2..=32).contains(&action.topics.len()) {
        problems.push(format!(
            "admin operation {} topics must contain between 2 and 32 entries",
            action.operation_id
        ));
    }
    let mut topics = BTreeSet::new();
    for selected in &action.topics {
        if selected.topic.is_empty()
            || selected.topic.len() > 249
            || !topics.insert(&selected.topic)
        {
            problems.push(format!(
                "admin operation {} topics must contain unique valid names",
                action.operation_id
            ));
        }
        if selected.config_name.is_empty() || selected.config_name.len() > MAX_CONFIG_NAME_BYTES {
            problems.push(format!(
                "admin operation {} has invalid config_name",
                action.operation_id
            ));
        }
        if selected.expected_value.is_empty()
            || selected.expected_value.len() > MAX_CONFIG_VALUE_BYTES
        {
            problems.push(format!(
                "admin operation {} configuration value must contain 1 to {MAX_CONFIG_VALUE_BYTES} bytes",
                action.operation_id
            ));
        }
    }
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
}

fn validate_alteration_batch(
    action: &crate::AlterTopicConfigsAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    if action.baseline_operation_id == action.operation_id {
        problems.push(format!(
            "admin operation {} cannot use itself as its configuration baseline",
            action.operation_id
        ));
    }
    if !(2..=32).contains(&action.topics.len()) {
        problems.push(format!(
            "admin operation {} topics must contain between 2 and 32 entries",
            action.operation_id
        ));
    }
    let mut topics = BTreeSet::new();
    for selected in &action.topics {
        if selected.topic.is_empty()
            || selected.topic.len() > 249
            || !topics.insert(&selected.topic)
        {
            problems.push(format!(
                "admin operation {} topics must contain unique valid names",
                action.operation_id
            ));
        }
        if selected.config_name.is_empty() || selected.config_name.len() > MAX_CONFIG_NAME_BYTES {
            problems.push(format!(
                "admin operation {} has invalid config_name",
                action.operation_id
            ));
        }
        for value in [&selected.expected_previous_value, &selected.value] {
            if value.is_empty() || value.len() > MAX_CONFIG_VALUE_BYTES {
                problems.push(format!(
                    "admin operation {} configuration value must contain 1 to {MAX_CONFIG_VALUE_BYTES} bytes",
                    action.operation_id
                ));
            }
        }
        if selected
            .operation_value
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_CONFIG_VALUE_BYTES)
        {
            problems.push(format!(
                "admin operation {} operation_value must contain 1 to {MAX_CONFIG_VALUE_BYTES} bytes",
                action.operation_id
            ));
        }
        let legacy = matches!(
            action.api,
            crate::TopicConfigMutationApi::LegacyTopic
                | crate::TopicConfigMutationApi::LegacyResource
        );
        let compatible = match selected.method {
            crate::TopicConfigMutationMethod::Set => true,
            crate::TopicConfigMutationMethod::RestoreDefault => legacy,
            crate::TopicConfigMutationMethod::Delete
            | crate::TopicConfigMutationMethod::Append
            | crate::TopicConfigMutationMethod::Subtract => !legacy,
        };
        if !compatible {
            problems.push(format!(
                "admin operation {} configuration method is incompatible with its API",
                action.operation_id
            ));
        }
        let requires_value = matches!(
            selected.method,
            crate::TopicConfigMutationMethod::Append | crate::TopicConfigMutationMethod::Subtract
        );
        if requires_value != selected.operation_value.is_some() {
            problems.push(format!(
                "admin operation {} operation_value is required only for append or subtract",
                action.operation_id
            ));
        }
    }
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
}
