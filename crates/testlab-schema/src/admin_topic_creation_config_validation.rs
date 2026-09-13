//! Topic-creation configurations remain bounded, unique, and non-secret evidence inputs.

use std::collections::BTreeSet;

use crate::{CreateTopicAction, TopicCreationConfig};

const MAX_CONFIGS: usize = 32;
const MAX_CONFIG_NAME_BYTES: usize = 249;
const MAX_CONFIG_VALUE_BYTES: usize = 32 * 1024;

pub(super) fn validate(action: &CreateTopicAction, problems: &mut Vec<String>) {
    validate_configs(
        &format!("admin operation {}", action.operation_id),
        &action.configs,
        problems,
    );
}

pub(crate) fn validate_configs(
    owner: &str,
    configs: &[TopicCreationConfig],
    problems: &mut Vec<String>,
) {
    if configs.len() > MAX_CONFIGS {
        problems.push(format!(
            "{owner} configs must contain at most {MAX_CONFIGS} entries"
        ));
    }
    let mut names = BTreeSet::new();
    for config in configs {
        if config.name.is_empty() || config.name.len() > MAX_CONFIG_NAME_BYTES {
            problems.push(format!("{owner} has invalid creation config name"));
        }
        if config.value.is_empty() || config.value.len() > MAX_CONFIG_VALUE_BYTES {
            problems.push(format!(
                "{owner} creation config value must contain 1 to {MAX_CONFIG_VALUE_BYTES} bytes"
            ));
        }
        if !names.insert(config.name.as_str()) {
            problems.push(format!("{owner} creation config names must be unique"));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ClientId, CreateTopicAction, OperationId, TopicCreationConfig};

    use super::validate;

    #[test]
    fn creation_configs_require_unique_bounded_names_and_values() {
        let mut action = configured_action();
        action.configs.push(TopicCreationConfig {
            name: "cleanup.policy".to_owned(),
            value: String::new(),
        });
        let mut problems = Vec::new();

        validate(&action, &mut problems);

        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("value must contain 1 to 32768 bytes"))
        );
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("config names must be unique"))
        );
    }

    fn configured_action() -> CreateTopicAction {
        CreateTopicAction {
            client_id: ClientId::new("client-1")
                .unwrap_or_else(|error| panic!("client ID: {error}")),
            operation_id: OperationId::new("configured-create")
                .unwrap_or_else(|error| panic!("operation ID: {error}")),
            topic: "orders".to_owned(),
            partitions: 1,
            replication_factor: 1,
            replica_assignments: None,
            configs: vec![TopicCreationConfig {
                name: "cleanup.policy".to_owned(),
                value: "compact".to_owned(),
            }],
            validate_only: false,
            expected_error_code: None,
            timeout_ms: 1_000,
        }
    }
}
