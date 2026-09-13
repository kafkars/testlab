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
#[path = "admin_topic_creation_config_validation_test.rs"]
mod tests;
