//! Exact configuration method and operand mapping uses only public constructors.

use testlab_schema::{OperationId, TopicConfigAlteration, TopicConfigMutationMethod};

use crate::AdapterError;
use crate::kafkars_api::{ConfigAlteration as PublicAlteration, LegacyTopicConfigEntry};

pub(crate) fn incremental_alteration(
    selected: &TopicConfigAlteration,
    operation_id: &OperationId,
) -> Result<PublicAlteration, AdapterError> {
    let key = selected.config_name.clone();
    match (selected.method, &selected.value) {
        (TopicConfigMutationMethod::Set, Some(value)) => {
            Ok(PublicAlteration::set(key, value.clone()))
        }
        (TopicConfigMutationMethod::Delete, None) => Ok(PublicAlteration::delete(key)),
        (TopicConfigMutationMethod::Append, Some(value)) => {
            Ok(PublicAlteration::append(key, value.clone()))
        }
        (TopicConfigMutationMethod::Subtract, Some(value)) => {
            Ok(PublicAlteration::subtract(key, value.clone()))
        }
        _ => Err(invalid(operation_id, "incremental")),
    }
}

pub(crate) fn legacy_entry(
    selected: &TopicConfigAlteration,
    operation_id: &OperationId,
) -> Result<LegacyTopicConfigEntry, AdapterError> {
    let key = selected.config_name.clone();
    match (selected.method, &selected.value) {
        (TopicConfigMutationMethod::Set, Some(value)) => {
            Ok(LegacyTopicConfigEntry::set(key, value.clone()))
        }
        (TopicConfigMutationMethod::RestoreDefault, None) => {
            Ok(LegacyTopicConfigEntry::restore_default(key))
        }
        _ => Err(invalid(operation_id, "legacy")),
    }
}

fn invalid(operation_id: &OperationId, api: &str) -> AdapterError {
    AdapterError::AdminResult(format!(
        "admin operation {operation_id} has an invalid {api} configuration method or operand"
    ))
}
