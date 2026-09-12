//! Described configuration entries preserve complete public metadata.

use testlab_schema::{
    AdminConfigEntryMetadata, AdminConfigSynonym, AdminTopicConfigDescriptionOutcome, OperationId,
    TopicConfigSelection,
};

use crate::AdapterError;
use crate::kafkars_api::{ConfigEntry, KafkaError};
use crate::protocol_admin_result::take_single_result;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SelectedConfig {
    pub(crate) name: String,
    pub(crate) value: Option<String>,
    pub(crate) metadata: AdminConfigEntryMetadata,
}

pub(crate) type TopicConfigResult = (String, Result<Vec<SelectedConfig>, KafkaError>);

pub(crate) fn normalize_entries(entries: Vec<ConfigEntry>) -> Vec<SelectedConfig> {
    entries.into_iter().map(normalize_entry).collect()
}

fn normalize_entry(entry: ConfigEntry) -> SelectedConfig {
    SelectedConfig {
        name: entry.name().to_owned(),
        value: entry.value().map(str::to_owned),
        metadata: AdminConfigEntryMetadata {
            read_only: entry.read_only(),
            source: entry.source(),
            sensitive: entry.sensitive(),
            synonyms: entry
                .synonyms()
                .iter()
                .map(|synonym| AdminConfigSynonym {
                    name: synonym.name().to_owned(),
                    value: synonym.value().map(str::to_owned),
                    source: synonym.source(),
                })
                .collect(),
            config_type: entry.config_type(),
            documentation: entry.documentation().map(str::to_owned),
        },
    }
}

pub(crate) fn described_value(
    entries: Vec<TopicConfigResult>,
    operation_id: &OperationId,
    expected_topic: &str,
    expected_name: &str,
) -> Result<Option<String>, AdapterError> {
    let configs = take_single_result(
        entries,
        operation_id,
        |topic| topic == expected_topic,
        "topic configuration",
    )?;
    let mut configs = configs.into_iter();
    let Some(config) = configs.next() else {
        return Err(invalid(operation_id, "returned no selected configuration"));
    };
    if configs.next().is_some() || config.name != expected_name {
        return Err(invalid(
            operation_id,
            "returned an unexpected selected configuration",
        ));
    }
    Ok(config.value)
}

pub(crate) fn described_outcomes(
    entries: Vec<TopicConfigResult>,
    expected: &[TopicConfigSelection],
    operation_id: &OperationId,
) -> Result<Vec<AdminTopicConfigDescriptionOutcome>, AdapterError> {
    if entries.len() != expected.len() {
        return Err(invalid(
            operation_id,
            "returned a different number of topic-configuration outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected)
        .map(|((topic, result), expected)| {
            if topic.as_str() != expected.topic.as_str() {
                return Err(invalid(
                    operation_id,
                    "returned topic-configuration outcomes outside caller order",
                ));
            }
            match result {
                Ok(configs) => successful_outcome(topic, configs, expected, operation_id),
                Err(error) => Ok(AdminTopicConfigDescriptionOutcome {
                    topic,
                    config_name: expected.config_name.clone(),
                    value: None,
                    metadata: None,
                    error_code: Some(crate::normalize::error_code(&error)),
                }),
            }
        })
        .collect()
}

fn successful_outcome(
    topic: String,
    configs: Vec<SelectedConfig>,
    expected: &TopicConfigSelection,
    operation_id: &OperationId,
) -> Result<AdminTopicConfigDescriptionOutcome, AdapterError> {
    let mut configs = configs.into_iter();
    let Some(config) = configs.next() else {
        return Err(invalid(operation_id, "returned no selected configuration"));
    };
    if configs.next().is_some() || config.name.as_str() != expected.config_name.as_str() {
        return Err(invalid(
            operation_id,
            "returned an unexpected selected configuration",
        ));
    }
    Ok(AdminTopicConfigDescriptionOutcome {
        topic,
        config_name: config.name,
        value: config.value,
        metadata: Some(config.metadata),
        error_code: None,
    })
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
