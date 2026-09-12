//! Plural topic-configuration mutation preserves every caller-positioned outcome.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminTopicConfigAlterationOutcome,
    AdminTopicConfigsAlteration, AlterTopicConfigsCommand, CommandId, OperationId,
    TopicConfigAlteration,
};

use crate::AdapterError;
use crate::kafkars_api::{
    ConfigAlteration as PublicAlteration, ConfigResourceAlterations, ConfigResourceType,
    KafkaError, LegacyConfigResourceReplacement, LegacyTopicConfigEntry,
    LegacyTopicConfigReplacement, TopicConfigAlterations,
};
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn alter<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterTopicConfigsCommand,
) -> Result<(), AdapterError> {
    match command.api {
        testlab_schema::TopicConfigMutationApi::Topic => {
            alter_topics(state, writer, command_id, command)
        }
        testlab_schema::TopicConfigMutationApi::Resource => {
            alter_resources(state, writer, command_id, command)
        }
        testlab_schema::TopicConfigMutationApi::LegacyTopic => {
            alter_legacy_topics(state, writer, command_id, command)
        }
        testlab_schema::TopicConfigMutationApi::LegacyResource => {
            alter_legacy_resources(state, writer, command_id, command)
        }
    }
}

fn alter_topics<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterTopicConfigsCommand,
) -> Result<(), AdapterError> {
    let changes = command.topics.iter().map(|selected| {
        TopicConfigAlterations::new(selected.topic.clone(), [incremental_alteration(selected)])
    });
    let result = state
        .client(&command.client_id)?
        .admin()
        .incremental_alter_configs(changes)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let outcomes = outcomes(
        result.into_topics().into_entries(),
        &command.topics,
        &command.operation_id,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicConfigsAltered(AdminTopicConfigsAlteration {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

fn alter_resources<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterTopicConfigsCommand,
) -> Result<(), AdapterError> {
    let changes = command.topics.iter().map(|selected| {
        ConfigResourceAlterations::new(
            ConfigResourceType::Topic,
            selected.topic.clone(),
            [incremental_alteration(selected)],
        )
    });
    let result = state
        .client(&command.client_id)?
        .admin()
        .incremental_alter_config_resources(changes)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_resources()
        .into_entries()
        .into_iter()
        .map(|(resource, result)| {
            if resource.resource_type() != ConfigResourceType::Topic {
                return Err(invalid(
                    &command.operation_id,
                    "returned a non-topic configuration resource",
                ));
            }
            Ok((resource.name().to_owned(), result))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let outcomes = outcomes(entries, &command.topics, &command.operation_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicConfigsAltered(AdminTopicConfigsAlteration {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

fn alter_legacy_topics<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterTopicConfigsCommand,
) -> Result<(), AdapterError> {
    let replacements = command.topics.iter().map(|selected| {
        LegacyTopicConfigReplacement::new(selected.topic.clone(), [legacy_entry(selected)])
    });
    let result = state
        .client(&command.client_id)?
        .admin()
        .legacy_replace_topic_configs(replacements)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let outcomes = outcomes(
        result.into_topics().into_entries(),
        &command.topics,
        &command.operation_id,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicConfigsAltered(AdminTopicConfigsAlteration {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

fn alter_legacy_resources<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterTopicConfigsCommand,
) -> Result<(), AdapterError> {
    let replacements = command.topics.iter().map(|selected| {
        LegacyConfigResourceReplacement::new(
            ConfigResourceType::Topic,
            selected.topic.clone(),
            [legacy_entry(selected)],
        )
    });
    let result = state
        .client(&command.client_id)?
        .admin()
        .legacy_replace_config_resources(replacements)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_resources()
        .into_entries()
        .into_iter()
        .map(|(resource, result)| {
            if resource.resource_type() != ConfigResourceType::Topic {
                return Err(invalid(
                    &command.operation_id,
                    "returned a non-topic legacy configuration resource",
                ));
            }
            Ok((resource.name().to_owned(), result))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let outcomes = outcomes(entries, &command.topics, &command.operation_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicConfigsAltered(AdminTopicConfigsAlteration {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

pub(crate) fn outcomes(
    entries: Vec<(String, Result<(), KafkaError>)>,
    expected: &[TopicConfigAlteration],
    operation_id: &OperationId,
) -> Result<Vec<AdminTopicConfigAlterationOutcome>, AdapterError> {
    if entries.len() != expected.len() {
        return Err(invalid(
            operation_id,
            "returned a different number of configuration-alteration outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected)
        .map(|((topic, result), expected)| {
            if topic.as_str() != expected.topic.as_str() {
                return Err(invalid(
                    operation_id,
                    "returned configuration-alteration outcomes outside caller order",
                ));
            }
            Ok(AdminTopicConfigAlterationOutcome {
                topic,
                config_name: expected.config_name.clone(),
                error_code: result
                    .err()
                    .map(|error| crate::normalize::error_code(&error)),
            })
        })
        .collect()
}

pub(crate) fn incremental_alteration(selected: &TopicConfigAlteration) -> PublicAlteration {
    match &selected.value {
        Some(value) => PublicAlteration::set(selected.config_name.clone(), value.clone()),
        None => PublicAlteration::delete(selected.config_name.clone()),
    }
}

pub(crate) fn legacy_entry(selected: &TopicConfigAlteration) -> LegacyTopicConfigEntry {
    match &selected.value {
        Some(value) => LegacyTopicConfigEntry::set(selected.config_name.clone(), value.clone()),
        None => LegacyTopicConfigEntry::restore_default(selected.config_name.clone()),
    }
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
