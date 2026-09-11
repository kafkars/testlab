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
    KafkaError, TopicConfigAlterations,
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
        testlab_schema::TopicConfigApi::Topic => alter_topics(state, writer, command_id, command),
        testlab_schema::TopicConfigApi::Resource => {
            alter_resources(state, writer, command_id, command)
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
        TopicConfigAlterations::new(
            selected.topic.clone(),
            [PublicAlteration::set(
                selected.config_name.clone(),
                selected.value.clone(),
            )],
        )
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
            [PublicAlteration::set(
                selected.config_name.clone(),
                selected.value.clone(),
            )],
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

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
