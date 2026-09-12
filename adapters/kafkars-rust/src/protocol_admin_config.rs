//! Topic-configuration admin commands preserve exact public resource identities.

use std::io::Write;
use std::time::Duration;

use crate::kafkars_api::{ConfigAlteration, TopicConfigAlterations, TopicConfigQuery};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminTopicConfigCompletion,
    AdminTopicConfigDescription, AdminTopicConfigsDescription, AlterTopicConfigCommand, CommandId,
    DescribeTopicConfigCommand, DescribeTopicConfigsCommand,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::protocol_admin_result::validate_single_topic_result;
use crate::protocol_admin_validation_event::config_alteration;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::DescribeTopicConfig(command) => {
            describe(state, writer, command_id, command)
        }
        AdapterCommand::DescribeTopicConfigs(command) => match command.api {
            testlab_schema::TopicConfigApi::Topic => {
                describe_batch(state, writer, command_id, command)
            }
            testlab_schema::TopicConfigApi::Resource => {
                crate::protocol_admin_config_resources::describe(state, writer, command_id, command)
            }
        },
        AdapterCommand::ListConfigResources(command) => {
            crate::protocol_admin_config_resources::list(state, writer, command_id, command)
        }
        AdapterCommand::AlterTopicConfigs(command) => {
            crate::protocol_admin_config_batch_mutation::alter(state, writer, command_id, command)
        }
        AdapterCommand::AlterTopicConfig(command) => alter(state, writer, command_id, command),
        _ => Err(AdapterError::AdminResult(
            "non-config command reached admin config dispatcher".to_owned(),
        )),
    }
}

fn describe_batch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeTopicConfigsCommand,
) -> Result<(), AdapterError> {
    let queries = command.topics.iter().map(|selected| {
        TopicConfigQuery::new(selected.topic.clone())
            .configuration_keys([selected.config_name.clone()])
    });
    let result = state
        .client(&command.client_id)?
        .admin()
        .describe_configs(queries)
        .include_synonyms(command.include_synonyms)
        .include_documentation(command.include_documentation)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_topics()
        .into_entries()
        .into_iter()
        .map(|(topic, result)| {
            (
                topic,
                result.map(crate::protocol_admin_config_entry::normalize_entries),
            )
        })
        .collect();
    let outcomes = crate::protocol_admin_config_entry::described_outcomes(
        entries,
        &command.topics,
        &command.operation_id,
    )?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::TopicConfigsDescribed(AdminTopicConfigsDescription {
            operation_id: command.operation_id,
            outcomes,
        }),
    )
}

fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeTopicConfigCommand,
) -> Result<(), AdapterError> {
    let query = TopicConfigQuery::new(command.topic.clone())
        .configuration_keys([command.config_name.clone()]);
    let result = state
        .client(&command.client_id)?
        .admin()
        .describe_configs([query])
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_topics()
        .into_entries()
        .into_iter()
        .map(|(topic, result)| {
            (
                topic,
                result.map(crate::protocol_admin_config_entry::normalize_entries),
            )
        })
        .collect();
    let value = crate::protocol_admin_config_entry::described_value(
        entries,
        &command.operation_id,
        &command.topic,
        &command.config_name,
    )?;
    emit_event(
        writer,
        command_id,
        AdapterEvent::TopicConfigDescribed(AdminTopicConfigDescription {
            operation_id: command.operation_id,
            topic: command.topic,
            config_name: command.config_name,
            value,
        }),
    )
}

fn alter<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterTopicConfigCommand,
) -> Result<(), AdapterError> {
    let validate_only = command.validate_only;
    let alterations = TopicConfigAlterations::new(
        command.topic.clone(),
        [ConfigAlteration::set(
            command.config_name.clone(),
            command.value,
        )],
    );
    let result = state
        .client(&command.client_id)?
        .admin()
        .incremental_alter_configs([alterations])
        .validate_only(validate_only)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    validate_single_topic_result(
        result.into_topics().into_entries(),
        &command.operation_id,
        &command.topic,
    )?;
    emit_event(
        writer,
        command_id,
        config_alteration(
            validate_only,
            AdminTopicConfigCompletion {
                operation_id: command.operation_id,
                topic: command.topic,
                config_name: command.config_name,
            },
        ),
    )
}

fn emit_event<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    event: AdapterEvent,
) -> Result<(), AdapterError> {
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}
