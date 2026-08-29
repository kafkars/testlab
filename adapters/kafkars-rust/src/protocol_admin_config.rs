//! Topic-configuration admin commands preserve exact public resource identities.

use std::io::Write;
use std::time::Duration;

use crate::kafkars_api::{ConfigAlteration, KafkaError, TopicConfigAlterations, TopicConfigQuery};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminTopicConfigCompletion,
    AdminTopicConfigDescription, AlterTopicConfigCommand, CommandId, DescribeTopicConfigCommand,
    OperationId,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::protocol_admin_result::{take_single_result, validate_single_topic_result};
use crate::protocol_admin_validation_event::config_alteration;
use crate::state::AdapterState;

type SelectedConfig = (String, Option<String>);
type TopicConfigResult = (String, Result<Vec<SelectedConfig>, KafkaError>);

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
        AdapterCommand::AlterTopicConfig(command) => alter(state, writer, command_id, command),
        _ => Err(AdapterError::AdminResult(
            "non-config command reached admin config dispatcher".to_owned(),
        )),
    }
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
                result.map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| (entry.name().to_owned(), entry.value().map(str::to_owned)))
                        .collect::<Vec<_>>()
                }),
            )
        })
        .collect();
    let value = described_value(
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
    let Some((name, value)) = configs.next() else {
        return Err(invalid(operation_id, "returned no selected configuration"));
    };
    if configs.next().is_some() || name != expected_name {
        return Err(invalid(
            operation_id,
            "returned an unexpected selected configuration",
        ));
    }
    Ok(value)
}

fn emit_event<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    event: AdapterEvent,
) -> Result<(), AdapterError> {
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
