//! Plural topic deletion preserves caller order and every public resource outcome.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminTopicDeletionOutcome, AdminTopicsDeletion, CommandId,
    DeleteTopicsCommand, OperationId, TopicSelection,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::KafkaError;
use crate::normalize;
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteTopicsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let outcomes = match command.selection {
        TopicSelection::Name => delete_by_name(client, &command, deadline)?,
        TopicSelection::TopicId => delete_by_id(client, &command, deadline)?,
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicsDeleted(AdminTopicsDeletion {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

fn delete_by_name(
    client: &crate::kafkars_api::Client,
    command: &DeleteTopicsCommand,
    deadline: std::time::Instant,
) -> Result<Vec<AdminTopicDeletionOutcome>, AdapterError> {
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .delete_topics(command.topics.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    outcomes(
        result.into_entries(),
        &command.topics,
        &command.operation_id,
    )
}

fn delete_by_id(
    client: &crate::kafkars_api::Client,
    command: &DeleteTopicsCommand,
    deadline: std::time::Instant,
) -> Result<Vec<AdminTopicDeletionOutcome>, AdapterError> {
    let resolved = crate::protocol_admin_topic_ids::resolve(
        client,
        &command.topics,
        &command.operation_id,
        deadline,
    )?;
    let ids = resolved.iter().map(|(_, id)| *id).collect::<Vec<_>>();
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .delete_topics_by_id(ids.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    outcomes_by_id(result.into_entries(), &resolved, &command.operation_id)
}

pub(crate) fn outcomes(
    entries: Vec<(String, Result<(), KafkaError>)>,
    expected: &[String],
    operation_id: &OperationId,
) -> Result<Vec<AdminTopicDeletionOutcome>, AdapterError> {
    if entries.len() != expected.len() {
        return Err(invalid(
            operation_id,
            "returned a different number of topic outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected)
        .map(|((topic, result), expected)| {
            if &topic != expected {
                return Err(invalid(
                    operation_id,
                    "returned topic outcomes outside caller order",
                ));
            }
            Ok(AdminTopicDeletionOutcome {
                topic,
                topic_id: None,
                error_code: result.err().map(|error| normalize::error_code(&error)),
            })
        })
        .collect()
}

pub(crate) fn outcomes_by_id(
    entries: Vec<([u8; 16], Result<(), KafkaError>)>,
    expected: &[(String, [u8; 16])],
    operation_id: &OperationId,
) -> Result<Vec<AdminTopicDeletionOutcome>, AdapterError> {
    if entries.len() != expected.len() {
        return Err(invalid(
            operation_id,
            "returned a different number of topic-ID outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected)
        .map(|((topic_id, result), (topic, expected_id))| {
            if &topic_id != expected_id {
                return Err(invalid(
                    operation_id,
                    "returned topic-ID outcomes outside caller order",
                ));
            }
            Ok(AdminTopicDeletionOutcome {
                topic: topic.clone(),
                topic_id: Some(topic_id),
                error_code: result.err().map(|error| normalize::error_code(&error)),
            })
        })
        .collect()
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
