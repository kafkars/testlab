//! Plural topic descriptions preserve caller order and every public resource outcome.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminTopicDescriptionOutcome, AdminTopicDescriptionValue,
    AdminTopicPartitionDescriptionOutcome, AdminTopicsDescription, CommandId,
    DescribeTopicsCommand, OperationId, TopicSelection,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{KafkaError, TopicDescription};
use crate::normalize;
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeTopicsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let outcomes = match command.selection {
        TopicSelection::Name => describe_by_name(client, &command, deadline)?,
        TopicSelection::TopicId => describe_by_id(client, &command, deadline)?,
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicsDescribed(AdminTopicsDescription {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

fn describe_by_name(
    client: &crate::kafkars_api::Client,
    command: &DescribeTopicsCommand,
    deadline: std::time::Instant,
) -> Result<Vec<AdminTopicDescriptionOutcome>, AdapterError> {
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_topics(command.topics.clone())
                .include_authorized_operations(command.include_authorized_operations)
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

fn describe_by_id(
    client: &crate::kafkars_api::Client,
    command: &DescribeTopicsCommand,
    deadline: std::time::Instant,
) -> Result<Vec<AdminTopicDescriptionOutcome>, AdapterError> {
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
                .describe_topics_by_id(ids.clone())
                .include_authorized_operations(command.include_authorized_operations)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    outcomes_by_id(result.into_entries(), &resolved, &command.operation_id)
}

fn outcomes(
    entries: Vec<(String, Result<TopicDescription, KafkaError>)>,
    expected: &[String],
    operation_id: &OperationId,
) -> Result<Vec<AdminTopicDescriptionOutcome>, AdapterError> {
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
            match result {
                Ok(description) => successful_outcome(topic, None, description, operation_id),
                Err(error) => Ok(AdminTopicDescriptionOutcome {
                    topic,
                    topic_id: None,
                    description: None,
                    error_code: Some(normalize::error_code(&error)),
                }),
            }
        })
        .collect()
}

pub(crate) fn outcomes_by_id(
    entries: Vec<([u8; 16], Result<TopicDescription, KafkaError>)>,
    expected: &[(String, [u8; 16])],
    operation_id: &OperationId,
) -> Result<Vec<AdminTopicDescriptionOutcome>, AdapterError> {
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
            match result {
                Ok(description) => {
                    successful_outcome(topic.clone(), Some(topic_id), description, operation_id)
                }
                Err(error) => Ok(AdminTopicDescriptionOutcome {
                    topic: topic.clone(),
                    topic_id: Some(topic_id),
                    description: None,
                    error_code: Some(normalize::error_code(&error)),
                }),
            }
        })
        .collect()
}

fn successful_outcome(
    topic: String,
    requested_topic_id: Option<[u8; 16]>,
    description: TopicDescription,
    operation_id: &OperationId,
) -> Result<AdminTopicDescriptionOutcome, AdapterError> {
    if description.name() != topic {
        return Err(invalid(
            operation_id,
            "returned a mismatched inner topic identity",
        ));
    }
    if requested_topic_id.is_some_and(|id| description.topic_id() != Some(id)) {
        return Err(invalid(
            operation_id,
            "returned a mismatched inner topic identity",
        ));
    }
    let partitions = description
        .partitions()
        .iter()
        .map(|partition| AdminTopicPartitionDescriptionOutcome {
            partition: partition.partition_index(),
            error_code: partition.error().map(|error| normalize::error_code(error)),
        })
        .collect();
    Ok(AdminTopicDescriptionOutcome {
        topic,
        topic_id: requested_topic_id,
        description: Some(AdminTopicDescriptionValue {
            topic_id: description.topic_id(),
            internal: description.is_internal(),
            authorized_operations: description.authorized_operations(),
            partitions,
        }),
        error_code: None,
    })
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
