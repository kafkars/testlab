//! Plural topic descriptions preserve caller order and every public resource outcome.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminTopicDescriptionOutcome, AdminTopicDescriptionValue,
    AdminTopicPartitionDescriptionOutcome, AdminTopicsDescription, CommandId,
    DescribeTopicsCommand, OperationId,
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
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_topics(command.topics.clone())
                .include_authorized_operations(false)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let outcomes = outcomes(
        result.into_entries(),
        &command.topics,
        &command.operation_id,
    )?;
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
                Ok(description) => successful_outcome(topic, description, operation_id),
                Err(error) => Ok(AdminTopicDescriptionOutcome {
                    topic,
                    description: None,
                    error_code: Some(normalize::error_code(&error)),
                }),
            }
        })
        .collect()
}

fn successful_outcome(
    topic: String,
    description: TopicDescription,
    operation_id: &OperationId,
) -> Result<AdminTopicDescriptionOutcome, AdapterError> {
    if description.name() != topic {
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
        description: Some(AdminTopicDescriptionValue {
            topic_id: description.topic_id(),
            internal: description.is_internal(),
            partitions,
        }),
        error_code: None,
    })
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
