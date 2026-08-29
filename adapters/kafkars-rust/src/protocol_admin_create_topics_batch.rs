//! Batched topic creation preserves caller order and every public per-topic outcome.

use std::io::Write;
use std::time::{Duration, Instant};

use crate::kafkars_api::{KafkaError, NewTopic, RetryAdvice};
use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminTopicCreationOutcome, AdminTopicsCreationBatch,
    CommandId, CreateTopicsBatchCommand, OperationId,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::normalize;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn create<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: CreateTopicsBatchCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let expected_topics = command
        .topics
        .iter()
        .map(|topic| topic.topic.clone())
        .collect::<Vec<_>>();
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            let requests = command.topics.iter().map(|topic| {
                NewTopic::new(topic.topic.clone(), topic.partitions)
                    .replication_factor(topic.replication_factor)
            });
            client
                .admin()
                .create_topics(requests)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let outcomes = creation_outcomes(
        result.into_entries(),
        &command.operation_id,
        &expected_topics,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicsCreationCompleted(AdminTopicsCreationBatch {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

pub(crate) fn creation_outcomes(
    entries: Vec<(String, Result<(), KafkaError>)>,
    operation_id: &OperationId,
    expected_topics: &[String],
) -> Result<Vec<AdminTopicCreationOutcome>, AdapterError> {
    if entries.len() != expected_topics.len() {
        return Err(invalid_result(
            operation_id,
            "returned a different number of topic outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected_topics)
        .map(|((topic, result), expected_topic)| {
            if topic != expected_topic.as_str() {
                return Err(invalid_result(
                    operation_id,
                    "returned topic outcomes outside caller order",
                ));
            }
            Ok(AdminTopicCreationOutcome {
                topic,
                error_code: result.err().map(|error| normalize::error_code(&error)),
            })
        })
        .collect()
}

fn deadline_after(timeout_ms: u64) -> Instant {
    let started = Instant::now();
    started
        .checked_add(Duration::from_millis(timeout_ms))
        .unwrap_or(started)
}

fn retry_safe(error: &KafkaError) -> bool {
    error.retry_advice() == RetryAdvice::RetrySafe
}

fn invalid_result(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
