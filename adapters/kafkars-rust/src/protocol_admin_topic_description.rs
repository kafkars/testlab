//! Topic description selects one explicit packaged public Admin operation.

use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminTopicDescription, CommandId, DescribeTopicCommand,
    TopicDescriptionApi,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{Client, KafkaError, RetryAdvice};
use crate::protocol::emit;
use crate::protocol_admin_result::{DescribedTopicResult, described_partitions};
use crate::state::AdapterState;

const COMPLETE_PAGE_PARTITION_LIMIT: u32 = 10_000;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeTopicCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let entries = match command.api {
        TopicDescriptionApi::Metadata => metadata_entries(client, &command.topic, deadline)?,
        TopicDescriptionApi::DescribeTopicPartitions => {
            partition_page_entries(client, &command.topic, deadline)?
        }
    };
    let partitions = described_partitions(entries, &command.operation_id, &command.topic)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicDescribed(AdminTopicDescription {
                operation_id: command.operation_id,
                topic: command.topic,
                partitions,
            }),
        ),
    )
}

fn metadata_entries(
    client: &Client,
    topic: &str,
    deadline: Instant,
) -> Result<Vec<(String, Result<DescribedTopicResult, KafkaError>)>, AdapterError> {
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_topics([topic.to_owned()])
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    Ok(result
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(DescribedTopicResult::from)))
        .collect())
}

fn partition_page_entries(
    client: &Client,
    topic: &str,
    deadline: Instant,
) -> Result<Vec<(String, Result<DescribedTopicResult, KafkaError>)>, AdapterError> {
    let page = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_topic_partitions([topic.to_owned()])
                .response_partition_limit(COMPLETE_PAGE_PARTITION_LIMIT)
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let (_, topics, next_cursor) = page.into_parts();
    if next_cursor.is_some() {
        return Err(AdapterError::AdminResult(
            "DescribeTopicPartitions exceeded the complete-page partition limit".to_owned(),
        ));
    }
    Ok(topics
        .into_iter()
        .map(|description| {
            let name = description.name().to_owned();
            let result = match description.error().cloned() {
                Some(error) => Err(error),
                None => Ok(DescribedTopicResult::from(description)),
            };
            (name, result)
        })
        .collect())
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
