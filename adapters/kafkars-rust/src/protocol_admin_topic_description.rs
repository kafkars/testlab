//! Topic description selects explicit packaged public Admin operations and pages.

use std::collections::BTreeSet;
use std::io::Write;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminTopicDescription, AdminTopicDescriptionPage,
    AdminTopicPageCursor, CommandId, DescribeTopicCommand, TopicDescriptionApi,
    TopicDescriptionPagination,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{Client, DescribeTopicPartitionsCursor, KafkaError, RetryAdvice};
use crate::protocol::emit;
use crate::protocol_admin_result::{
    DescribedTopicResult, described_partitions, sorted_unique_nonnegative,
};
use crate::state::AdapterState;

const MAX_PAGINATION_PAGES: usize = 10_000;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeTopicCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let (partitions, pages) = match (command.api, command.pagination) {
        (TopicDescriptionApi::Metadata, None) => {
            let entries = metadata_entries(client, &command.topic, deadline)?;
            (
                described_partitions(entries, &command.operation_id, &command.topic)?,
                Vec::new(),
            )
        }
        (TopicDescriptionApi::DescribeTopicPartitions, Some(pagination)) => {
            partition_pages(client, &command, pagination, deadline)?
        }
        _ => {
            return Err(AdapterError::AdminResult(format!(
                "admin operation {} has pagination controls inconsistent with its description API",
                command.operation_id
            )));
        }
    };
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicDescribed(AdminTopicDescription {
                operation_id: command.operation_id,
                topic: command.topic,
                partitions,
                pages,
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

fn partition_pages(
    client: &Client,
    command: &DescribeTopicCommand,
    pagination: TopicDescriptionPagination,
    deadline: Instant,
) -> Result<(Vec<i32>, Vec<AdminTopicDescriptionPage>), AdapterError> {
    let mut requested_cursor = None::<DescribeTopicPartitionsCursor>;
    let mut seen_cursors = BTreeSet::new();
    let mut all_partitions = Vec::new();
    let mut pages = Vec::new();
    loop {
        let page = retry_until_with_remaining(
            deadline,
            |remaining| {
                let request = client
                    .admin()
                    .describe_topic_partitions([command.topic.clone()])
                    .response_partition_limit(pagination.response_partition_limit)
                    .deadline_after(remaining);
                let request = match requested_cursor.clone() {
                    Some(cursor) => request.cursor(cursor),
                    None => request,
                };
                request.submit().wait()
            },
            retry_safe,
        )
        .map_err(AdapterError::Client)?;
        let (_, topics, next_cursor) = page.into_parts();
        let entries = topics.into_iter().map(topic_entry).collect();
        let page_partitions = described_partitions(entries, &command.operation_id, &command.topic)?;
        all_partitions.extend(page_partitions.iter().copied());
        pages.push(AdminTopicDescriptionPage {
            partitions: page_partitions,
            next_cursor: next_cursor.as_ref().map(cursor_fact),
        });
        let Some(next_cursor) = next_cursor else {
            break;
        };
        if !pagination.follow_cursors {
            break;
        }
        if pages.len() >= MAX_PAGINATION_PAGES {
            return Err(invalid_pagination(
                command,
                "exceeded the bounded page count",
            ));
        }
        let identity = (
            next_cursor.topic_name().to_owned(),
            next_cursor.partition_index(),
        );
        if !seen_cursors.insert(identity) {
            return Err(invalid_pagination(command, "returned a repeated cursor"));
        }
        requested_cursor = Some(next_cursor);
    }
    let all_partitions = sorted_unique_nonnegative(
        all_partitions,
        &command.operation_id,
        "paginated topic partitions",
    )?;
    Ok((all_partitions, pages))
}

fn topic_entry(
    description: crate::kafkars_api::DescribeTopicPartitionsTopic,
) -> (String, Result<DescribedTopicResult, KafkaError>) {
    let name = description.name().to_owned();
    let result = match description.error().cloned() {
        Some(error) => Err(error),
        None => Ok(DescribedTopicResult::from(description)),
    };
    (name, result)
}

fn cursor_fact(cursor: &DescribeTopicPartitionsCursor) -> AdminTopicPageCursor {
    AdminTopicPageCursor {
        topic_name: cursor.topic_name().to_owned(),
        partition_index: cursor.partition_index(),
    }
}

fn invalid_pagination(command: &DescribeTopicCommand, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!(
        "admin operation {} DescribeTopicPartitions pagination {detail}",
        command.operation_id
    ))
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
