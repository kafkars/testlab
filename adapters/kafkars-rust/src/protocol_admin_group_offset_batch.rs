//! Plural offset reads use one bounded public call and retain partial outcomes.

use std::io::Write;
use std::time::Duration;

use crate::kafkars_api::{
    ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsResult, TopicPartition,
};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminConsumerGroupOffsetOutcome,
    AdminConsumerGroupOffsetsListing, AdminConsumerGroupOffsetsOutcome,
    AdminConsumerGroupsOffsetsListing, CommandId, ConsumerGroupOffsetSelection,
    ConsumerGroupOffsetsSelection, ListConsumerGroupOffsetsBatchCommand,
    ListConsumerGroupsOffsetsCommand, OperationId,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::protocol_admin_plural_result::{
    GroupResult, PartitionResult, ResourceResult, ordered_group_results, ordered_partition_results,
};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::ListConsumerGroupOffsetsBatch(command) => {
            list_one_group(state, writer, command_id, command)
        }
        AdapterCommand::ListConsumerGroupsOffsets(command) => {
            list_groups(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-plural-offset-read command reached its dispatcher".to_owned(),
        )),
    }
}

fn list_one_group<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListConsumerGroupOffsetsBatchCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .list_consumer_group_offsets(command.group_id.clone())
        .partitions(public_partitions(&command.partitions))
        .require_stable(command.require_stable)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_offsets()
        .into_entries()
        .into_iter()
        .map(|(key, result)| (key, result.map(|offset| offset.committed_offset())))
        .collect();
    let requested = partition_identities(&command.partitions);
    let results = ordered_partition_results(
        entries,
        &requested,
        &command.operation_id,
        "consumer-group offset",
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConsumerGroupOffsetsListed(AdminConsumerGroupOffsetsListing {
                operation_id: command.operation_id,
                group_id: command.group_id,
                outcomes: listing_outcomes(results),
            }),
        ),
    )
}

fn list_groups<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListConsumerGroupsOffsetsCommand,
) -> Result<(), AdapterError> {
    let queries = command.groups.iter().map(public_group_query);
    let result = state
        .client(&command.client_id)?
        .admin()
        .list_consumer_groups_offsets(queries)
        .require_stable(command.require_stable)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let requested_groups = command
        .groups
        .iter()
        .map(|group| group.group_id.clone())
        .collect::<Vec<_>>();
    let group_results = ordered_group_results(
        result.into_groups().into_entries(),
        &requested_groups,
        &command.operation_id,
        "consumer-group offset listing",
    )?;
    let groups = group_listing_outcomes(group_results, &command.groups, &command.operation_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ConsumerGroupsOffsetsListed(AdminConsumerGroupsOffsetsListing {
                operation_id: command.operation_id,
                groups,
            }),
        ),
    )
}

pub(crate) fn listing_outcomes(
    results: Vec<PartitionResult<Option<i64>>>,
) -> Vec<AdminConsumerGroupOffsetOutcome> {
    results
        .into_iter()
        .map(|result| match result.result {
            ResourceResult::Success(offset) => AdminConsumerGroupOffsetOutcome {
                topic: result.topic,
                partition: result.partition,
                offset,
                error_code: None,
            },
            ResourceResult::Failure(error_code) => AdminConsumerGroupOffsetOutcome {
                topic: result.topic,
                partition: result.partition,
                offset: None,
                error_code: Some(error_code),
            },
        })
        .collect()
}

fn group_listing_outcomes(
    results: Vec<GroupResult<ListConsumerGroupOffsetsResult>>,
    requested: &[ConsumerGroupOffsetsSelection],
    operation_id: &OperationId,
) -> Result<Vec<AdminConsumerGroupOffsetsOutcome>, AdapterError> {
    results
        .into_iter()
        .zip(requested)
        .map(|(result, selection)| match result.result {
            ResourceResult::Failure(error_code) => Ok(AdminConsumerGroupOffsetsOutcome {
                group_id: result.group_id,
                error_code: Some(error_code),
                offsets: Vec::new(),
            }),
            ResourceResult::Success(listing) => {
                let entries = listing
                    .into_offsets()
                    .into_entries()
                    .into_iter()
                    .map(|(key, value)| (key, value.map(|offset| offset.committed_offset())))
                    .collect();
                let identities = partition_identities(&selection.partitions);
                let offsets = ordered_partition_results(
                    entries,
                    &identities,
                    operation_id,
                    "consumer-group offset",
                )?;
                Ok(AdminConsumerGroupOffsetsOutcome {
                    group_id: result.group_id,
                    error_code: None,
                    offsets: listing_outcomes(offsets),
                })
            }
        })
        .collect()
}

fn public_partitions(
    partitions: &[ConsumerGroupOffsetSelection],
) -> impl Iterator<Item = TopicPartition> + '_ {
    partitions
        .iter()
        .map(|entry| TopicPartition::new(entry.topic.clone(), entry.partition))
}

fn public_group_query(selection: &ConsumerGroupOffsetsSelection) -> ListConsumerGroupOffsetsQuery {
    ListConsumerGroupOffsetsQuery::selected(
        selection.group_id.clone(),
        public_partitions(&selection.partitions),
    )
}

fn partition_identities(partitions: &[ConsumerGroupOffsetSelection]) -> Vec<(String, i32)> {
    partitions
        .iter()
        .map(|entry| (entry.topic.clone(), entry.partition))
        .collect()
}
