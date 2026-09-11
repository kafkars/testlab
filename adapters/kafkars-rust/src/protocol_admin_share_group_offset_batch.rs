//! Plural Share-group offset reads retain caller order and partial public outcomes.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminShareGroupOffsetOutcome,
    AdminShareGroupOffsetsOutcome, AdminShareGroupsOffsetsListing, CommandId,
    ListShareGroupsOffsetsCommand, OperationId, ShareGroupOffsetSelection,
    ShareGroupOffsetsSelection,
};

use crate::AdapterError;
use crate::kafkars_api::{ListShareGroupOffsetsQuery, ListShareGroupOffsetsResult, TopicPartition};
use crate::protocol::emit;
use crate::protocol_admin_plural_result::{
    GroupResult, PartitionResult, ResourceResult, ordered_group_results, ordered_partition_results,
};
use crate::state::AdapterState;

pub(crate) fn list<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListShareGroupsOffsetsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .list_share_groups_offsets(command.groups.iter().map(public_query))
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let requested_groups = command
        .groups
        .iter()
        .map(|group| group.group_id.clone())
        .collect::<Vec<_>>();
    let results = ordered_group_results(
        result.into_groups().into_entries(),
        &requested_groups,
        &command.operation_id,
        "Share-group offset listing",
    )?;
    let groups = group_outcomes(results, &command.groups, &command.operation_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ShareGroupsOffsetsListed(AdminShareGroupsOffsetsListing {
                operation_id: command.operation_id,
                groups,
            }),
        ),
    )
}

fn group_outcomes(
    results: Vec<GroupResult<ListShareGroupOffsetsResult>>,
    requested: &[ShareGroupOffsetsSelection],
    operation_id: &OperationId,
) -> Result<Vec<AdminShareGroupOffsetsOutcome>, AdapterError> {
    results
        .into_iter()
        .zip(requested)
        .map(|(result, selection)| match result.result {
            ResourceResult::Failure(error_code) => Ok(AdminShareGroupOffsetsOutcome {
                group_id: result.group_id,
                error_code: Some(error_code),
                offsets: Vec::new(),
            }),
            ResourceResult::Success(listing) => {
                let requested = partition_identities(&selection.partitions);
                let offsets = ordered_partition_results(
                    listing.into_offsets().into_entries(),
                    &requested,
                    operation_id,
                    "Share-group offset",
                )?;
                Ok(AdminShareGroupOffsetsOutcome {
                    group_id: result.group_id,
                    error_code: None,
                    offsets: offset_outcomes(offsets),
                })
            }
        })
        .collect()
}

fn offset_outcomes(
    results: Vec<PartitionResult<crate::kafkars_api::ShareGroupOffset>>,
) -> Vec<AdminShareGroupOffsetOutcome> {
    results
        .into_iter()
        .map(|result| match result.result {
            ResourceResult::Success(offset) => AdminShareGroupOffsetOutcome {
                topic: result.topic,
                partition: result.partition,
                topic_id: offset.topic_id(),
                start_offset: offset.start_offset(),
                leader_epoch: offset.leader_epoch(),
                lag: offset.lag(),
                error_code: None,
            },
            ResourceResult::Failure(error_code) => AdminShareGroupOffsetOutcome {
                topic: result.topic,
                partition: result.partition,
                topic_id: [0; 16],
                start_offset: None,
                leader_epoch: None,
                lag: None,
                error_code: Some(error_code),
            },
        })
        .collect()
}

fn public_query(selection: &ShareGroupOffsetsSelection) -> ListShareGroupOffsetsQuery {
    ListShareGroupOffsetsQuery::selected(
        selection.group_id.clone(),
        selection
            .partitions
            .iter()
            .map(|partition| TopicPartition::new(partition.topic.clone(), partition.partition)),
    )
}

fn partition_identities(partitions: &[ShareGroupOffsetSelection]) -> Vec<(String, i32)> {
    partitions
        .iter()
        .map(|partition| (partition.topic.clone(), partition.partition))
        .collect()
}
