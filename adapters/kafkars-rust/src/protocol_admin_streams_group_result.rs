//! Streams-group normalization retains exact caller order and bounded public facts.

use std::collections::BTreeSet;
use std::time::Duration;

use testlab_schema::{AdminStreamsGroupDescription, AdminStreamsGroupOffset, OperationId};

use crate::AdapterError;
use crate::kafkars_api::{
    ConsumerGroupOffset, DescribeStreamsGroupResult, DescribeStreamsGroupsResult, KafkaError,
    ListStreamsGroupOffsetsResult, ListStreamsGroupsOffsetsResult, StreamsGroupDescription,
    TopicPartition,
};
use crate::protocol_admin_plural_result::{
    ResourceResult, ordered_group_results, ordered_partition_results,
};

pub(crate) fn singular_description(
    result: DescribeStreamsGroupResult,
    operation_id: &OperationId,
) -> Result<(u64, AdminStreamsGroupDescription), AdapterError> {
    let throttle = throttle_ms(result.throttle_time(), operation_id, "describe-one")?;
    let description = summarize_description(result.description(), operation_id)?;
    Ok((throttle, description))
}

pub(crate) fn plural_descriptions(
    result: DescribeStreamsGroupsResult,
    requested: &[String],
    operation_id: &OperationId,
) -> Result<(u64, Vec<AdminStreamsGroupDescription>), AdapterError> {
    let throttle = throttle_ms(result.throttle_time(), operation_id, "describe-many")?;
    let groups = ordered_group_results(
        result.into_groups().into_entries(),
        requested,
        operation_id,
        "Streams-group description",
    )?;
    let descriptions = groups
        .into_iter()
        .map(|group| match group.result {
            ResourceResult::Success(value) if value.group_id() == group.group_id => {
                summarize_description(&value, operation_id)
            }
            ResourceResult::Success(_) => Err(invalid(
                operation_id,
                "description returned a mismatched inner group identity",
            )),
            ResourceResult::Failure(code) => Err(invalid(
                operation_id,
                &format!("description failed for {} with {code}", group.group_id),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((throttle, descriptions))
}

pub(crate) fn singular_offset(
    result: ListStreamsGroupOffsetsResult,
    group_id: &str,
    topic: &str,
    partition: i32,
    operation_id: &OperationId,
    label: &str,
) -> Result<(u64, AdminStreamsGroupOffset), AdapterError> {
    let throttle = throttle_ms(result.throttle_time(), operation_id, label)?;
    let offset = selected_offset(result, group_id, topic, partition, operation_id)?;
    Ok((throttle, offset))
}

pub(crate) fn plural_offsets(
    result: ListStreamsGroupsOffsetsResult,
    requested: &[String],
    topic: &str,
    partition: i32,
    operation_id: &OperationId,
) -> Result<(u64, Vec<AdminStreamsGroupOffset>), AdapterError> {
    let throttle = throttle_ms(result.throttle_time(), operation_id, "list-many")?;
    let groups = ordered_group_results(
        result.into_groups().into_entries(),
        requested,
        operation_id,
        "Streams-group offset listing",
    )?;
    let offsets = groups
        .into_iter()
        .map(|group| match group.result {
            ResourceResult::Success(value) => {
                selected_offset(value, &group.group_id, topic, partition, operation_id)
            }
            ResourceResult::Failure(code) => Err(invalid(
                operation_id,
                &format!("offset listing failed for {} with {code}", group.group_id),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((throttle, offsets))
}

pub(crate) fn partition_success<V>(
    entries: Vec<(TopicPartition, Result<V, KafkaError>)>,
    topic: &str,
    partition: i32,
    operation_id: &OperationId,
    label: &str,
) -> Result<(), AdapterError> {
    let requested = [(topic.to_owned(), partition)];
    let mut outcomes = ordered_partition_results(entries, &requested, operation_id, label)?;
    let outcome = outcomes
        .pop()
        .ok_or_else(|| invalid(operation_id, "partition outcome disappeared"))?;
    match outcome.result {
        ResourceResult::Success(_) => Ok(()),
        ResourceResult::Failure(code) => Err(invalid(
            operation_id,
            &format!("{label} failed with {code}"),
        )),
    }
}

pub(crate) fn groups_success(
    entries: Vec<(String, Result<(), KafkaError>)>,
    requested: &[String],
    operation_id: &OperationId,
) -> Result<(), AdapterError> {
    let outcomes =
        ordered_group_results(entries, requested, operation_id, "Streams-group deletion")?;
    for outcome in outcomes {
        if let ResourceResult::Failure(code) = outcome.result {
            return Err(invalid(
                operation_id,
                &format!("deletion failed for {} with {code}", outcome.group_id),
            ));
        }
    }
    Ok(())
}

pub(crate) fn throttle_ms(
    value: Duration,
    operation_id: &OperationId,
    label: &str,
) -> Result<u64, AdapterError> {
    u64::try_from(value.as_millis()).map_err(|_| {
        invalid(
            operation_id,
            &format!("{label} throttle was unrepresentable"),
        )
    })
}

fn summarize_description(
    value: &StreamsGroupDescription,
    operation_id: &OperationId,
) -> Result<AdminStreamsGroupDescription, AdapterError> {
    let subtopologies = value
        .topology()
        .and_then(|topology| topology.subtopologies());
    let topology_source_topics = subtopologies
        .into_iter()
        .flatten()
        .flat_map(|subtopology| subtopology.source_topics().iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let topology_subtopology_count = optional_count(subtopologies, operation_id, "topology")?;
    let topology_description_subtopology_count = value
        .topology_description()
        .map(|description| {
            count(
                description.subtopologies().len(),
                operation_id,
                "full topology",
            )
        })
        .transpose()?;
    Ok(AdminStreamsGroupDescription {
        group_id: value.group_id().to_owned(),
        state: value.state().to_owned(),
        group_epoch: value.group_epoch(),
        assignment_epoch: value.assignment_epoch(),
        topology_epoch: value.topology().map(|topology| topology.epoch()),
        topology_source_topics,
        topology_subtopology_count,
        member_count: count(value.members().len(), operation_id, "members")?,
        authorized_operations: value.authorized_operations(),
        topology_description_status: value
            .topology_description_status()
            .map(|status| status.as_raw()),
        topology_description_subtopology_count,
    })
}

fn selected_offset(
    result: ListStreamsGroupOffsetsResult,
    group_id: &str,
    topic: &str,
    partition: i32,
    operation_id: &OperationId,
) -> Result<AdminStreamsGroupOffset, AdapterError> {
    let requested = [(topic.to_owned(), partition)];
    let mut outcomes = ordered_partition_results(
        result.into_offsets().into_entries(),
        &requested,
        operation_id,
        "Streams-group offset",
    )?;
    let outcome = outcomes
        .pop()
        .ok_or_else(|| invalid(operation_id, "selected offset disappeared"))?;
    match outcome.result {
        ResourceResult::Success(value) => Ok(offset(group_id, outcome.topic, partition, value)),
        ResourceResult::Failure(code) => Err(invalid(
            operation_id,
            &format!("selected offset failed with {code}"),
        )),
    }
}

fn offset(
    group_id: &str,
    topic: String,
    partition: i32,
    value: ConsumerGroupOffset,
) -> AdminStreamsGroupOffset {
    let (committed_offset, leader_epoch, metadata) = value.into_parts();
    AdminStreamsGroupOffset {
        group_id: group_id.to_owned(),
        topic,
        partition,
        committed_offset,
        leader_epoch,
        metadata,
    }
}

fn optional_count<T>(
    values: Option<&[T]>,
    operation_id: &OperationId,
    label: &str,
) -> Result<Option<u32>, AdapterError> {
    values
        .map(|values| count(values.len(), operation_id, label))
        .transpose()
}

fn count(value: usize, operation_id: &OperationId, label: &str) -> Result<u32, AdapterError> {
    u32::try_from(value).map_err(|_| invalid(operation_id, &format!("{label} count overflowed")))
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
