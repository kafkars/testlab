//! Direct-consumer protocol completions resolve relative positions before acceptance.

use std::collections::BTreeMap;
use std::time::Instant;

use testlab_schema::{AssignedConsumerControl, AssignedPartitionPosition, AssignedStartPosition};

use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{
    AssignedConsumer, Client, ErrorKind, KafkaError, ListOffsetsQuery, OffsetSpec, RetryAdvice,
    StartPosition, TopicPartition,
};

pub(crate) fn apply_control(
    consumer: &mut AssignedConsumer,
    control: &AssignedConsumerControl,
    deadline: Instant,
) -> Result<(), KafkaError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    match control {
        AssignedConsumerControl::Replace { partitions } => {
            consumer.try_replace_assignment(partitions.iter().map(positioned_partition), remaining)
        }
        AssignedConsumerControl::Add { partitions } => {
            consumer.try_add_assignments(partitions.iter().map(positioned_partition), remaining)
        }
        AssignedConsumerControl::Remove { partitions } => consumer.try_remove_assignments(
            partitions
                .iter()
                .map(|entry| TopicPartition::new(entry.topic.clone(), entry.partition)),
        ),
        AssignedConsumerControl::Seek {
            partition,
            position,
        } => consumer.try_seek(
            &TopicPartition::new(partition.topic.clone(), partition.partition),
            start_position(*position),
            remaining,
        ),
        AssignedConsumerControl::Pause { partition } => consumer.try_pause(&TopicPartition::new(
            partition.topic.clone(),
            partition.partition,
        )),
        AssignedConsumerControl::Resume { partition } => consumer.try_resume(
            &TopicPartition::new(partition.topic.clone(), partition.partition),
            remaining,
        ),
    }
}

pub(crate) fn resolve_control(
    client: &Client,
    control: &AssignedConsumerControl,
    deadline: Instant,
) -> Result<AssignedConsumerControl, KafkaError> {
    match control {
        AssignedConsumerControl::Replace { partitions } => Ok(AssignedConsumerControl::Replace {
            partitions: resolve_partitions(client, partitions, deadline)?,
        }),
        AssignedConsumerControl::Add { partitions } => Ok(AssignedConsumerControl::Add {
            partitions: resolve_partitions(client, partitions, deadline)?,
        }),
        AssignedConsumerControl::Seek {
            partition,
            position,
        } => {
            let resolved = resolve_partitions(
                client,
                &[AssignedPartitionPosition {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                    position: *position,
                }],
                deadline,
            )?;
            let entry = resolved
                .into_iter()
                .next()
                .ok_or_else(position_resolution_invariant)?;
            Ok(AssignedConsumerControl::Seek {
                partition: partition.clone(),
                position: entry.position,
            })
        }
        control => Ok(control.clone()),
    }
}

fn resolve_partitions(
    client: &Client,
    partitions: &[AssignedPartitionPosition],
    deadline: Instant,
) -> Result<Vec<AssignedPartitionPosition>, KafkaError> {
    let queries = partitions
        .iter()
        .filter_map(|entry| {
            offset_spec(entry.position)
                .map(|spec| ListOffsetsQuery::new(entry.topic.clone(), entry.partition, spec))
        })
        .collect::<Vec<_>>();
    if queries.is_empty() {
        return Ok(partitions.to_vec());
    }
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .list_offsets(queries.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        |error| error.retry_advice() == RetryAdvice::RetrySafe,
    )?;
    let mut offsets = BTreeMap::new();
    for (partition, result) in result.into_offsets().into_entries() {
        let value = result?
            .offset()
            .ok_or_else(|| KafkaError::new(ErrorKind::Broker, "ListOffsets returned no offset"))?;
        offsets.insert((partition.topic().to_owned(), partition.partition()), value);
    }
    partitions
        .iter()
        .map(|entry| {
            let position = match entry.position {
                AssignedStartPosition::Beginning | AssignedStartPosition::End => {
                    let offset = offsets
                        .get(&(entry.topic.clone(), entry.partition))
                        .copied()
                        .ok_or_else(position_resolution_invariant)?;
                    AssignedStartPosition::Offset { offset }
                }
                position @ AssignedStartPosition::Offset { .. } => position,
            };
            Ok(AssignedPartitionPosition {
                topic: entry.topic.clone(),
                partition: entry.partition,
                position,
            })
        })
        .collect()
}

pub(crate) const fn offset_spec(position: AssignedStartPosition) -> Option<OffsetSpec> {
    match position {
        AssignedStartPosition::Beginning => Some(OffsetSpec::earliest()),
        AssignedStartPosition::End => Some(OffsetSpec::latest()),
        AssignedStartPosition::Offset { .. } => None,
    }
}

pub(crate) fn positioned_partition(entry: &AssignedPartitionPosition) -> TopicPartition {
    TopicPartition::new(entry.topic.clone(), entry.partition)
        .start_at(start_position(entry.position))
}

pub(crate) const fn start_position(position: AssignedStartPosition) -> StartPosition {
    match position {
        AssignedStartPosition::Beginning => StartPosition::Beginning,
        AssignedStartPosition::End => StartPosition::End,
        AssignedStartPosition::Offset { offset } => StartPosition::Offset(offset),
    }
}

fn position_resolution_invariant() -> KafkaError {
    KafkaError::new(
        ErrorKind::Internal,
        "assigned-consumer position resolution lost its partition",
    )
}
