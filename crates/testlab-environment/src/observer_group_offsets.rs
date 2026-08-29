//! Plural committed-offset observation retains complete same-query snapshots.

use std::collections::BTreeMap;
use std::thread;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use testlab_schema::{BrokerConsumerGroupOffset, BrokerStateObservation, OperationId, RunId};

use crate::observer::remaining;
use crate::observer_admin::AdminObserverRequest;
use crate::observer_admin_target::{
    GroupOffsetTarget, GroupOffsetsSelectionTarget, GroupOffsetsTarget, GroupsOffsetsTarget,
    ordinal,
};
use crate::observer_error::ObserverError;
use crate::security::ClientSecurity;

const POLL_SLICE: Duration = Duration::from_millis(50);

pub(super) fn capture_group(
    request: AdminObserverRequest<'_>,
    target: &GroupOffsetsTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let group = GroupOffsetsSelectionTarget {
        group_id: target.group_id.clone(),
        offsets: target.offsets.clone(),
    };
    let consumer = consumer(
        request.endpoint,
        request.run_id,
        request.first_observation,
        &group.group_id,
        request.security,
    )?;
    loop {
        let observed = query(
            &consumer,
            request.deadline,
            request.first_observation,
            &target.operation_id,
            &group,
        )?;
        if !target.poll_expected || snapshot_matches(&observed, &target.offsets) {
            return Ok(observed);
        }
        let wait = request
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        if wait.is_zero() {
            return Err(ObserverError::Deadline);
        }
        thread::sleep(POLL_SLICE.min(wait));
    }
}

pub(super) fn capture_groups(
    request: AdminObserverRequest<'_>,
    target: &GroupsOffsetsTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let mut observed =
        Vec::with_capacity(target.groups.iter().map(|group| group.offsets.len()).sum());
    for group in &target.groups {
        let first = ordinal(request.first_observation, observed.len())?;
        let consumer = consumer(
            request.endpoint,
            request.run_id,
            first,
            &group.group_id,
            request.security,
        )?;
        observed.extend(query(
            &consumer,
            request.deadline,
            first,
            &target.operation_id,
            group,
        )?);
    }
    Ok(observed)
}

fn query(
    consumer: &BaseConsumer,
    deadline: std::time::Instant,
    first_observation: u64,
    operation_id: &OperationId,
    group: &GroupOffsetsSelectionTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let mut partitions = TopicPartitionList::new();
    for target in &group.offsets {
        partitions.add_partition(&target.topic, target.partition);
    }
    let offsets = consumer.committed_offsets(partitions, remaining(deadline)?)?;
    normalize_response(first_observation, operation_id, group, &offsets)
}

fn consumer(
    endpoint: &str,
    run_id: &RunId,
    observation: u64,
    group_id: &str,
    security: &ClientSecurity,
) -> Result<BaseConsumer, ObserverError> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", endpoint)
        .set(
            "client.id",
            format!("testlab-state-observer-{run_id}-{observation}"),
        )
        .set("group.id", group_id)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    security.configure(&mut config);
    config.create().map_err(ObserverError::Kafka)
}

pub(super) fn normalize_response(
    first_observation: u64,
    operation_id: &OperationId,
    group: &GroupOffsetsSelectionTarget,
    offsets: &TopicPartitionList,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let mut by_partition = BTreeMap::new();
    for element in offsets.elements() {
        element.error()?;
        let key = (element.topic().to_owned(), element.partition());
        let offset = normalize_offset(&group.group_id, &key.0, key.1, element.offset())?;
        if by_partition.insert(key.clone(), offset).is_some() {
            return Err(invalid(
                &group.group_id,
                format!("returned duplicate partition {}:{}", key.0, key.1),
            ));
        }
    }
    if by_partition.len() != group.offsets.len() {
        return Err(invalid(
            &group.group_id,
            "returned a missing or extra partition",
        ));
    }
    group
        .offsets
        .iter()
        .enumerate()
        .map(|(index, target)| {
            let offset = by_partition
                .remove(&(target.topic.clone(), target.partition))
                .ok_or_else(|| {
                    invalid(
                        &group.group_id,
                        format!(
                            "did not return selected partition {}:{}",
                            target.topic, target.partition
                        ),
                    )
                })?;
            Ok(BrokerStateObservation::ConsumerGroupOffset(
                BrokerConsumerGroupOffset {
                    observation: ordinal(first_observation, index)?,
                    operation_id: operation_id.clone(),
                    group_id: group.group_id.clone(),
                    topic: target.topic.clone(),
                    partition: target.partition,
                    offset,
                },
            ))
        })
        .collect()
}

fn normalize_offset(
    group_id: &str,
    topic: &str,
    partition: i32,
    offset: Offset,
) -> Result<Option<i64>, ObserverError> {
    match offset {
        Offset::Invalid => Ok(None),
        Offset::Offset(offset) if offset >= 0 => Ok(Some(offset)),
        unsupported => Err(invalid(
            group_id,
            format!("offset for {topic}:{partition} was unsupported {unsupported:?}"),
        )),
    }
}

pub(super) fn snapshot_matches(
    observed: &[BrokerStateObservation],
    targets: &[GroupOffsetTarget],
) -> bool {
    observed.len() == targets.len()
        && observed.iter().zip(targets).all(|(observation, target)| {
            let BrokerStateObservation::ConsumerGroupOffset(observation) = observation else {
                return false;
            };
            observation.topic == target.topic
                && observation.partition == target.partition
                && observation.offset == target.expected_offset
        })
}

fn invalid(group_id: &str, detail: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("consumer group {group_id} offsets {detail}"))
}
