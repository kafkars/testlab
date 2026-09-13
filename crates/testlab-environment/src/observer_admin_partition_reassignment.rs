//! Independent metadata proves exact converged replica assignments after mutation.

use std::collections::BTreeSet;
use std::thread;
use std::time::Duration;

use rdkafka::metadata::Metadata;
use testlab_schema::{
    BrokerPartitionAssignment, BrokerPartitionAssignmentsState, BrokerStateObservation,
    BrokerTopicPartitionSet,
};

use crate::observer::remaining;
use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_partition_reassignment_target::{
    ExpectedPartitionAssignment, PartitionAssignmentsTarget,
};
use crate::observer_error::ObserverError;

const POLL_SLICE: Duration = Duration::from_millis(50);

pub(super) fn capture(
    request: AdminObserverRequest<'_>,
    target: &PartitionAssignmentsTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let admin = client(request, "partition-reassignments")?;
    loop {
        let metadata = admin
            .inner()
            .fetch_metadata(None, remaining(request.deadline)?)?;
        let state = normalize(request, target, &metadata)?;
        if converged(&state, target) {
            return Ok(BrokerStateObservation::PartitionAssignments(state));
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

fn normalize(
    request: AdminObserverRequest<'_>,
    target: &PartitionAssignmentsTarget,
    metadata: &Metadata,
) -> Result<BrokerPartitionAssignmentsState, ObserverError> {
    let brokers = metadata
        .brokers()
        .iter()
        .map(rdkafka::metadata::MetadataBroker::id)
        .collect::<BTreeSet<_>>();
    if brokers.len() != usize::from(request.cluster_size)
        || brokers.iter().any(|broker| *broker < 0)
    {
        return Err(invalid("metadata exposed an invalid broker set"));
    }
    let assignments = target
        .assignments
        .iter()
        .map(|expected| assignment(metadata, &brokers, expected))
        .collect::<Result<Vec<_>, _>>()?;
    let topic_partitions = target
        .assignments
        .iter()
        .map(|assignment| assignment.topic.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|topic| complete_topic_partitions(metadata, topic))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BrokerPartitionAssignmentsState {
        observation: request.first_observation,
        operation_id: target.operation_id.clone(),
        topic_partitions,
        assignments,
    })
}

fn complete_topic_partitions(
    metadata: &Metadata,
    expected_topic: &str,
) -> Result<BrokerTopicPartitionSet, ObserverError> {
    let mut topics = metadata
        .topics()
        .iter()
        .filter(|topic| topic.name() == expected_topic);
    let topic = topics
        .next()
        .ok_or_else(|| invalid(format!("metadata omitted topic {expected_topic}")))?;
    if topics.next().is_some() || topic.error().is_some() {
        return Err(invalid(format!(
            "metadata repeated or rejected topic {expected_topic}"
        )));
    }
    let mut partitions = topic
        .partitions()
        .iter()
        .map(|partition| {
            if partition.id() < 0 || partition.error().is_some() {
                return Err(invalid(format!(
                    "metadata returned malformed partition for topic {expected_topic}"
                )));
            }
            Ok(partition.id())
        })
        .collect::<Result<Vec<_>, _>>()?;
    partitions.sort_unstable();
    if partitions.is_empty() || partitions.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid(format!(
            "metadata returned an invalid partition set for topic {expected_topic}"
        )));
    }
    Ok(BrokerTopicPartitionSet {
        topic: expected_topic.to_owned(),
        partitions,
    })
}

fn assignment(
    metadata: &Metadata,
    brokers: &BTreeSet<i32>,
    expected: &ExpectedPartitionAssignment,
) -> Result<BrokerPartitionAssignment, ObserverError> {
    let mut topics = metadata
        .topics()
        .iter()
        .filter(|topic| topic.name() == expected.topic);
    let topic = topics
        .next()
        .ok_or_else(|| invalid(format!("metadata omitted topic {}", expected.topic)))?;
    if topics.next().is_some() || topic.error().is_some() {
        return Err(invalid(format!(
            "metadata repeated or rejected topic {}",
            expected.topic
        )));
    }
    let mut partitions = topic
        .partitions()
        .iter()
        .filter(|partition| partition.id() == expected.partition);
    let partition = partitions.next().ok_or_else(|| {
        invalid(format!(
            "metadata omitted partition {}-{}",
            expected.topic, expected.partition
        ))
    })?;
    if partitions.next().is_some() || partition.error().is_some() {
        return Err(invalid(format!(
            "metadata repeated or rejected partition {}-{}",
            expected.topic, expected.partition
        )));
    }
    let replicas = partition.replicas().to_vec();
    let mut in_sync_replicas = partition.isr().to_vec();
    in_sync_replicas.sort_unstable();
    if partition.leader() < 0
        || !valid_ids(&replicas, brokers)
        || !valid_ids(&in_sync_replicas, brokers)
    {
        return Err(invalid(format!(
            "metadata returned malformed assignment for {}-{}",
            expected.topic, expected.partition
        )));
    }
    Ok(BrokerPartitionAssignment {
        topic: expected.topic.clone(),
        partition: expected.partition,
        leader_id: partition.leader(),
        replicas,
        in_sync_replicas,
    })
}

fn valid_ids(values: &[i32], brokers: &BTreeSet<i32>) -> bool {
    !values.is_empty()
        && values.iter().all(|value| brokers.contains(value))
        && values.iter().copied().collect::<BTreeSet<_>>().len() == values.len()
}

fn converged(state: &BrokerPartitionAssignmentsState, target: &PartitionAssignmentsTarget) -> bool {
    state
        .assignments
        .iter()
        .zip(&target.assignments)
        .all(|(actual, expected)| {
            let mut expected_isr = expected.replicas.clone();
            expected_isr.sort_unstable();
            actual.topic == expected.topic
                && actual.partition == expected.partition
                && actual.replicas == expected.replicas
                && actual.in_sync_replicas == expected_isr
                && actual.replicas.contains(&actual.leader_id)
        })
}

fn invalid(detail: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(detail.into())
}
