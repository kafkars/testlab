//! Metadata observation normalizes topic topology and exact cluster identity.

use std::collections::BTreeSet;
use std::thread;
use std::time::Duration;

use rdkafka::metadata::Metadata;
use testlab_schema::{BrokerClusterState, BrokerStateObservation, BrokerTopicState, OperationId};

use crate::observer::remaining;
use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_target::{BrokerUnregistrationTarget, ListTarget, TopicTarget, ordinal};
use crate::observer_error::ObserverError;

const POLL_SLICE: Duration = Duration::from_millis(50);

pub(super) fn capture_topic(
    request: AdminObserverRequest<'_>,
    target: &TopicTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let admin = client(request, "topic")?;
    loop {
        let timeout = remaining(request.deadline)?;
        let metadata = match admin.inner().fetch_metadata(None, timeout) {
            Ok(metadata) => metadata,
            Err(error) => return Err(ObserverError::Kafka(error)),
        };
        let observed = normalize_topic(
            request.first_observation,
            &target.operation_id,
            &target.topic,
            &metadata,
        )?;
        if !target.poll_expected || topic_matches(&observed, target) {
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

pub(super) fn capture_topics(
    request: AdminObserverRequest<'_>,
    target: &ListTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let admin = client(request, "topics")?;
    let metadata = admin
        .inner()
        .fetch_metadata(None, remaining(request.deadline)?)?;
    normalize_topics(request.first_observation, target, &metadata)
}

pub(super) fn capture_absent_topics(
    request: AdminObserverRequest<'_>,
    target: &ListTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    let admin = client(request, "topic-deletions")?;
    loop {
        let metadata = admin
            .inner()
            .fetch_metadata(None, remaining(request.deadline)?)?;
        let observed = normalize_topics(request.first_observation, target, &metadata)?;
        if observed.iter().all(topic_absent) {
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

fn normalize_topics(
    first_observation: u64,
    target: &ListTarget,
    metadata: &Metadata,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    target
        .names
        .iter()
        .enumerate()
        .map(|(index, topic)| {
            normalize_topic(
                ordinal(first_observation, index)?,
                &target.operation_id,
                topic,
                metadata,
            )
        })
        .collect()
}

pub(super) fn capture_cluster(
    request: AdminObserverRequest<'_>,
    operation_id: &OperationId,
) -> Result<BrokerStateObservation, ObserverError> {
    capture_cluster_when(request, operation_id, "cluster", |broker_ids| {
        broker_ids.len() == usize::from(request.cluster_size)
    })
}

pub(super) fn capture_broker_unregistration(
    request: AdminObserverRequest<'_>,
    target: &BrokerUnregistrationTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    capture_cluster_when(
        request,
        &target.operation_id,
        "broker-unregistration",
        |broker_ids| broker_ids == target.expected_remaining_broker_ids.as_slice(),
    )
}

fn capture_cluster_when(
    request: AdminObserverRequest<'_>,
    operation_id: &OperationId,
    purpose: &str,
    ready: impl Fn(&[i32]) -> bool,
) -> Result<BrokerStateObservation, ObserverError> {
    let admin = client(request, purpose)?;
    loop {
        let metadata = admin
            .inner()
            .fetch_metadata(None, remaining(request.deadline)?)?;
        let cluster_id = admin
            .inner()
            .fetch_cluster_id(remaining(request.deadline)?)
            .ok_or_else(|| invalid("cluster ID was null"))?;
        let observation = normalize_cluster(
            request.first_observation,
            operation_id,
            cluster_id,
            &metadata,
        )?;
        let BrokerStateObservation::Cluster(cluster) = &observation else {
            unreachable!("cluster normalization returned another observation kind");
        };
        if ready(&cluster.broker_ids) {
            return Ok(observation);
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

fn normalize_cluster(
    observation: u64,
    operation_id: &OperationId,
    cluster_id: String,
    metadata: &Metadata,
) -> Result<BrokerStateObservation, ObserverError> {
    if cluster_id.is_empty() {
        return Err(invalid("cluster ID was empty"));
    }
    let mut broker_ids = metadata
        .brokers()
        .iter()
        .map(rdkafka::metadata::MetadataBroker::id)
        .collect::<Vec<_>>();
    broker_ids.sort_unstable();
    if broker_ids.iter().any(|broker| *broker < 0)
        || broker_ids.windows(2).any(|pair| pair[0] == pair[1])
    {
        return Err(invalid(
            "metadata contained invalid or duplicate broker IDs",
        ));
    }
    Ok(BrokerStateObservation::Cluster(BrokerClusterState {
        observation,
        operation_id: operation_id.clone(),
        cluster_id: Some(cluster_id),
        broker_ids,
    }))
}

fn normalize_topic(
    observation: u64,
    operation_id: &OperationId,
    name: &str,
    metadata: &Metadata,
) -> Result<BrokerStateObservation, ObserverError> {
    let mut matching = metadata
        .topics()
        .iter()
        .filter(|topic| topic.name() == name);
    let Some(topic) = matching.next() else {
        return Ok(BrokerStateObservation::Topic(BrokerTopicState {
            observation,
            operation_id: operation_id.clone(),
            topic: name.to_owned(),
            exists: false,
            partitions: Vec::new(),
        }));
    };
    if matching.next().is_some() {
        return Err(invalid(format!("metadata repeated topic {name}")));
    }
    if let Some(error) = topic.error() {
        return Err(invalid(format!("topic {name} returned error {error:?}")));
    }
    let mut partitions = BTreeSet::new();
    for partition in topic.partitions() {
        if let Some(error) = partition.error() {
            return Err(invalid(format!(
                "topic {name} partition {} returned error {error:?}",
                partition.id()
            )));
        }
        if partition.id() < 0 || !partitions.insert(partition.id()) {
            return Err(invalid(format!(
                "topic {name} contained an invalid or duplicate partition"
            )));
        }
        validate_replicas(name, partition.id(), partition.replicas())?;
    }
    Ok(BrokerStateObservation::Topic(BrokerTopicState {
        observation,
        operation_id: operation_id.clone(),
        topic: name.to_owned(),
        exists: true,
        partitions: partitions.into_iter().collect(),
    }))
}

fn validate_replicas(topic: &str, partition: i32, replicas: &[i32]) -> Result<(), ObserverError> {
    let unique = replicas.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != replicas.len() || replicas.iter().any(|replica| *replica < 0) {
        return Err(invalid(format!(
            "topic {topic} partition {partition} contained invalid replica IDs"
        )));
    }
    Ok(())
}

fn topic_matches(observation: &BrokerStateObservation, target: &TopicTarget) -> bool {
    let BrokerStateObservation::Topic(observed) = observation else {
        return false;
    };
    observed.exists == target.expected_exists
        && target
            .expected_partitions
            .as_ref()
            .is_none_or(|partitions| observed.partitions == *partitions)
}

fn topic_absent(observation: &BrokerStateObservation) -> bool {
    matches!(
        observation,
        BrokerStateObservation::Topic(topic) if !topic.exists && topic.partitions.is_empty()
    )
}

fn invalid(detail: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidBrokerState(detail.to_string())
}
