//! Independent metadata proves preferred and unclean leader-election postconditions.

use std::collections::BTreeSet;
use std::thread;
use std::time::Duration;

use rdkafka::metadata::Metadata;
use testlab_schema::{
    AdminLeaderElectionType, BrokerLeaderElectionPartition, BrokerLeaderElectionState,
    BrokerStateObservation,
};

use crate::observer::remaining;
use crate::observer_admin::{AdminObserverRequest, client};
use crate::observer_admin_leader_election_target::LeaderElectionTarget;
use crate::observer_error::ObserverError;

const POLL_SLICE: Duration = Duration::from_millis(50);

pub(super) fn capture(
    request: AdminObserverRequest<'_>,
    target: &LeaderElectionTarget,
) -> Result<BrokerStateObservation, ObserverError> {
    let admin = client(request, "leader-election")?;
    loop {
        let metadata = admin
            .inner()
            .fetch_metadata(None, remaining(request.deadline)?)?;
        let state = normalize(request, target, &metadata)?;
        if converged(&state) {
            return Ok(BrokerStateObservation::LeaderElection(state));
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
    target: &LeaderElectionTarget,
    metadata: &Metadata,
) -> Result<BrokerLeaderElectionState, ObserverError> {
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
    let partitions = target
        .partitions
        .iter()
        .map(|expected| partition(metadata, &brokers, expected))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BrokerLeaderElectionState {
        observation: request.first_observation,
        operation_id: target.operation_id.clone(),
        election_type: target.election_type,
        partitions,
    })
}

fn partition(
    metadata: &Metadata,
    brokers: &BTreeSet<i32>,
    expected: &testlab_schema::LeaderElectionSelection,
) -> Result<BrokerLeaderElectionPartition, ObserverError> {
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
            "metadata returned malformed election state for {}-{}",
            expected.topic, expected.partition
        )));
    }
    Ok(BrokerLeaderElectionPartition {
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

fn converged(state: &BrokerLeaderElectionState) -> bool {
    state.partitions.iter().all(|partition| {
        let leader_valid = partition.replicas.contains(&partition.leader_id)
            && partition.in_sync_replicas.contains(&partition.leader_id);
        match state.election_type {
            AdminLeaderElectionType::Preferred => {
                let mut replicas = partition.replicas.clone();
                replicas.sort_unstable();
                leader_valid
                    && partition.replicas.first() == Some(&partition.leader_id)
                    && partition.in_sync_replicas == replicas
            }
            AdminLeaderElectionType::Unclean => leader_valid,
        }
    })
}

fn invalid(detail: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("leader-election observation: {}", detail.into()))
}
