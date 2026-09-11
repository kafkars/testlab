//! Replica log-directory verification joins caller order to Kafka CLI placement state.

use testlab_schema::{
    AdminReplicaLogDirDescription, AdminReplicaLogDirsDescription, BrokerLogDirsBrokerState,
    BrokerLogDirsState, DescribeReplicaLogDirsAction, ReplicaLogDirLocationState, ScenarioAction,
    Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_features::Indexed;
use crate::support::violation;

pub(crate) fn verify_replica_log_dirs_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    let ScenarioAction::DescribeReplicaLogDirs(action) = action else {
        return false;
    };
    let public = index
        .admin_features
        .replica_log_dirs_described
        .get(&action.operation_id);
    let independent = index
        .admin_features
        .log_dirs_observed
        .get(&action.operation_id);
    if exact_match(public, independent, command_window, action) {
        return true;
    }
    violations.push(violation(
        "ADMIN-055",
        format!(
            "admin operation {} expected every discovered broker in descending caller order and {} current replica placement(s) exactly matching one immediate Kafka CLI snapshot",
            action.operation_id, action.expected_replica_count
        ),
        Some(action.operation_id.clone()),
        evidence(public, independent),
    ));
    true
}

fn exact_match(
    public: Option<&Vec<Indexed<AdminReplicaLogDirsDescription>>>,
    independent: Option<&Vec<Indexed<BrokerLogDirsState>>>,
    window: Option<AdminCommandWindow>,
    action: &DescribeReplicaLogDirsAction,
) -> bool {
    let (Some([public]), Some([independent])) =
        (public.map(Vec::as_slice), independent.map(Vec::as_slice))
    else {
        return false;
    };
    public.value.topic == action.topic
        && public.value.partition == action.partition
        && independent.value.topic == action.topic
        && independent.value.partition == action.partition
        && public.value.throttle_time_ms <= action.timeout_ms
        && canonical_public(&public.value, action)
        && canonical_independent(&independent.value, action)
        && same_placements(&public.value.replicas, &independent.value.brokers)
        && current_count(&public.value.replicas) == action.expected_replica_count
        && public_after_command(window, public.history_sequence)
        && immediate_after_public(
            window,
            public.history_sequence,
            independent.history_sequence,
        )
}

fn canonical_public(
    value: &AdminReplicaLogDirsDescription,
    action: &DescribeReplicaLogDirsAction,
) -> bool {
    !value.replicas.is_empty()
        && value.replicas.iter().all(|entry| {
            entry.replica.topic == action.topic
                && entry.replica.partition == action.partition
                && entry.replica.broker_id >= 0
                && entry
                    .current
                    .iter()
                    .chain(entry.future.iter())
                    .all(|location| !location.path.is_empty())
                && entry.future.is_none()
        })
        && value
            .replicas
            .windows(2)
            .all(|pair| pair[0].replica.broker_id > pair[1].replica.broker_id)
}

fn canonical_independent(
    value: &BrokerLogDirsState,
    action: &DescribeReplicaLogDirsAction,
) -> bool {
    !value.brokers.is_empty()
        && value.brokers.iter().all(|broker| {
            broker.broker_id >= 0
                && !broker.log_dirs.is_empty()
                && broker.log_dirs.iter().all(|directory| {
                    !directory.path.is_empty()
                        && directory.replicas.iter().all(|replica| {
                            replica.topic == action.topic
                                && replica.partition == action.partition
                                && replica.size_bytes >= 0
                                && !replica.is_future
                        })
                        && directory.replicas.windows(2).all(|pair| pair[0] < pair[1])
                })
                && broker
                    .log_dirs
                    .windows(2)
                    .all(|pair| pair[0].path.as_bytes() < pair[1].path.as_bytes())
        })
        && value
            .brokers
            .windows(2)
            .all(|pair| pair[0].broker_id < pair[1].broker_id)
}

fn same_placements(
    public: &[AdminReplicaLogDirDescription],
    independent: &[BrokerLogDirsBrokerState],
) -> bool {
    public.len() == independent.len()
        && public
            .iter()
            .rev()
            .zip(independent)
            .all(|(public, independent)| {
                public.replica.broker_id == independent.broker_id
                    && location_matches(public.current.as_ref(), independent, false)
                    && location_matches(public.future.as_ref(), independent, true)
            })
}

fn location_matches(
    public: Option<&ReplicaLogDirLocationState>,
    independent: &BrokerLogDirsBrokerState,
    future: bool,
) -> bool {
    let mut observed = independent.log_dirs.iter().flat_map(|directory| {
        directory
            .replicas
            .iter()
            .filter(move |replica| replica.is_future == future)
            .map(move |replica| (directory.path.as_str(), replica))
    });
    let first = observed.next();
    if observed.next().is_some() {
        return false;
    }
    match (public, first) {
        (None, None) => true,
        (Some(public), Some((path, replica))) => {
            public.path == path && public.offset_lag == replica.offset_lag
        }
        _ => false,
    }
}

fn current_count(replicas: &[AdminReplicaLogDirDescription]) -> usize {
    replicas
        .iter()
        .filter(|entry| entry.current.is_some())
        .count()
}

fn evidence(
    public: Option<&Vec<Indexed<AdminReplicaLogDirsDescription>>>,
    independent: Option<&Vec<Indexed<BrokerLogDirsState>>>,
) -> Vec<String> {
    public
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.value.observation)),
        )
        .collect()
}
