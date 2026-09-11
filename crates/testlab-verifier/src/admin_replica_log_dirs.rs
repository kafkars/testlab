//! Replica log-directory verification joins caller order to Kafka CLI placement state.

use testlab_schema::{
    AdminReplicaLogDirDescription, AdminReplicaLogDirsAlteration, AdminReplicaLogDirsDescription,
    AlterReplicaLogDirsAction, BrokerLogDirsBrokerState, BrokerLogDirsState,
    DescribeReplicaLogDirsAction, ReplicaLogDirAssignmentSpec, ReplicaLogDirLocationState,
    ScenarioAction, Violation,
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
    match action {
        ScenarioAction::AlterReplicaLogDirs(action) => {
            verify_alteration(action, command_window, index, violations);
        }
        ScenarioAction::DescribeReplicaLogDirs(action) => {
            verify_description(action, command_window, index, violations);
        }
        _ => return false,
    }
    true
}

fn verify_description(
    action: &DescribeReplicaLogDirsAction,
    command_window: Option<AdminCommandWindow>,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let public = index
        .admin_features
        .replica_log_dirs_described
        .get(&action.operation_id);
    let independent = index
        .admin_features
        .log_dirs_observed
        .get(&action.operation_id);
    if exact_match(public, independent, command_window, action) {
        return;
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
}

fn verify_alteration(
    action: &AlterReplicaLogDirsAction,
    command_window: Option<AdminCommandWindow>,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let public = index
        .admin_replica_log_dirs
        .altered
        .get(&action.operation_id);
    let independent = index
        .admin_features
        .log_dirs_observed
        .get(&action.operation_id);
    if exact_alteration(public, independent, command_window, action) {
        return;
    }
    violations.push(violation(
        "ADMIN-070",
        format!(
            "admin operation {} expected caller-ordered successful replica moves and one immediate independently observed settled target path",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        evidence(public, independent),
    ));
}

fn exact_alteration(
    public: Option<&Vec<Indexed<AdminReplicaLogDirsAlteration>>>,
    independent: Option<&Vec<Indexed<BrokerLogDirsState>>>,
    window: Option<AdminCommandWindow>,
    action: &AlterReplicaLogDirsAction,
) -> bool {
    let (Some([public]), Some([independent]), Some(first)) = (
        public.map(Vec::as_slice),
        independent.map(Vec::as_slice),
        action.assignments.first(),
    ) else {
        return false;
    };
    public.value.operation_id == action.operation_id
        && independent.value.operation_id == action.operation_id
        && public.value.throttle_time_ms <= action.timeout_ms
        && public.value.outcomes.len() == action.assignments.len()
        && public
            .value
            .outcomes
            .iter()
            .zip(&action.assignments)
            .all(|(actual, expected)| {
                actual.replica.topic == expected.topic
                    && actual.replica.partition == expected.partition
                    && actual.replica.broker_id == expected.broker_id
                    && actual.error_code.is_none()
            })
        && independent.value.topic == first.topic
        && independent.value.partition == first.partition
        && canonical_independent(&independent.value, &first.topic, first.partition)
        && action
            .assignments
            .iter()
            .all(|assignment| settled(&independent.value, assignment))
        && public_after_command(window, public.history_sequence)
        && immediate_after_public(
            window,
            public.history_sequence,
            independent.history_sequence,
        )
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
        && canonical_independent(&independent.value, &action.topic, action.partition)
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

fn canonical_independent(value: &BrokerLogDirsState, topic: &str, partition: i32) -> bool {
    !value.brokers.is_empty()
        && value.brokers.iter().all(|broker| {
            broker.broker_id >= 0
                && !broker.log_dirs.is_empty()
                && broker.log_dirs.iter().all(|directory| {
                    !directory.path.is_empty()
                        && directory.replicas.iter().all(|replica| {
                            replica.topic == topic
                                && replica.partition == partition
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

fn settled(value: &BrokerLogDirsState, assignment: &ReplicaLogDirAssignmentSpec) -> bool {
    let placements = value
        .brokers
        .iter()
        .filter(|broker| broker.broker_id == assignment.broker_id)
        .flat_map(|broker| &broker.log_dirs)
        .flat_map(|directory| {
            directory
                .replicas
                .iter()
                .filter(|replica| {
                    replica.topic == assignment.topic && replica.partition == assignment.partition
                })
                .map(|replica| (directory.path.as_str(), replica.is_future))
        })
        .collect::<Vec<_>>();
    placements.len() == 1 && placements[0] == (assignment.target_path.as_str(), false)
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

fn evidence<T>(
    public: Option<&Vec<Indexed<T>>>,
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
