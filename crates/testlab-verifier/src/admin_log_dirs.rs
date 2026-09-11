//! Log-directory verification joins public broker routing to Kafka CLI state.

use testlab_schema::{
    AdminBrokerLogDirsDescription, AdminLogDirDescription, AdminLogDirsDescription,
    BrokerLogDirState, BrokerLogDirsBrokerState, BrokerLogDirsState, DescribeLogDirsAction,
    LogDirReplicaState, ScenarioAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_features::Indexed;
use crate::support::violation;

pub(crate) fn verify_log_dirs_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    let ScenarioAction::DescribeLogDirs(action) = action else {
        return false;
    };
    let public = index
        .admin_features
        .log_dirs_described
        .get(&action.operation_id);
    let independent = index
        .admin_features
        .log_dirs_observed
        .get(&action.operation_id);
    if exact_match(public, independent, command_window, action) {
        return true;
    }
    violations.push(violation(
        "ADMIN-054",
        format!(
            "admin operation {} expected {} current replica log(s) in descending caller broker order exactly matching one immediate Kafka CLI snapshot",
            action.operation_id, action.expected_replica_count
        ),
        Some(action.operation_id.clone()),
        evidence(public, independent),
    ));
    true
}

fn exact_match(
    public: Option<&Vec<Indexed<AdminLogDirsDescription>>>,
    independent: Option<&Vec<Indexed<BrokerLogDirsState>>>,
    window: Option<AdminCommandWindow>,
    action: &DescribeLogDirsAction,
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
        && same_shared_state(&public.value.brokers, &independent.value.brokers)
        && current_replica_count(&public.value.brokers) == action.expected_replica_count
        && public_after_command(window, public.history_sequence)
        && immediate_after_public(
            window,
            public.history_sequence,
            independent.history_sequence,
        )
}

fn canonical_public(value: &AdminLogDirsDescription, action: &DescribeLogDirsAction) -> bool {
    !value.brokers.is_empty()
        && value.brokers.iter().all(|broker| {
            broker.broker_id >= 0
                && !broker.log_dirs.is_empty()
                && canonical_public_directories(&broker.log_dirs, action)
        })
        && value
            .brokers
            .windows(2)
            .all(|pair| pair[0].broker_id > pair[1].broker_id)
        && value
            .brokers
            .iter()
            .flat_map(|broker| &broker.log_dirs)
            .flat_map(|directory| &directory.replicas)
            .all(|replica| !replica.is_future)
}

fn canonical_public_directories(
    directories: &[AdminLogDirDescription],
    action: &DescribeLogDirsAction,
) -> bool {
    directories.iter().all(|directory| {
        !directory.path.is_empty()
            && directory.total_bytes.is_none_or(|value| value >= 0)
            && directory.usable_bytes.is_none_or(|value| value >= 0)
            && match (directory.total_bytes, directory.usable_bytes) {
                (Some(total), Some(usable)) => usable <= total,
                _ => true,
            }
            && canonical_replicas(&directory.replicas, action)
    }) && directories
        .windows(2)
        .all(|pair| pair[0].path.as_bytes() < pair[1].path.as_bytes())
}

fn canonical_independent(value: &BrokerLogDirsState, action: &DescribeLogDirsAction) -> bool {
    !value.brokers.is_empty()
        && value.brokers.iter().all(|broker| {
            broker.broker_id >= 0
                && !broker.log_dirs.is_empty()
                && canonical_broker_directories(&broker.log_dirs, action)
        })
        && value
            .brokers
            .windows(2)
            .all(|pair| pair[0].broker_id < pair[1].broker_id)
}

fn canonical_broker_directories(
    directories: &[BrokerLogDirState],
    action: &DescribeLogDirsAction,
) -> bool {
    directories.iter().all(|directory| {
        !directory.path.is_empty() && canonical_replicas(&directory.replicas, action)
    }) && directories
        .windows(2)
        .all(|pair| pair[0].path.as_bytes() < pair[1].path.as_bytes())
}

fn canonical_replicas(replicas: &[LogDirReplicaState], action: &DescribeLogDirsAction) -> bool {
    replicas.iter().all(|replica| {
        replica.topic == action.topic
            && replica.partition == action.partition
            && replica.size_bytes >= 0
    }) && replicas.windows(2).all(|pair| pair[0] < pair[1])
}

fn same_shared_state(
    public: &[AdminBrokerLogDirsDescription],
    independent: &[BrokerLogDirsBrokerState],
) -> bool {
    public.len() == independent.len()
        && public
            .iter()
            .rev()
            .zip(independent)
            .all(|(public, independent)| {
                public.broker_id == independent.broker_id
                    && public.log_dirs.len() == independent.log_dirs.len()
                    && public.log_dirs.iter().zip(&independent.log_dirs).all(
                        |(public, independent)| {
                            public.path == independent.path
                                && public.replicas == independent.replicas
                        },
                    )
            })
}

fn current_replica_count(brokers: &[AdminBrokerLogDirsDescription]) -> usize {
    brokers
        .iter()
        .flat_map(|broker| &broker.log_dirs)
        .flat_map(|directory| &directory.replicas)
        .filter(|replica| !replica.is_future)
        .count()
}

fn evidence(
    public: Option<&Vec<Indexed<AdminLogDirsDescription>>>,
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
