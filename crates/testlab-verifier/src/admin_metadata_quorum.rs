//! Metadata-quorum verification joins public Raft state to both Kafka CLI views.

use testlab_schema::{
    AdminMetadataQuorumDescription, BrokerMetadataQuorumState, MetadataQuorumNodeState,
    MetadataQuorumReplicaState, ScenarioAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_features::Indexed;
use crate::support::violation;

pub(crate) fn verify_metadata_quorum_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    let ScenarioAction::DescribeMetadataQuorum(action) = action else {
        return false;
    };
    let public = index
        .admin_features
        .metadata_quorum_described
        .get(&action.operation_id);
    let independent = index
        .admin_features
        .metadata_quorum_observed
        .get(&action.operation_id);
    if exact_match(public, independent, command_window) {
        return true;
    }
    violations.push(violation(
        "ADMIN-056",
        format!(
            "admin operation {} expected one canonical public metadata-quorum result followed immediately by matching Kafka CLI status and replication state",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        evidence(public, independent),
    ));
    true
}

fn exact_match(
    public: Option<&Vec<Indexed<AdminMetadataQuorumDescription>>>,
    independent: Option<&Vec<Indexed<BrokerMetadataQuorumState>>>,
    window: Option<AdminCommandWindow>,
) -> bool {
    let (Some([public]), Some([independent])) =
        (public.map(Vec::as_slice), independent.map(Vec::as_slice))
    else {
        return false;
    };
    canonical(
        public.value.leader_id,
        public.value.leader_epoch,
        public.value.high_watermark,
        &public.value.voters,
        &public.value.observers,
        public.value.nodes.as_deref().unwrap_or_default(),
    ) && canonical(
        independent.value.leader_id,
        independent.value.leader_epoch,
        independent.value.high_watermark,
        &independent.value.voters,
        &independent.value.observers,
        &independent.value.nodes,
    ) && public.value.leader_id == independent.value.leader_id
        && public.value.leader_epoch == independent.value.leader_epoch
        && public.value.high_watermark <= independent.value.high_watermark
        && replicas_progress(&public.value.voters, &independent.value.voters)
        && replicas_progress(&public.value.observers, &independent.value.observers)
        && nodes_match(public.value.nodes.as_deref(), &independent.value.nodes)
        && public_after_command(window, public.history_sequence)
        && immediate_after_public(
            window,
            public.history_sequence,
            independent.history_sequence,
        )
}

fn canonical(
    leader_id: Option<i32>,
    leader_epoch: i32,
    high_watermark: i64,
    voters: &[MetadataQuorumReplicaState],
    observers: &[MetadataQuorumReplicaState],
    nodes: &[MetadataQuorumNodeState],
) -> bool {
    !voters.is_empty()
        && leader_epoch >= 0
        && high_watermark >= 0
        && leader_id.is_some_and(|leader| voters.iter().any(|item| item.replica_id == leader))
        && canonical_replicas(voters)
        && canonical_replicas(observers)
        && voters.iter().all(|voter| {
            observers
                .iter()
                .all(|observer| observer.replica_id != voter.replica_id)
        })
        && canonical_nodes(nodes)
}

fn canonical_replicas(replicas: &[MetadataQuorumReplicaState]) -> bool {
    replicas.iter().all(|replica| {
        replica.replica_id >= 0
            && replica
                .replica_directory_id
                .as_deref()
                .is_none_or(valid_uuid)
            && replica.log_end_offset.is_none_or(|value| value >= 0)
            && replica
                .last_fetch_timestamp_ms
                .is_none_or(|value| value >= 0)
            && replica
                .last_caught_up_timestamp_ms
                .is_none_or(|value| value >= 0)
    }) && replicas
        .windows(2)
        .all(|pair| pair[0].replica_id < pair[1].replica_id)
}

fn canonical_nodes(nodes: &[MetadataQuorumNodeState]) -> bool {
    nodes.iter().all(|node| {
        node.node_id >= 0
            && !node.listeners.is_empty()
            && node.listeners.iter().all(|listener| {
                !listener.name.is_empty() && !listener.host.is_empty() && listener.port > 0
            })
            && node
                .listeners
                .windows(2)
                .all(|pair| pair[0].name.as_bytes() < pair[1].name.as_bytes())
    }) && nodes
        .windows(2)
        .all(|pair| pair[0].node_id < pair[1].node_id)
}

fn replicas_progress(
    public: &[MetadataQuorumReplicaState],
    independent: &[MetadataQuorumReplicaState],
) -> bool {
    public.len() == independent.len()
        && public.iter().zip(independent).all(|(public, independent)| {
            public.replica_id == independent.replica_id
                && public.replica_directory_id == independent.replica_directory_id
                && optional_progress(public.log_end_offset, independent.log_end_offset)
                && optional_progress(
                    public.last_fetch_timestamp_ms,
                    independent.last_fetch_timestamp_ms,
                )
                && optional_progress(
                    public.last_caught_up_timestamp_ms,
                    independent.last_caught_up_timestamp_ms,
                )
        })
}

fn optional_progress(public: Option<i64>, independent: Option<i64>) -> bool {
    match (public, independent) {
        (None, None) => true,
        (None, Some(_)) => true,
        (Some(public), Some(independent)) => public <= independent,
        _ => false,
    }
}

fn nodes_match(
    public: Option<&[MetadataQuorumNodeState]>,
    independent: &[MetadataQuorumNodeState],
) -> bool {
    public.map_or_else(|| independent.is_empty(), |public| public == independent)
}

fn valid_uuid(value: &str) -> bool {
    value.len() == 22
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        && value
            .as_bytes()
            .last()
            .is_some_and(|byte| matches!(*byte, b'A' | b'Q' | b'g' | b'w'))
}

fn evidence(
    public: Option<&Vec<Indexed<AdminMetadataQuorumDescription>>>,
    independent: Option<&Vec<Indexed<BrokerMetadataQuorumState>>>,
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
