//! Metadata-quorum discovery exposes every stable public Raft fact.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminMetadataQuorumDescription, CommandId,
    DescribeMetadataQuorumCommand, MetadataQuorumListenerState, MetadataQuorumNodeState,
    MetadataQuorumReplicaState, OperationId,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{MetadataQuorumListener, MetadataQuorumNode, MetadataQuorumReplica};
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeMetadataQuorumCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let description = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_metadata_quorum()
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let (leader_id, leader_epoch, high_watermark, voters, observers, nodes) =
        description.into_parts();
    let voters = voters.into_iter().map(replica).collect::<Vec<_>>();
    let observers = observers.into_iter().map(replica).collect::<Vec<_>>();
    let nodes = nodes.map(|nodes| nodes.into_iter().map(node).collect::<Vec<_>>());
    validate(
        &command.operation_id,
        leader_id,
        leader_epoch,
        high_watermark,
        &voters,
        &observers,
        nodes.as_deref(),
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::MetadataQuorumDescribed(AdminMetadataQuorumDescription {
                operation_id: command.operation_id,
                leader_id,
                leader_epoch,
                high_watermark,
                voters,
                observers,
                nodes,
            }),
        ),
    )
}

fn replica(replica: MetadataQuorumReplica) -> MetadataQuorumReplicaState {
    let (replica_id, directory_id, log_end_offset, last_fetch, last_caught_up) =
        replica.into_parts();
    MetadataQuorumReplicaState {
        replica_id,
        replica_directory_id: directory_id.map(kafka_uuid),
        log_end_offset,
        last_fetch_timestamp_ms: last_fetch,
        last_caught_up_timestamp_ms: last_caught_up,
    }
}

fn node(node: MetadataQuorumNode) -> MetadataQuorumNodeState {
    let (node_id, listeners) = node.into_parts();
    MetadataQuorumNodeState {
        node_id,
        listeners: listeners.into_iter().map(listener).collect(),
    }
}

fn listener(listener: MetadataQuorumListener) -> MetadataQuorumListenerState {
    let (name, host, port) = listener.into_parts();
    MetadataQuorumListenerState { name, host, port }
}

pub(crate) fn kafka_uuid(source: [u8; 16]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut encoded = String::with_capacity(22);
    for chunk in source[..15].chunks_exact(3) {
        encoded.push(char::from(ALPHABET[usize::from(chunk[0] >> 2)]));
        encoded.push(char::from(
            ALPHABET[usize::from(((chunk[0] & 3) << 4) | (chunk[1] >> 4))],
        ));
        encoded.push(char::from(
            ALPHABET[usize::from(((chunk[1] & 15) << 2) | (chunk[2] >> 6))],
        ));
        encoded.push(char::from(ALPHABET[usize::from(chunk[2] & 63)]));
    }
    encoded.push(char::from(ALPHABET[usize::from(source[15] >> 2)]));
    encoded.push(char::from(ALPHABET[usize::from((source[15] & 3) << 4)]));
    encoded
}

#[allow(
    clippy::too_many_arguments,
    reason = "parameters mirror one metadata-quorum state record"
)]
fn validate(
    operation_id: &OperationId,
    leader_id: Option<i32>,
    leader_epoch: i32,
    high_watermark: i64,
    voters: &[MetadataQuorumReplicaState],
    observers: &[MetadataQuorumReplicaState],
    nodes: Option<&[MetadataQuorumNodeState]>,
) -> Result<(), AdapterError> {
    let canonical_replicas = |replicas: &[MetadataQuorumReplicaState]| {
        replicas.iter().all(valid_replica)
            && replicas
                .windows(2)
                .all(|pair| pair[0].replica_id < pair[1].replica_id)
    };
    if voters.is_empty()
        || leader_epoch < 0
        || high_watermark < 0
        || !canonical_replicas(voters)
        || !canonical_replicas(observers)
        || voters.iter().any(|voter| {
            observers
                .iter()
                .any(|observer| observer.replica_id == voter.replica_id)
        })
        || leader_id.is_some_and(|leader| !voters.iter().any(|item| item.replica_id == leader))
        || nodes.is_some_and(|nodes| !valid_nodes(nodes))
    {
        return Err(invalid(operation_id, "returned noncanonical quorum state"));
    }
    Ok(())
}

fn valid_replica(replica: &MetadataQuorumReplicaState) -> bool {
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
}

fn valid_nodes(nodes: &[MetadataQuorumNodeState]) -> bool {
    nodes.iter().all(|node| {
        node.node_id >= 0
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

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
