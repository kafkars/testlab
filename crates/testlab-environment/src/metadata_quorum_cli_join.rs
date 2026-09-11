//! Exact joins and cross-view invariants for metadata-quorum CLI output.

use testlab_schema::{
    MetadataQuorumListenerState, MetadataQuorumNodeState, MetadataQuorumReplicaState,
};

use super::{ReplicationRow, Role, StatusReplica, StatusSnapshot, invalid};
use crate::observer_error::ObserverError;

type Combined = (
    Vec<MetadataQuorumReplicaState>,
    Vec<MetadataQuorumReplicaState>,
    Vec<MetadataQuorumNodeState>,
);

pub(super) fn combine(
    status: &StatusSnapshot,
    replication: &[ReplicationRow],
) -> Result<Combined, ObserverError> {
    let voters = join_role(&status.voters, replication, status.leader_id, false)?;
    let observers = join_role(&status.observers, replication, status.leader_id, true)?;
    if voters.len() + observers.len() != replication.len() {
        return Err(invalid(
            "status and replication views exposed different replicas",
        ));
    }
    validate_offsets(status, replication)?;
    Ok((voters, observers, nodes(&status.voters, &status.observers)?))
}

fn join_role(
    status: &[StatusReplica],
    replication: &[ReplicationRow],
    leader_id: Option<i32>,
    observer: bool,
) -> Result<Vec<MetadataQuorumReplicaState>, ObserverError> {
    status
        .iter()
        .map(|expected| {
            let row = replication
                .iter()
                .find(|row| row.replica.replica_id == expected.id)
                .ok_or_else(|| invalid("status replica was absent from replication view"))?;
            let expected_role = if observer {
                Role::Observer
            } else if leader_id == Some(expected.id) {
                Role::Leader
            } else {
                Role::Follower
            };
            if row.role != expected_role
                || row.replica.replica_directory_id != expected.directory_id
            {
                return Err(invalid(
                    "CLI views disagreed on replica role or directory ID",
                ));
            }
            Ok(row.replica.clone())
        })
        .collect()
}

fn validate_offsets(
    status: &StatusSnapshot,
    replication: &[ReplicationRow],
) -> Result<(), ObserverError> {
    let Some(leader_id) = status.leader_id else {
        return Err(invalid("stable status view had no leader"));
    };
    let leader = replication
        .iter()
        .find(|row| row.replica.replica_id == leader_id)
        .and_then(|row| row.replica.log_end_offset)
        .ok_or_else(|| invalid("leader had no log-end offset"))?;
    if status.high_watermark > leader {
        return Err(invalid("high watermark exceeded the leader log-end offset"));
    }
    for row in replication {
        let Some(offset) = row.replica.log_end_offset else {
            return Err(invalid("stable replication row had no log-end offset"));
        };
        if leader.checked_sub(offset) != Some(row.lag) {
            return Err(invalid("replication lag disagreed with log-end offsets"));
        }
    }
    let minimum = replication
        .iter()
        .filter(|row| row.role != Role::Observer)
        .filter_map(|row| row.replica.log_end_offset)
        .min()
        .ok_or_else(|| invalid("voters had no log-end offset"))?;
    if leader - minimum != status.max_follower_lag {
        return Err(invalid(
            "status maximum lag disagreed with replication offsets",
        ));
    }
    Ok(())
}

fn nodes(
    voters: &[StatusReplica],
    observers: &[StatusReplica],
) -> Result<Vec<MetadataQuorumNodeState>, ObserverError> {
    voters
        .iter()
        .chain(observers)
        .filter(|replica| !replica.endpoints.is_empty())
        .map(|replica| {
            let mut listeners = replica
                .endpoints
                .iter()
                .map(|endpoint| listener(endpoint))
                .collect::<Result<Vec<_>, _>>()?;
            listeners.sort_unstable();
            if listeners
                .windows(2)
                .any(|pair| pair[0].name == pair[1].name)
            {
                return Err(invalid("status node repeated a listener name"));
            }
            Ok(MetadataQuorumNodeState {
                node_id: replica.id,
                listeners,
            })
        })
        .collect()
}

fn listener(value: &str) -> Result<MetadataQuorumListenerState, ObserverError> {
    let (name, address) = value
        .split_once("://")
        .ok_or_else(|| invalid("controller endpoint omitted its listener separator"))?;
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| invalid("controller endpoint omitted its port"))?;
    let host = bracketed_host(host)?;
    let port = port
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| invalid("controller endpoint had an invalid port"))?;
    if name.is_empty() || host.is_empty() {
        return Err(invalid("controller endpoint had an empty name or host"));
    }
    Ok(MetadataQuorumListenerState {
        name: name.to_owned(),
        host: host.to_owned(),
        port,
    })
}

fn bracketed_host(value: &str) -> Result<&str, ObserverError> {
    let host = match (value.strip_prefix('['), value.strip_suffix(']')) {
        (Some(without_open), Some(_)) => without_open
            .strip_suffix(']')
            .ok_or_else(|| invalid("controller endpoint had malformed IPv6 brackets")),
        (None, None) => Ok(value),
        _ => Err(invalid("controller endpoint had unmatched IPv6 brackets")),
    }?;
    if host.contains('[') || host.contains(']') {
        return Err(invalid("controller endpoint had nested IPv6 brackets"));
    }
    Ok(host)
}
