//! Pinned Kafka quorum CLI views become one canonical independent snapshot.

use std::str;

use serde::Deserialize;
use testlab_schema::{
    BrokerMetadataQuorumState, BrokerStateObservation, MetadataQuorumReplicaState, OperationId,
};

use crate::observer_error::ObserverError;

#[path = "metadata_quorum_cli_join.rs"]
mod join;
#[path = "metadata_quorum_cli_membership.rs"]
mod membership;

const DIRECTORY_REPLICATION_HEADERS: [&str; 7] = [
    "NodeId",
    "DirectoryId",
    "LogEndOffset",
    "Lag",
    "LastFetchTimestamp",
    "LastCaughtUpTimestamp",
    "Status",
];
const LEGACY_REPLICATION_HEADERS: [&str; 6] = [
    "NodeId",
    "LogEndOffset",
    "Lag",
    "LastFetchTimestamp",
    "LastCaughtUpTimestamp",
    "Status",
];
const ZERO_UUID: &str = "AAAAAAAAAAAAAAAAAAAAAA";

#[derive(Debug)]
pub(super) struct StatusSnapshot {
    pub(super) leader_id: Option<i32>,
    pub(super) leader_epoch: i32,
    pub(super) high_watermark: i64,
    pub(super) max_follower_lag: i64,
    pub(super) voters: Vec<StatusReplica>,
    pub(super) observers: Vec<StatusReplica>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StatusReplica {
    pub(super) id: i32,
    #[serde(default)]
    pub(super) directory_id: Option<String>,
    #[serde(default)]
    pub(super) endpoints: Vec<String>,
}

#[derive(Debug)]
pub(super) struct ReplicationRow {
    pub(super) replica: MetadataQuorumReplicaState,
    pub(super) lag: i64,
    pub(super) role: Role,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Role {
    Leader,
    Follower,
    Observer,
}

pub(crate) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    status_stdout: &[u8],
    replication_stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let status = parse_status(status_stdout)?;
    let replication = parse_replication(replication_stdout)?;
    let (voters, observers, nodes) = join::combine(&status, &replication)?;
    Ok(BrokerStateObservation::MetadataQuorum(
        BrokerMetadataQuorumState {
            observation,
            operation_id: operation_id.clone(),
            leader_id: status.leader_id,
            leader_epoch: status.leader_epoch,
            high_watermark: status.high_watermark,
            voters,
            observers,
            nodes,
        },
    ))
}

fn parse_status(stdout: &[u8]) -> Result<StatusSnapshot, ObserverError> {
    let text = str::from_utf8(stdout).map_err(|_| invalid("status view was not UTF-8"))?;
    let lines = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let [
        cluster,
        leader,
        epoch,
        watermark,
        lag,
        lag_time,
        voters,
        observers,
    ] = lines.as_slice()
    else {
        return Err(invalid("status view did not contain exactly eight fields"));
    };
    let cluster_id = field(cluster, "ClusterId:")?;
    if !valid_uuid(cluster_id) {
        return Err(invalid("status view had an invalid cluster ID"));
    }
    let leader_id = optional_i32(field(leader, "LeaderId:")?, "leader ID")?;
    let leader_epoch = nonnegative_i32(field(epoch, "LeaderEpoch:")?, "leader epoch")?;
    let high_watermark = nonnegative_i64(field(watermark, "HighWatermark:")?, "high watermark")?;
    let max_follower_lag = nonnegative_i64(field(lag, "MaxFollowerLag:")?, "maximum lag")?;
    sentinel_i64(
        field(lag_time, "MaxFollowerLagTimeMs:")?,
        "maximum lag time",
    )?;
    let mut voters = membership::parse(field(voters, "CurrentVoters:")?, "voters")?;
    let mut observers = membership::parse(field(observers, "CurrentObservers:")?, "observers")?;
    membership::canonicalize(&mut voters, "voters")?;
    membership::canonicalize(&mut observers, "observers")?;
    if voters.is_empty()
        || voters
            .iter()
            .any(|voter| observers.iter().any(|observer| voter.id == observer.id))
        || leader_id.is_some_and(|id| !voters.iter().any(|voter| voter.id == id))
    {
        return Err(invalid("status view had incoherent quorum membership"));
    }
    Ok(StatusSnapshot {
        leader_id,
        leader_epoch,
        high_watermark,
        max_follower_lag,
        voters,
        observers,
    })
}

fn field<'a>(line: &'a str, label: &str) -> Result<&'a str, ObserverError> {
    line.trim()
        .strip_prefix(label)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid(format!("status view omitted {label}")))
}

fn parse_replication(stdout: &[u8]) -> Result<Vec<ReplicationRow>, ObserverError> {
    let text = str::from_utf8(stdout).map_err(|_| invalid("replication view was not UTF-8"))?;
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let Some(header) = lines.next() else {
        return Err(invalid("replication view omitted its header"));
    };
    let columns = header.split_whitespace().collect::<Vec<_>>();
    let has_directory_id = if columns.as_slice() == DIRECTORY_REPLICATION_HEADERS.as_slice() {
        true
    } else if columns.as_slice() == LEGACY_REPLICATION_HEADERS.as_slice() {
        false
    } else {
        return Err(invalid("replication view had unexpected columns"));
    };
    let mut rows = lines
        .map(|line| replication_row(line, has_directory_id))
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Err(invalid("replication view returned no replicas"));
    }
    rows.sort_unstable_by_key(|row| row.replica.replica_id);
    if rows
        .windows(2)
        .any(|pair| pair[0].replica.replica_id == pair[1].replica.replica_id)
    {
        return Err(invalid("replication view repeated a node ID"));
    }
    Ok(rows)
}

fn replication_row(line: &str, has_directory_id: bool) -> Result<ReplicationRow, ObserverError> {
    let columns = line.split_whitespace().collect::<Vec<_>>();
    let (id, directory, offset, lag, fetched, caught_up, role) = if has_directory_id {
        let [id, directory, offset, lag, fetched, caught_up, role] = columns.as_slice() else {
            return Err(invalid("replication row did not contain seven columns"));
        };
        (
            *id,
            Some(*directory),
            *offset,
            *lag,
            *fetched,
            *caught_up,
            *role,
        )
    } else {
        let [id, offset, lag, fetched, caught_up, role] = columns.as_slice() else {
            return Err(invalid(
                "legacy replication row did not contain six columns",
            ));
        };
        (*id, None, *offset, *lag, *fetched, *caught_up, *role)
    };
    Ok(ReplicationRow {
        replica: MetadataQuorumReplicaState {
            replica_id: nonnegative_i32(id, "replica ID")?,
            replica_directory_id: directory_id(directory)?,
            log_end_offset: optional_i64(offset, "log-end offset")?,
            last_fetch_timestamp_ms: optional_i64(fetched, "last-fetch timestamp")?,
            last_caught_up_timestamp_ms: optional_i64(caught_up, "last-caught-up timestamp")?,
        },
        lag: nonnegative_i64(lag, "replica lag")?,
        role: match role {
            "Leader" => Role::Leader,
            "Follower" => Role::Follower,
            "Observer" => Role::Observer,
            _ => return Err(invalid("replication row had an unknown status")),
        },
    })
}

fn directory_id(value: Option<&str>) -> Result<Option<String>, ObserverError> {
    match value {
        None | Some(ZERO_UUID) => Ok(None),
        Some(value) if valid_uuid(value) => Ok(Some(value.to_owned())),
        Some(_) => Err(invalid("replica had an invalid directory ID")),
    }
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

fn optional_i32(value: &str, field: &str) -> Result<Option<i32>, ObserverError> {
    value
        .parse::<i32>()
        .ok()
        .and_then(|value| match value {
            -1 => Some(None),
            0.. => Some(Some(value)),
            _ => None,
        })
        .ok_or_else(|| invalid(format!("{field} had an invalid sentinel")))
}

fn optional_i64(value: &str, field: &str) -> Result<Option<i64>, ObserverError> {
    value
        .parse::<i64>()
        .ok()
        .and_then(|value| match value {
            -1 => Some(None),
            0.. => Some(Some(value)),
            _ => None,
        })
        .ok_or_else(|| invalid(format!("{field} had an invalid sentinel")))
}

fn sentinel_i64(value: &str, field: &str) -> Result<(), ObserverError> {
    optional_i64(value, field).map(|_| ())
}

fn nonnegative_i32(value: &str, field: &str) -> Result<i32, ObserverError> {
    value
        .parse::<i32>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| invalid(format!("{field} was not a nonnegative i32")))
}

fn nonnegative_i64(value: &str, field: &str) -> Result<i64, ObserverError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| invalid(format!("{field} was not a nonnegative i64")))
}

pub(super) fn invalid(message: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "Kafka CLI metadata-quorum snapshot: {}",
        message.into()
    ))
}

#[cfg(test)]
#[path = "metadata_quorum_cli_observation_test.rs"]
mod tests;
