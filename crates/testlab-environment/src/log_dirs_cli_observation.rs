//! Pinned Kafka CLI JSON becomes canonical selected-partition log-directory state.

use std::str;

use serde::Deserialize;
use testlab_schema::{
    BrokerLogDirState, BrokerLogDirsBrokerState, BrokerLogDirsState, BrokerStateObservation,
    LogDirReplicaState,
};

use crate::observer_admin_log_dirs_target::LogDirsTarget;
use crate::observer_error::ObserverError;

const QUERY_LINE: &str = "Querying brokers for log directories information";
const RECEIVED_PREFIX: &str = "Received log directory information from brokers ";

pub(crate) fn normalize(
    observation: u64,
    target: &LogDirsTarget,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let text =
        str::from_utf8(stdout).map_err(|_| invalid("log-directory output was not valid UTF-8"))?;
    let lines = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    let [query, received, json] = lines.as_slice() else {
        return Err(invalid(
            "log-directory output did not contain exactly two status lines and one JSON value",
        ));
    };
    if *query != QUERY_LINE {
        return Err(invalid("log-directory output had an unexpected query line"));
    }
    let Some(reported_brokers) = received.strip_prefix(RECEIVED_PREFIX) else {
        return Err(invalid(
            "log-directory output had an unexpected completion line",
        ));
    };
    let reported_brokers = parse_broker_list(reported_brokers)?;
    let root = serde_json::from_str::<CliRoot>(json)
        .map_err(|error| invalid(format!("log-directory JSON was invalid: {error}")))?;
    if root.version != 1 {
        return Err(invalid("log-directory JSON used an unsupported version"));
    }
    let mut brokers = root
        .brokers
        .into_iter()
        .map(|broker| normalize_broker(broker, target))
        .collect::<Result<Vec<_>, _>>()?;
    brokers.sort_unstable_by_key(|broker| broker.broker_id);
    if brokers.is_empty()
        || brokers.iter().any(|broker| broker.broker_id < 0)
        || brokers
            .windows(2)
            .any(|pair| pair[0].broker_id == pair[1].broker_id)
        || !brokers
            .iter()
            .map(|broker| broker.broker_id)
            .eq(reported_brokers)
    {
        return Err(invalid(
            "log-directory output had missing, repeated, or inconsistent brokers",
        ));
    }
    Ok(BrokerStateObservation::LogDirs(BrokerLogDirsState {
        observation,
        operation_id: target.operation_id.clone(),
        topic: target.topic.clone(),
        partition: target.partition,
        brokers,
    }))
}

fn normalize_broker(
    broker: CliBroker,
    target: &LogDirsTarget,
) -> Result<BrokerLogDirsBrokerState, ObserverError> {
    let mut log_dirs = broker
        .log_dirs
        .into_iter()
        .map(|directory| normalize_directory(directory, target))
        .collect::<Result<Vec<_>, _>>()?;
    log_dirs.sort_unstable_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    if log_dirs.is_empty() || log_dirs.windows(2).any(|pair| pair[0].path == pair[1].path) {
        return Err(invalid(
            "log-directory JSON omitted directories or repeated a path",
        ));
    }
    Ok(BrokerLogDirsBrokerState {
        broker_id: broker.broker,
        log_dirs,
    })
}

fn normalize_directory(
    directory: CliLogDir,
    target: &LogDirsTarget,
) -> Result<BrokerLogDirState, ObserverError> {
    if directory.path.is_empty() || directory.error.is_some() {
        return Err(invalid(
            "log-directory JSON contained an empty path or broker error",
        ));
    }
    let mut replicas = directory
        .partitions
        .into_iter()
        .map(|replica| normalize_replica(replica, target))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    replicas.sort_unstable();
    if replicas.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid("log-directory JSON repeated a replica"));
    }
    Ok(BrokerLogDirState {
        path: directory.path,
        replicas,
    })
}

fn normalize_replica(
    replica: CliReplica,
    target: &LogDirsTarget,
) -> Result<Option<LogDirReplicaState>, ObserverError> {
    let prefix = format!("{}-", target.topic);
    let partition = replica
        .partition
        .strip_prefix(&prefix)
        .and_then(|value| value.parse::<i32>().ok())
        .ok_or_else(|| invalid("log-directory JSON contained a malformed partition identity"))?;
    if partition < 0 {
        return Err(invalid(
            "log-directory JSON contained a negative partition identity",
        ));
    }
    if replica.size < 0 {
        return Err(invalid(
            "log-directory JSON contained a negative replica size",
        ));
    }
    if partition != target.partition {
        return Ok(None);
    }
    Ok(Some(LogDirReplicaState {
        topic: target.topic.clone(),
        partition,
        size_bytes: replica.size,
        offset_lag: replica.offset_lag,
        is_future: replica.is_future,
    }))
}

fn parse_broker_list(value: &str) -> Result<Vec<i32>, ObserverError> {
    let mut brokers = value
        .split(',')
        .map(|value| value.parse::<i32>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid("completion line contained an invalid broker ID"))?;
    brokers.sort_unstable();
    if brokers.is_empty()
        || brokers.iter().any(|broker| *broker < 0)
        || brokers.windows(2).any(|pair| pair[0] == pair[1])
    {
        return Err(invalid("completion line contained invalid broker IDs"));
    }
    Ok(brokers)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CliRoot {
    version: i32,
    brokers: Vec<CliBroker>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CliBroker {
    broker: i32,
    #[serde(rename = "logDirs")]
    log_dirs: Vec<CliLogDir>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CliLogDir {
    #[serde(rename = "logDir")]
    path: String,
    error: Option<String>,
    partitions: Vec<CliReplica>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CliReplica {
    partition: String,
    size: i64,
    #[serde(rename = "offsetLag")]
    offset_lag: i64,
    #[serde(rename = "isFuture")]
    is_future: bool,
}

fn invalid(message: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "Kafka CLI log-directory snapshot: {}",
        message.into()
    ))
}

#[cfg(test)]
#[path = "log_dirs_cli_observation_test.rs"]
mod tests;
