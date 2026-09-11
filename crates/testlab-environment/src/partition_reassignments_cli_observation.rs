//! Kafka's pinned reassignment CLI becomes canonical active-movement evidence.

use std::collections::BTreeSet;
use std::str;

use testlab_schema::{
    BrokerPartitionReassignmentsState, BrokerStateObservation, OperationId,
    PartitionReassignmentSnapshot,
};

use crate::observer_error::ObserverError;

const EMPTY: &str = "No partition reassignments found.";
const HEADER: &str = "Current partition reassignments:";

pub(crate) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let text = str::from_utf8(stdout)
        .map_err(|_| invalid("output was not valid UTF-8"))?
        .trim();
    let reassignments = if text == EMPTY {
        Vec::new()
    } else {
        let mut lines = text.lines();
        if lines.next() != Some(HEADER) {
            return Err(invalid("output had an unexpected header"));
        }
        lines.map(parse_row).collect::<Result<Vec<_>, _>>()?
    };
    if reassignments.windows(2).any(|pair| {
        pair[0].topic.as_bytes() > pair[1].topic.as_bytes()
            || (pair[0].topic == pair[1].topic && pair[0].partition >= pair[1].partition)
    }) {
        return Err(invalid("rows were not in canonical topic-partition order"));
    }
    Ok(BrokerStateObservation::PartitionReassignments(
        BrokerPartitionReassignmentsState {
            observation,
            operation_id: operation_id.clone(),
            reassignments,
        },
    ))
}

fn parse_row(line: &str) -> Result<PartitionReassignmentSnapshot, ObserverError> {
    let (identity, fields) = line
        .split_once(": replicas: ")
        .ok_or_else(|| invalid("row omitted its replicas field"))?;
    let (topic, partition) = identity
        .rsplit_once('-')
        .ok_or_else(|| invalid("row had a malformed partition identity"))?;
    let partition = partition
        .parse::<i32>()
        .map_err(|_| invalid("row had an invalid partition ID"))?;
    if topic.is_empty() || partition < 0 || !fields.ends_with('.') {
        return Err(invalid(
            "row had an invalid partition identity or terminator",
        ));
    }
    let fields = fields
        .strip_suffix('.')
        .ok_or_else(|| invalid("row omitted its terminator"))?;
    let mut parts = fields.split(". ");
    let replicas = parse_ids(
        parts
            .next()
            .ok_or_else(|| invalid("row omitted current replicas"))?,
        false,
    )?;
    let mut adding_replicas = Vec::new();
    let mut removing_replicas = Vec::new();
    for part in parts {
        if let Some(value) = part.strip_prefix("adding: ")
            && adding_replicas.is_empty()
        {
            adding_replicas = parse_ids(value, false)?;
        } else if let Some(value) = part.strip_prefix("removing: ")
            && removing_replicas.is_empty()
        {
            removing_replicas = parse_ids(value, false)?;
        } else {
            return Err(invalid("row contained repeated or unknown fields"));
        }
    }
    Ok(PartitionReassignmentSnapshot {
        topic: topic.to_owned(),
        partition,
        replicas,
        adding_replicas,
        removing_replicas,
    })
}

fn parse_ids(value: &str, allow_empty: bool) -> Result<Vec<i32>, ObserverError> {
    if allow_empty && value.is_empty() {
        return Ok(Vec::new());
    }
    let values = value
        .split(',')
        .map(str::parse::<i32>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid("row contained an invalid broker ID"))?;
    let unique = values.iter().copied().collect::<BTreeSet<_>>();
    if values.is_empty() || values.iter().any(|value| *value < 0) || unique.len() != values.len() {
        return Err(invalid("row contained invalid or repeated broker IDs"));
    }
    Ok(values)
}

fn invalid(detail: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "Kafka CLI partition-reassignment snapshot: {}",
        detail.into()
    ))
}

#[cfg(test)]
#[path = "partition_reassignments_cli_observation_test.rs"]
mod tests;
