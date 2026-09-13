//! Pinned Kafka CLI output becomes canonical active-producer broker state.

use std::str;

use testlab_schema::{BrokerProducersState, BrokerStateObservation, ProducerStateSnapshot};

use crate::observer_admin_producer_target::ProducerTarget;
use crate::observer_error::ObserverError;

const HEADERS: [&str; 6] = [
    "ProducerId",
    "ProducerEpoch",
    "LatestCoordinatorEpoch",
    "LastSequence",
    "LastTimestamp",
    "CurrentTransactionStartOffset",
];

pub(crate) fn normalize(
    observation: u64,
    target: &ProducerTarget,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let text = str::from_utf8(stdout)
        .map_err(|_| invalid("active-producer output was not valid UTF-8"))?;
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let Some(header) = lines.next() else {
        return Err(invalid("active-producer output omitted its header"));
    };
    if !header.split_whitespace().eq(HEADERS) {
        return Err(invalid("active-producer output had unexpected columns"));
    }
    let mut producers = lines.map(parse_row).collect::<Result<Vec<_>, _>>()?;
    producers.sort_unstable_by_key(|producer| producer.producer_id);
    if producers
        .windows(2)
        .any(|pair| pair[0].producer_id == pair[1].producer_id)
    {
        return Err(invalid("active-producer output repeated a producer ID"));
    }
    Ok(BrokerStateObservation::Producers(BrokerProducersState {
        observation,
        operation_id: target.operation_id.clone(),
        topic: target.topic.clone(),
        partition: target.partition,
        producers,
    }))
}

fn parse_row(line: &str) -> Result<ProducerStateSnapshot, ObserverError> {
    let columns = line.split_whitespace().collect::<Vec<_>>();
    let [
        producer_id,
        producer_epoch,
        coordinator_epoch,
        last_sequence,
        last_timestamp,
        start,
    ] = columns.as_slice()
    else {
        return Err(invalid("active-producer row did not contain six columns"));
    };
    Ok(ProducerStateSnapshot {
        producer_id: nonnegative_i64(producer_id, "producer ID")?,
        producer_epoch: nonnegative_i32(producer_epoch, "producer epoch")?,
        coordinator_epoch: nonnegative_i32(coordinator_epoch, "coordinator epoch")?,
        last_sequence: sentinel_i32(last_sequence, "last sequence")?,
        last_timestamp: sentinel_i64(last_timestamp, "last timestamp")?,
        current_transaction_start_offset: if *start == "None" {
            None
        } else {
            Some(nonnegative_i64(start, "transaction start offset")?)
        },
    })
}

fn nonnegative_i64(value: &str, field: &str) -> Result<i64, ObserverError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| invalid(format!("{field} was not a nonnegative i64")))
}

fn nonnegative_i32(value: &str, field: &str) -> Result<i32, ObserverError> {
    value
        .parse::<i32>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| invalid(format!("{field} was not a nonnegative i32")))
}

fn sentinel_i64(value: &str, field: &str) -> Result<i64, ObserverError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= -1)
        .ok_or_else(|| invalid(format!("{field} was below Kafka's -1 sentinel")))
}

fn sentinel_i32(value: &str, field: &str) -> Result<i32, ObserverError> {
    value
        .parse::<i32>()
        .ok()
        .filter(|value| *value >= -1)
        .ok_or_else(|| invalid(format!("{field} was below Kafka's -1 sentinel")))
}

fn invalid(message: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "Kafka CLI active-producer snapshot: {}",
        message.into()
    ))
}

#[cfg(test)]
#[path = "producer_cli_observation_test.rs"]
mod tests;
