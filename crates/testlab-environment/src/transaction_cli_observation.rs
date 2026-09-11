//! Pinned Kafka transaction CLI output becomes canonical broker state.

use std::collections::BTreeMap;
use std::str;

use testlab_schema::{
    BrokerStateObservation, BrokerTransactionListing, BrokerTransactionState,
    BrokerTransactionsState, OperationId, TransactionDescriptionSnapshot,
    TransactionListingSnapshot, TransactionTopicSnapshot,
};

use crate::observer_error::ObserverError;

const LIST_HEADERS: [&str; 4] = [
    "TransactionalId",
    "Coordinator",
    "ProducerId",
    "TransactionState",
];
const DESCRIPTION_HEADERS: [&str; 9] = [
    "CoordinatorId",
    "TransactionalId",
    "ProducerId",
    "ProducerEpoch",
    "TransactionState",
    "TransactionTimeoutMs",
    "CurrentTransactionStartTimeMs",
    "TransactionDurationMs",
    "TopicPartitions",
];

pub(crate) fn normalize_list(
    observation: u64,
    operation_id: &OperationId,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let lines = rows(stdout, &LIST_HEADERS, "transaction listing")?;
    let mut transactions = lines
        .into_iter()
        .map(parse_listing)
        .collect::<Result<Vec<_>, _>>()?;
    transactions.sort_unstable_by(|left, right| {
        left.transaction
            .transactional_id
            .as_bytes()
            .cmp(right.transaction.transactional_id.as_bytes())
    });
    if transactions
        .windows(2)
        .any(|pair| pair[0].transaction.transactional_id == pair[1].transaction.transactional_id)
    {
        return Err(invalid("transaction listing repeated a transactional ID"));
    }
    Ok(BrokerStateObservation::Transactions(
        BrokerTransactionsState {
            observation,
            operation_id: operation_id.clone(),
            transactions,
        },
    ))
}

pub(crate) fn normalize_description(
    observation: u64,
    operation_id: &OperationId,
    expected_id: &str,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let rows = rows(stdout, &DESCRIPTION_HEADERS, "transaction description")?;
    let [row] = rows.as_slice() else {
        return Err(invalid(
            "transaction description did not contain exactly one row",
        ));
    };
    let columns = row.split_whitespace().collect::<Vec<_>>();
    if !(8..=9).contains(&columns.len()) {
        return Err(invalid(
            "transaction description row did not contain eight or nine columns",
        ));
    }
    let transactional_id = columns[1];
    if transactional_id != expected_id {
        return Err(invalid(
            "transaction description changed the transactional ID",
        ));
    }
    let start = optional_i64(columns[6], "transaction start time")?;
    let duration = optional_i64(columns[7], "transaction duration")?;
    if start.is_some() != duration.is_some() {
        return Err(invalid(
            "transaction start time and duration presence disagreed",
        ));
    }
    let transaction = TransactionDescriptionSnapshot {
        transactional_id: transactional_id.to_owned(),
        producer_id: nonnegative_i64(columns[2], "producer ID")?,
        producer_epoch: nonnegative_i16(columns[3], "producer epoch")?,
        transaction_state: nonempty(columns[4], "transaction state")?,
        transaction_timeout_ms: positive_i32(columns[5], "transaction timeout")?,
        transaction_start_time_ms: start,
        topics: parse_topics(columns.get(8).copied().unwrap_or_default())?,
    };
    Ok(BrokerStateObservation::Transaction(
        BrokerTransactionState {
            observation,
            operation_id: operation_id.clone(),
            coordinator_id: nonnegative_i32(columns[0], "coordinator ID")?,
            transaction,
        },
    ))
}

fn rows<'a>(stdout: &'a [u8], headers: &[&str], kind: &str) -> Result<Vec<&'a str>, ObserverError> {
    let text = str::from_utf8(stdout).map_err(|_| invalid(format!("{kind} was not UTF-8")))?;
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let Some(header) = lines.next() else {
        return Err(invalid(format!("{kind} omitted its header")));
    };
    if !header.split_whitespace().eq(headers.iter().copied()) {
        return Err(invalid(format!("{kind} had unexpected columns")));
    }
    Ok(lines.collect())
}

fn parse_listing(line: &str) -> Result<BrokerTransactionListing, ObserverError> {
    let columns = line.split_whitespace().collect::<Vec<_>>();
    let [transactional_id, coordinator_id, producer_id, state] = columns.as_slice() else {
        return Err(invalid(
            "transaction listing row did not contain four columns",
        ));
    };
    Ok(BrokerTransactionListing {
        coordinator_id: nonnegative_i32(coordinator_id, "coordinator ID")?,
        transaction: TransactionListingSnapshot {
            transactional_id: nonempty(transactional_id, "transactional ID")?,
            producer_id: nonnegative_i64(producer_id, "producer ID")?,
            transaction_state: nonempty(state, "transaction state")?,
        },
    })
}

fn parse_topics(value: &str) -> Result<Vec<TransactionTopicSnapshot>, ObserverError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut topics = BTreeMap::<String, Vec<i32>>::new();
    for item in value.split(',') {
        let Some((topic, partition)) = item.rsplit_once('-') else {
            return Err(invalid("transaction topic-partition omitted its separator"));
        };
        if topic.is_empty() {
            return Err(invalid("transaction topic was empty"));
        }
        let partition = nonnegative_i32(partition, "transaction partition")?;
        let partitions = topics.entry(topic.to_owned()).or_default();
        if partitions.contains(&partition) {
            return Err(invalid("transaction topic-partition was repeated"));
        }
        partitions.push(partition);
    }
    Ok(topics
        .into_iter()
        .map(|(topic, mut partitions)| {
            partitions.sort_unstable();
            TransactionTopicSnapshot { topic, partitions }
        })
        .collect())
}

fn optional_i64(value: &str, field: &str) -> Result<Option<i64>, ObserverError> {
    if value == "None" {
        Ok(None)
    } else {
        nonnegative_i64(value, field).map(Some)
    }
}

fn nonempty(value: &str, field: &str) -> Result<String, ObserverError> {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        Err(invalid(format!(
            "{field} was empty or contained whitespace"
        )))
    } else {
        Ok(value.to_owned())
    }
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

fn nonnegative_i16(value: &str, field: &str) -> Result<i16, ObserverError> {
    value
        .parse::<i16>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| invalid(format!("{field} was not a nonnegative i16")))
}

fn positive_i32(value: &str, field: &str) -> Result<i32, ObserverError> {
    value
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid(format!("{field} was not a positive i32")))
}

fn invalid(message: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "Kafka CLI transaction snapshot: {}",
        message.into()
    ))
}

#[cfg(test)]
#[path = "transaction_cli_observation_test.rs"]
mod tests;
