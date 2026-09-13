//! Pinned Kafka topic CLI output becomes exact UUID and topology evidence.

use std::collections::BTreeSet;
use std::str;

use testlab_schema::{BrokerStateObservation, BrokerTopicIdentityState, OperationId};

use crate::observer_error::ObserverError;

const TOPIC: &str = "Topic:";
const TOPIC_ID: &str = "TopicId:";
const PARTITION_COUNT: &str = "PartitionCount:";
const PARTITION: &str = "Partition:";

pub(crate) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    expected_topic: &str,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let text = str::from_utf8(stdout)
        .map_err(|_| invalid("topic description output was not valid UTF-8"))?;
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let mut headers = lines
        .iter()
        .copied()
        .filter(|line| column(line, TOPIC_ID).is_ok());
    let Some(header) = headers.next() else {
        return Err(invalid("topic description omitted its identity header"));
    };
    if headers.next().is_some() {
        return Err(invalid("topic description repeated its identity header"));
    }
    let topic = column(header, TOPIC)?;
    if topic != expected_topic {
        return Err(invalid(format!(
            "topic description returned {topic:?}, expected {expected_topic:?}"
        )));
    }
    let topic_id = decode_topic_id(column(header, TOPIC_ID)?)?;
    if topic_id == [0; 16] {
        return Err(invalid("topic description returned the zero topic ID"));
    }
    let count = column(header, PARTITION_COUNT)?
        .parse::<usize>()
        .map_err(|_| invalid("topic description had an invalid partition count"))?;
    let mut partitions = BTreeSet::new();
    for line in lines {
        if line == header {
            continue;
        }
        let row_topic = column(line, TOPIC)?;
        let partition = column(line, PARTITION)?
            .parse::<i32>()
            .map_err(|_| invalid("topic description had an invalid partition index"))?;
        if row_topic != expected_topic || partition < 0 || !partitions.insert(partition) {
            return Err(invalid(
                "topic description had a mismatched, negative, or duplicate partition row",
            ));
        }
    }
    if partitions.len() != count || count == 0 {
        return Err(invalid(
            "topic description partition rows did not match its positive partition count",
        ));
    }
    Ok(BrokerStateObservation::TopicIdentity(
        BrokerTopicIdentityState {
            observation,
            operation_id: operation_id.clone(),
            topic: expected_topic.to_owned(),
            topic_id,
            partitions: partitions.into_iter().collect(),
        },
    ))
}

fn column<'a>(line: &'a str, label: &str) -> Result<&'a str, ObserverError> {
    let mut matches = line
        .split('\t')
        .map(str::trim)
        .filter_map(|value| value.strip_prefix(label).map(str::trim));
    let Some(value) = matches.next().filter(|value| !value.is_empty()) else {
        return Err(invalid(format!("topic description omitted {label}")));
    };
    if matches.next().is_some() {
        return Err(invalid(format!("topic description repeated {label}")));
    }
    Ok(value)
}

fn decode_topic_id(value: &str) -> Result<[u8; 16], ObserverError> {
    if value.len() != 22 || !value.is_ascii() {
        return Err(invalid("topic ID was not 22-character Kafka base64url"));
    }
    let mut output = [0_u8; 16];
    let mut output_index = 0;
    let mut bits = 0_u8;
    let mut buffer = 0_u32;
    for byte in value.bytes() {
        let digit = base64url_digit(byte)
            .ok_or_else(|| invalid("topic ID contained a non-base64url character"))?;
        buffer = (buffer << 6) | u32::from(digit);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            if output_index >= output.len() {
                return Err(invalid("topic ID decoded beyond 16 bytes"));
            }
            output[output_index] = ((buffer >> bits) & 0xff) as u8;
            output_index += 1;
            buffer &= (1_u32 << bits).saturating_sub(1);
        }
    }
    if output_index != output.len() || bits != 4 || buffer != 0 {
        return Err(invalid("topic ID had noncanonical trailing bits"));
    }
    Ok(output)
}

const fn base64url_digit(value: u8) -> Option<u8> {
    match value {
        b'A'..=b'Z' => Some(value - b'A'),
        b'a'..=b'z' => Some(value - b'a' + 26),
        b'0'..=b'9' => Some(value - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

fn invalid(message: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(format!(
        "Kafka CLI topic identity snapshot: {}",
        message.into()
    ))
}

#[cfg(test)]
#[path = "topic_identity_cli_observation_test.rs"]
mod tests;
