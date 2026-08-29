//! Admin result normalization rejects malformed public batch identities.

use crate::kafkars_api::{KafkaError, TopicDescription, TopicPartition};
use testlab_schema::{AdminBrokerError, OperationId};

use crate::AdapterError;

#[derive(Debug)]
pub(crate) struct DescribedTopicResult {
    pub(crate) name: String,
    pub(crate) partitions: Vec<(i32, Option<KafkaError>)>,
}

impl From<TopicDescription> for DescribedTopicResult {
    fn from(description: TopicDescription) -> Self {
        Self {
            name: description.name().to_owned(),
            partitions: description
                .partitions()
                .iter()
                .map(|partition| (partition.partition_index(), partition.error().cloned()))
                .collect(),
        }
    }
}

pub(crate) fn validate_single_topic_result(
    entries: Vec<(String, Result<(), KafkaError>)>,
    operation_id: &OperationId,
    expected_topic: &str,
) -> Result<(), AdapterError> {
    take_single_result(
        entries,
        operation_id,
        |topic| topic == expected_topic,
        "topic",
    )
}

pub(crate) fn described_partitions(
    entries: Vec<(String, Result<DescribedTopicResult, KafkaError>)>,
    operation_id: &OperationId,
    expected_topic: &str,
) -> Result<Vec<i32>, AdapterError> {
    let description = take_single_result(
        entries,
        operation_id,
        |topic| topic == expected_topic,
        "topic description",
    )?;
    if description.name != expected_topic {
        return Err(invalid_result(
            operation_id,
            "topic description name did not match its request",
        ));
    }
    let partitions = description
        .partitions
        .into_iter()
        .map(|(partition, error)| match error {
            Some(error) => Err(AdapterError::Client(error)),
            None => Ok(partition),
        })
        .collect::<Result<Vec<_>, _>>()?;
    sorted_unique_nonnegative(partitions, operation_id, "topic partitions")
}

pub(crate) fn listed_topics(
    entries: Vec<(String, Result<String, KafkaError>)>,
    operation_id: &OperationId,
) -> Result<Vec<String>, AdapterError> {
    let topics = entries
        .into_iter()
        .map(|(key, result)| {
            let name = result.map_err(AdapterError::Client)?;
            if key != name {
                return Err(invalid_result(
                    operation_id,
                    "listed topic key did not match its reported name",
                ));
            }
            Ok(name)
        })
        .collect::<Result<Vec<_>, _>>()?;
    sorted_unique_strings(topics, operation_id, "topic listing")
}

pub(crate) fn sorted_unique_strings(
    mut values: Vec<String>,
    operation_id: &OperationId,
    resource: &str,
) -> Result<Vec<String>, AdapterError> {
    values.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid_result(
            operation_id,
            &format!("returned duplicate {resource} identities"),
        ));
    }
    Ok(values)
}

pub(crate) fn sorted_unique_nonnegative(
    mut values: Vec<i32>,
    operation_id: &OperationId,
    resource: &str,
) -> Result<Vec<i32>, AdapterError> {
    if values.iter().any(|value| *value < 0) {
        return Err(invalid_result(
            operation_id,
            &format!("returned a negative {resource} identity"),
        ));
    }
    values.sort_unstable();
    if values.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid_result(
            operation_id,
            &format!("returned duplicate {resource} identities"),
        ));
    }
    Ok(values)
}

pub(crate) fn sorted_unique_broker_errors(
    mut errors: Vec<AdminBrokerError>,
    operation_id: &OperationId,
) -> Result<Vec<AdminBrokerError>, AdapterError> {
    if errors.iter().any(|error| error.broker_id < 0) {
        return Err(invalid_result(
            operation_id,
            "returned an error for a negative broker identity",
        ));
    }
    errors.sort_by_key(|error| (error.broker_id, error.code));
    if errors
        .windows(2)
        .any(|pair| pair[0].broker_id == pair[1].broker_id)
    {
        return Err(invalid_result(
            operation_id,
            "returned duplicate broker-error identities",
        ));
    }
    Ok(errors)
}

pub(crate) fn listed_offset(
    entries: Vec<(TopicPartition, Result<Option<i64>, KafkaError>)>,
    operation_id: &OperationId,
    expected_topic: &str,
    expected_partition: i32,
) -> Result<Option<i64>, AdapterError> {
    take_single_result(
        entries,
        operation_id,
        |topic_partition| {
            topic_partition.topic() == expected_topic
                && topic_partition.partition() == expected_partition
                && topic_partition.start_position().is_none()
        },
        "topic-partition offset",
    )
}

pub(crate) fn listed_consumer_group_offset(
    entries: Vec<(TopicPartition, Result<Option<i64>, KafkaError>)>,
    operation_id: &OperationId,
    expected_topic: &str,
    expected_partition: i32,
) -> Result<Option<i64>, AdapterError> {
    take_single_result(
        entries,
        operation_id,
        |topic_partition| {
            topic_partition.topic() == expected_topic
                && topic_partition.partition() == expected_partition
                && topic_partition.start_position().is_none()
        },
        "consumer-group topic-partition offset",
    )
}

pub(crate) fn deleted_records_low_watermark(
    entries: Vec<(TopicPartition, Result<i64, KafkaError>)>,
    operation_id: &OperationId,
    expected_topic: &str,
    expected_partition: i32,
) -> Result<i64, AdapterError> {
    take_single_result(
        entries,
        operation_id,
        |topic_partition| {
            topic_partition.topic() == expected_topic
                && topic_partition.partition() == expected_partition
                && topic_partition.start_position().is_none()
        },
        "record-deletion topic-partition",
    )
}

pub(crate) fn take_single_result<K, V, F>(
    entries: Vec<(K, Result<V, KafkaError>)>,
    operation_id: &OperationId,
    key_matches: F,
    resource: &str,
) -> Result<V, AdapterError>
where
    F: FnOnce(&K) -> bool,
{
    let mut entries = entries.into_iter();
    let Some((key, result)) = entries.next() else {
        return Err(invalid_result(
            operation_id,
            &format!("returned no {resource} result"),
        ));
    };
    if entries.next().is_some() || !key_matches(&key) {
        return Err(invalid_result(
            operation_id,
            &format!("returned an unexpected {resource} result"),
        ));
    }
    result.map_err(AdapterError::Client)
}

fn invalid_result(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
