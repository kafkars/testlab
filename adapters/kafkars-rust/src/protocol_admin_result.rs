//! Admin result normalization rejects malformed public batch identities.

use kafkars::{KafkaError, TopicDescription, TopicPartition};
use testlab_schema::OperationId;

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
    description
        .partitions
        .into_iter()
        .map(|(partition, error)| match error {
            Some(error) => Err(AdapterError::Client(error)),
            None => Ok(partition),
        })
        .collect()
}

pub(crate) fn listed_topics(
    entries: Vec<(String, Result<String, KafkaError>)>,
    operation_id: &OperationId,
) -> Result<Vec<String>, AdapterError> {
    entries
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
        .collect()
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

fn take_single_result<K, V, F>(
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
