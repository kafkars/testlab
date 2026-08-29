//! Plural admin normalization reconstructs public results in caller order.

use crate::kafkars_api::{KafkaError, TopicPartition};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::normalize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResourceResult<T> {
    Success(T),
    Failure(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PartitionResult<T> {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) result: ResourceResult<T>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupResult<T> {
    pub(crate) group_id: String,
    pub(crate) result: ResourceResult<T>,
}

pub(crate) fn ordered_partition_results<T>(
    mut entries: Vec<(TopicPartition, Result<T, KafkaError>)>,
    requested: &[(String, i32)],
    operation_id: &OperationId,
    resource: &str,
) -> Result<Vec<PartitionResult<T>>, AdapterError> {
    validate_requested_partitions(requested, operation_id, resource)?;
    validate_public_partitions(&entries, operation_id, resource)?;
    if entries.len() != requested.len() {
        return Err(invalid_result(
            operation_id,
            &format!("returned a different number of {resource} outcomes than requested"),
        ));
    }
    let mut ordered = Vec::with_capacity(requested.len());
    for (topic, partition) in requested {
        let Some(index) = entries
            .iter()
            .position(|(key, _)| key.topic() == topic && key.partition() == *partition)
        else {
            return Err(invalid_result(
                operation_id,
                &format!("returned mismatched {resource} identities"),
            ));
        };
        let (key, result) = entries.remove(index);
        ordered.push(PartitionResult {
            topic: key.topic().to_owned(),
            partition: key.partition(),
            result: normalize_result(result),
        });
    }
    Ok(ordered)
}

pub(crate) fn ordered_group_results<T>(
    mut entries: Vec<(String, Result<T, KafkaError>)>,
    requested: &[String],
    operation_id: &OperationId,
    resource: &str,
) -> Result<Vec<GroupResult<T>>, AdapterError> {
    validate_unique_strings(requested, operation_id, resource, "requested")?;
    let returned = entries
        .iter()
        .map(|(group_id, _)| group_id.clone())
        .collect::<Vec<_>>();
    validate_unique_strings(&returned, operation_id, resource, "returned")?;
    if entries.len() != requested.len() {
        return Err(invalid_result(
            operation_id,
            &format!("returned a different number of {resource} outcomes than requested"),
        ));
    }
    let mut ordered = Vec::with_capacity(requested.len());
    for expected_group in requested {
        let Some(index) = entries
            .iter()
            .position(|(group_id, _)| group_id == expected_group)
        else {
            return Err(invalid_result(
                operation_id,
                &format!("returned mismatched {resource} identities"),
            ));
        };
        let (group_id, result) = entries.remove(index);
        ordered.push(GroupResult {
            group_id,
            result: normalize_result(result),
        });
    }
    Ok(ordered)
}

fn normalize_result<T>(result: Result<T, KafkaError>) -> ResourceResult<T> {
    match result {
        Ok(value) => ResourceResult::Success(value),
        Err(error) => ResourceResult::Failure(normalize::error_code(&error)),
    }
}

fn validate_requested_partitions(
    requested: &[(String, i32)],
    operation_id: &OperationId,
    resource: &str,
) -> Result<(), AdapterError> {
    if requested
        .iter()
        .enumerate()
        .any(|(index, identity)| requested[..index].iter().any(|prior| prior == identity))
    {
        return Err(invalid_result(
            operation_id,
            &format!("received duplicate requested {resource} identities"),
        ));
    }
    Ok(())
}

fn validate_public_partitions<T>(
    entries: &[(TopicPartition, Result<T, KafkaError>)],
    operation_id: &OperationId,
    resource: &str,
) -> Result<(), AdapterError> {
    for (index, (key, _)) in entries.iter().enumerate() {
        if key.start_position().is_some() {
            return Err(invalid_result(
                operation_id,
                &format!("returned a positioned {resource} identity"),
            ));
        }
        if entries[..index]
            .iter()
            .any(|(prior, _)| prior.topic() == key.topic() && prior.partition() == key.partition())
        {
            return Err(invalid_result(
                operation_id,
                &format!("returned duplicate {resource} identities"),
            ));
        }
    }
    Ok(())
}

fn validate_unique_strings(
    values: &[String],
    operation_id: &OperationId,
    resource: &str,
    origin: &str,
) -> Result<(), AdapterError> {
    if values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
    {
        return Err(invalid_result(
            operation_id,
            &format!("{origin} duplicate {resource} identities"),
        ));
    }
    Ok(())
}

fn invalid_result(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
