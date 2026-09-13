//! All-topic normalization retains complete public outcomes in canonical name order.

use testlab_schema::{
    AdminListedTopicOutcome, AdminTopicDescriptionValue, AdminTopicPartitionDescriptionOutcome,
    OperationId,
};

use crate::kafkars_api::{KafkaError, TopicDescription};
use crate::protocol_admin_result::invalid_result;
use crate::{AdapterError, normalize};

#[derive(Debug)]
pub(crate) struct ListedTopicResult {
    pub(crate) name: String,
    pub(crate) topic_id: Option<[u8; 16]>,
    pub(crate) internal: bool,
    pub(crate) authorized_operations: Option<i32>,
    pub(crate) partitions: Vec<AdminTopicPartitionDescriptionOutcome>,
}

impl From<TopicDescription> for ListedTopicResult {
    fn from(description: TopicDescription) -> Self {
        Self {
            name: description.name().to_owned(),
            topic_id: description.topic_id(),
            internal: description.is_internal(),
            authorized_operations: description.authorized_operations(),
            partitions: description
                .partitions()
                .iter()
                .map(crate::protocol_admin_topic_partition::metadata)
                .collect(),
        }
    }
}

pub(crate) fn listed_topics(
    entries: Vec<(String, Result<ListedTopicResult, KafkaError>)>,
    operation_id: &OperationId,
) -> Result<Vec<AdminListedTopicOutcome>, AdapterError> {
    let mut outcomes = entries
        .into_iter()
        .map(|(key, result)| outcome(key, result, operation_id))
        .collect::<Result<Vec<_>, _>>()?;
    outcomes.sort_by(|left, right| left.topic.as_bytes().cmp(right.topic.as_bytes()));
    if outcomes
        .windows(2)
        .any(|pair| pair[0].topic == pair[1].topic)
    {
        return Err(invalid_result(
            operation_id,
            "returned duplicate topic listing identities",
        ));
    }
    Ok(outcomes)
}

fn outcome(
    key: String,
    result: Result<ListedTopicResult, KafkaError>,
    operation_id: &OperationId,
) -> Result<AdminListedTopicOutcome, AdapterError> {
    let description = match result {
        Ok(description) => description,
        Err(error) => {
            return Ok(AdminListedTopicOutcome {
                topic: key,
                description: None,
                error_code: Some(normalize::error_code(&error)),
            });
        }
    };
    if key != description.name {
        return Err(invalid_result(operation_id, "listed topic key mismatch"));
    }
    let partitions = partitions(description.partitions, operation_id)?;
    Ok(AdminListedTopicOutcome {
        topic: key,
        description: Some(AdminTopicDescriptionValue {
            topic_id: description.topic_id,
            internal: description.internal,
            authorized_operations: description.authorized_operations,
            partitions,
        }),
        error_code: None,
    })
}

fn partitions(
    mut values: Vec<AdminTopicPartitionDescriptionOutcome>,
    operation_id: &OperationId,
) -> Result<Vec<AdminTopicPartitionDescriptionOutcome>, AdapterError> {
    values.sort_by_key(|value| value.partition);
    if values.iter().any(|value| value.partition < 0)
        || values
            .windows(2)
            .any(|pair| pair[0].partition == pair[1].partition)
    {
        return Err(invalid_result(
            operation_id,
            "invalid listed topic partitions",
        ));
    }
    Ok(values)
}
