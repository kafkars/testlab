//! Topic-admin actions produce exact metadata observation targets.

use testlab_schema::{
    AdapterCommand, CreatePartitionsCommand, CreateTopicCommand, DeleteTopicCommand,
    DescribeTopicCommand, ListOffsetsCommand, ListTopicsCommand, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, ListTarget, TargetMatch, TopicTarget, unique};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let matched = match action {
        ScenarioAction::CreateTopic(action) => (
            AdapterCommand::CreateTopic(CreateTopicCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partitions: action.partitions,
                replication_factor: action.replication_factor,
                validate_only: action.validate_only,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::Topic(create_topic_target(action)?),
        ),
        ScenarioAction::CreatePartitions(action) => (
            AdapterCommand::CreatePartitions(CreatePartitionsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                total_count: action.total_count,
                validate_only: action.validate_only,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::Topic(TopicTarget {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                expected_partitions: expected_partition_topology(action)?,
                expected_exists: action.expected_error_code.is_none(),
                poll_expected: action.expected_error_code.is_none() && !action.validate_only,
            }),
        ),
        ScenarioAction::DeleteTopic(action) => (
            AdapterCommand::DeleteTopic(DeleteTopicCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::Topic(TopicTarget {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                expected_partitions: None,
                expected_exists: false,
                poll_expected: action.expected_error_code.is_none(),
            }),
        ),
        ScenarioAction::DescribeTopic(action) => {
            if let Some(expected) = action.expected_partitions.as_deref() {
                unique(expected, &action.operation_id, "partitions")?;
            }
            (
                AdapterCommand::DescribeTopic(DescribeTopicCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    topic: action.topic.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::Topic(TopicTarget {
                    operation_id: action.operation_id.clone(),
                    topic: action.topic.clone(),
                    expected_partitions: action.expected_partitions.clone(),
                    expected_exists: action.expected_error_code.is_none(),
                    poll_expected: false,
                }),
            )
        }
        ScenarioAction::ListOffsets(action) if action.expected_error_code.is_some() => {
            missing_partition_target(action)?
        }
        ScenarioAction::ListTopics(action) => {
            unique(&action.required_topics, &action.operation_id, "topics")?;
            (
                AdapterCommand::ListTopics(ListTopicsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    include_internal: action.include_internal,
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::Topics(ListTarget {
                    operation_id: action.operation_id.clone(),
                    names: action.required_topics.clone(),
                }),
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(matched))
}

fn create_topic_target(
    action: &testlab_schema::CreateTopicAction,
) -> Result<TopicTarget, ObserverError> {
    let authorization_denied = action.expected_error_code.as_deref()
        == Some(testlab_schema::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE);
    Ok(TopicTarget {
        operation_id: action.operation_id.clone(),
        topic: action.topic.clone(),
        expected_partitions: create_expected_partitions(action)?,
        expected_exists: !action.validate_only && !authorization_denied,
        poll_expected: !action.validate_only && !authorization_denied,
    })
}

fn create_expected_partitions(
    action: &testlab_schema::CreateTopicAction,
) -> Result<Option<Vec<i32>>, ObserverError> {
    if action.expected_error_code.as_deref()
        == Some(testlab_schema::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE)
    {
        Ok(None)
    } else if action.validate_only {
        Ok(Some(Vec::new()))
    } else {
        Ok(Some(partitions(action.partitions)?))
    }
}

fn expected_partition_topology(
    action: &testlab_schema::CreatePartitionsAction,
) -> Result<Option<Vec<i32>>, ObserverError> {
    if action.expected_error_code.is_some() {
        return Ok(None);
    }
    let count = if action.validate_only {
        action.expected_current_count.ok_or_else(|| {
            ObserverError::InvalidTarget(format!(
                "validate-only admin operation {} omitted expected current partition count",
                action.operation_id
            ))
        })?
    } else {
        action.total_count
    };
    Ok(Some(partitions(count)?))
}

fn missing_partition_target(
    action: &testlab_schema::ListOffsetsAction,
) -> Result<TargetMatch, ObserverError> {
    Ok((
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
            position: action.position,
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::Topic(TopicTarget {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            expected_partitions: Some(partitions(action.partition)?),
            expected_exists: true,
            poll_expected: false,
        }),
    ))
}

fn partitions(count: i32) -> Result<Vec<i32>, ObserverError> {
    if count <= 0 {
        return Err(ObserverError::InvalidTarget(
            "admin partition count must be positive".to_owned(),
        ));
    }
    Ok((0..count).collect())
}
