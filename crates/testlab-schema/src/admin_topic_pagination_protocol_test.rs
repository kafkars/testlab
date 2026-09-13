//! Pagination protocol tests separate public controls from verifier expectations.

use crate::{
    AdapterCommand, ClientId, DescribeTopicAction, DescribeTopicCommand, OperationId,
    ScenarioAction, TopicDescriptionApi, TopicDescriptionPagination,
};

#[test]
fn describe_topic_command_excludes_page_expectations() {
    let pagination = TopicDescriptionPagination {
        response_partition_limit: 1,
        follow_cursors: true,
    };
    let action = ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation(),
        topic: "records".to_owned(),
        api: TopicDescriptionApi::DescribeTopicPartitions,
        pagination: Some(pagination),
        expected_partitions: Some(vec![0, 1]),
        expected_page_partitions: Some(vec![vec![0], vec![1]]),
        expected_error_code: None,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeTopic(DescribeTopicCommand {
        client_id: client(),
        operation_id: operation(),
        topic: "records".to_owned(),
        api: TopicDescriptionApi::DescribeTopicPartitions,
        pagination: Some(pagination),
        timeout_ms: 1_000,
    });

    let action = encode(&action);
    let command = encode(&command);

    assert!(action.contains("expected_partitions = [0, 1]"));
    assert!(action.contains("expected_page_partitions = [[0], [1]]"));
    assert!(command.contains("api = \"describe_topic_partitions\""));
    assert!(command.contains("response_partition_limit = 1"));
    assert!(command.contains("follow_cursors = true"));
    assert!(!command.contains("expected_partitions"));
    assert!(!command.contains("expected_page_partitions"));
}

fn encode<T>(value: &T) -> String
where
    T: serde::Serialize,
{
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize pagination: {error}"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-describe-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
