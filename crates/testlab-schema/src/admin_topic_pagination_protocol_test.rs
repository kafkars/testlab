//! Pagination protocol tests separate public controls from verifier expectations.

use crate::{
    AdapterCommand, AdapterEvent, AdminOffsetListing, AdminTopicDescription,
    AdminTopicDescriptionPage, AdminTopicPageCursor, AdminTopicPartitionDescriptionOutcome,
    AdminTopicsListing, ClientId, DescribeTopicAction, DescribeTopicCommand, OperationId,
    ScenarioAction, TopicDescriptionApi, TopicDescriptionPagination,
};

#[test]
fn admin_query_events_report_only_observed_facts() {
    let described = encode(&AdapterEvent::TopicDescribed(AdminTopicDescription {
        operation_id: operation(),
        topic: "records".to_owned(),
        partitions: vec![0, 1],
        partition_details: vec![partition(0), partition(1)],
        pages: vec![
            AdminTopicDescriptionPage {
                partitions: vec![0],
                partition_details: vec![partition(0)],
                next_cursor: Some(AdminTopicPageCursor {
                    topic_name: "records".to_owned(),
                    partition_index: 1,
                }),
            },
            AdminTopicDescriptionPage {
                partitions: vec![1],
                partition_details: vec![partition(1)],
                next_cursor: None,
            },
        ],
    }));
    let listed = encode(&AdapterEvent::TopicsListed(AdminTopicsListing {
        operation_id: OperationId::new("admin-topics-1")
            .unwrap_or_else(|error| panic!("operation id: {error}")),
        outcomes: Vec::new(),
    }));
    let offset = encode(&AdapterEvent::OffsetListed(AdminOffsetListing {
        operation_id: OperationId::new("admin-offset-1")
            .unwrap_or_else(|error| panic!("operation id: {error}")),
        topic: "records".to_owned(),
        partition: 0,
        offset: Some(3),
        timestamp_millis: None,
    }));

    assert!(described.contains("kind = \"topic_described\""));
    assert!(described.contains("partitions = [0, 1]"));
    assert!(described.contains("partition_index = 1"));
    assert!(described.contains("leader_id = 1"));
    assert!(described.contains("leader_epoch = 1"));
    assert!(described.contains("replicas = [1]"));
    assert!(described.contains("in_sync_replicas = [1]"));
    assert!(described.contains("eligible_leader_replicas = [1]"));
    assert!(described.contains("last_known_eligible_leader_replicas = [1]"));
    assert!(described.contains("offline_replicas = []"));
    assert!(!described.contains("expected_partitions"));
    assert!(listed.contains("kind = \"topics_listed\""));
    assert!(listed.contains("outcomes = []"));
    assert!(!listed.contains("expected_topics"));
    assert!(offset.contains("kind = \"offset_listed\""));
    assert!(offset.contains("offset = 3"));
    assert!(!offset.contains("expected_offset"));
}

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

fn partition(partition: i32) -> AdminTopicPartitionDescriptionOutcome {
    AdminTopicPartitionDescriptionOutcome {
        partition,
        error_code: None,
        leader_id: Some(1),
        leader_epoch: Some(1),
        replicas: vec![1],
        in_sync_replicas: vec![1],
        eligible_leader_replicas: Some(vec![1]),
        last_known_eligible_leader_replicas: Some(vec![1]),
        offline_replicas: Vec::new(),
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-describe-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
