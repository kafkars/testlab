//! Protocol v18 admin tests keep declarative expectations outside wire payloads.

use super::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupDescription, AdminConsumerGroupsListing,
    AlterConsumerGroupOffsetCommand, ClientId, DeleteConsumerGroupCommand,
    DeleteConsumerGroupOffsetCommand, DeleteTopicAction, DeleteTopicCommand,
    DescribeClusterCommand, DescribeConsumerGroupAction, DescribeConsumerGroupCommand,
    ListConsumerGroupsAction, ListConsumerGroupsCommand, OperationId, ScenarioAction,
    UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

#[test]
fn group_list_expectations_do_not_cross_the_wire_boundary() {
    let action = ScenarioAction::ListConsumerGroups(ListConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("admin-groups-list"),
        required_group_ids: vec!["group-1".to_owned()],
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::ListConsumerGroups(ListConsumerGroupsCommand {
        client_id: client(),
        operation_id: operation("admin-groups-list"),
        timeout_ms: 1_000,
    });

    let action = encode(&action);
    let command = encode(&command);

    assert!(action.contains("required_group_ids = [\"group-1\"]"));
    assert!(!command.contains("required_group_ids"));
    assert_round_trip::<AdapterCommand>(&command);
}

#[test]
fn group_description_expectations_do_not_cross_the_wire_boundary() {
    let action = ScenarioAction::DescribeConsumerGroup(DescribeConsumerGroupAction {
        client_id: client(),
        operation_id: operation("admin-group-describe"),
        group_id: "group-1".to_owned(),
        expected_member_count: 2,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeConsumerGroup(DescribeConsumerGroupCommand {
        client_id: client(),
        operation_id: operation("admin-group-describe"),
        group_id: "group-1".to_owned(),
        timeout_ms: 1_000,
    });

    let action = encode(&action);
    let command = encode(&command);

    assert!(action.contains("expected_member_count = 2"));
    assert!(!command.contains("expected_member_count"));
    assert_round_trip::<AdapterCommand>(&command);
}

#[test]
fn admin_commands_have_exact_v18_kinds() {
    let commands = [
        (
            "delete_topic",
            AdapterCommand::DeleteTopic(DeleteTopicCommand {
                client_id: client(),
                operation_id: operation("admin-topic-delete"),
                topic: "records".to_owned(),
                timeout_ms: 1_000,
            }),
        ),
        (
            "describe_cluster",
            AdapterCommand::DescribeCluster(DescribeClusterCommand {
                client_id: client(),
                operation_id: operation("admin-cluster-describe"),
                timeout_ms: 1_000,
            }),
        ),
        (
            "list_consumer_groups",
            AdapterCommand::ListConsumerGroups(ListConsumerGroupsCommand {
                client_id: client(),
                operation_id: operation("admin-groups-list"),
                timeout_ms: 1_000,
            }),
        ),
        (
            "describe_consumer_group",
            AdapterCommand::DescribeConsumerGroup(DescribeConsumerGroupCommand {
                client_id: client(),
                operation_id: operation("admin-group-describe"),
                group_id: "group-1".to_owned(),
                timeout_ms: 1_000,
            }),
        ),
        (
            "alter_consumer_group_offset",
            AdapterCommand::AlterConsumerGroupOffset(AlterConsumerGroupOffsetCommand {
                client_id: client(),
                operation_id: operation("admin-offset-alter"),
                group_id: "group-1".to_owned(),
                topic: "records".to_owned(),
                partition: 0,
                offset: 2,
                timeout_ms: 1_000,
            }),
        ),
        (
            "delete_consumer_group_offset",
            AdapterCommand::DeleteConsumerGroupOffset(DeleteConsumerGroupOffsetCommand {
                client_id: client(),
                operation_id: operation("admin-offset-delete"),
                group_id: "group-1".to_owned(),
                topic: "records".to_owned(),
                partition: 0,
                timeout_ms: 1_000,
            }),
        ),
        (
            "delete_consumer_group",
            AdapterCommand::DeleteConsumerGroup(DeleteConsumerGroupCommand {
                client_id: client(),
                operation_id: operation("admin-group-delete"),
                group_id: "group-1".to_owned(),
                timeout_ms: 1_000,
            }),
        ),
    ];

    for (kind, command) in commands {
        let encoded = encode(&command);
        assert!(encoded.contains(&format!("kind = \"{kind}\"")));
        assert_eq!(decode::<AdapterCommand>(&encoded), command);
    }
}

#[test]
fn group_events_report_observed_facts_instead_of_expectations() {
    let listed = encode(&AdapterEvent::ConsumerGroupsListed(
        AdminConsumerGroupsListing {
            operation_id: operation("admin-groups-list"),
            group_ids: vec!["group-1".to_owned()],
            broker_errors: Vec::new(),
        },
    ));
    let described = encode(&AdapterEvent::ConsumerGroupDescribed(
        AdminConsumerGroupDescription {
            operation_id: operation("admin-group-describe"),
            group_id: "group-1".to_owned(),
            member_count: 2,
        },
    ));

    assert!(listed.contains("group_ids = [\"group-1\"]"));
    assert!(!listed.contains("required_group_ids"));
    assert!(described.contains("member_count = 2"));
    assert!(!described.contains("expected_member_count"));
}

#[test]
fn delete_topic_error_expectation_does_not_cross_the_wire_boundary() {
    let action = ScenarioAction::DeleteTopic(DeleteTopicAction {
        client_id: client(),
        operation_id: operation("admin-topic-delete-missing"),
        topic: "missing".to_owned(),
        expected_error_code: Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned()),
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DeleteTopic(DeleteTopicCommand {
        client_id: client(),
        operation_id: operation("admin-topic-delete-missing"),
        topic: "missing".to_owned(),
        timeout_ms: 1_000,
    });

    assert!(encode(&action).contains("expected_error_code = \"broker:broker_3\""));
    assert!(!encode(&command).contains("expected_error_code"));
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize admin value: {error}"))
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> T {
    toml::from_str(value).unwrap_or_else(|error| panic!("deserialize admin value: {error}"))
}

fn assert_round_trip<T>(value: &str)
where
    T: serde::de::DeserializeOwned + serde::Serialize + PartialEq + std::fmt::Debug,
{
    let decoded = decode::<T>(value);
    assert_eq!(encode(&decoded), value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
