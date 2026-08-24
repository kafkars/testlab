//! Read-only admin wire tests separate verifier expectations from public facts.

use super::{
    AdapterCommand, AdapterEvent, AdminOffsetPosition, ClientId, DescribeTopicAction,
    ListOffsetsAction, ListTopicsAction, OperationId, PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION,
    ScenarioAction,
};

#[test]
fn admin_query_versions_are_exact() {
    assert_eq!(PROTOCOL_VERSION, 15);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 13);
}

#[test]
fn describe_topic_command_excludes_expected_partitions() {
    let action = ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation("admin-describe-1"),
        topic: "records".to_owned(),
        expected_partitions: vec![0, 1],
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeTopic {
        client_id: client(),
        operation_id: operation("admin-describe-1"),
        topic: "records".to_owned(),
        timeout_ms: 1_000,
    };

    let action = encode_action(&action);
    let command = encode(&command);

    assert!(action.contains("kind = \"describe_topic\""));
    assert!(action.contains("expected_partitions = [0, 1]"));
    assert!(command.contains("kind = \"describe_topic\""));
    assert!(!command.contains("expected_partitions"));
}

#[test]
fn list_topics_command_excludes_required_topics() {
    let action = ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id: operation("admin-topics-1"),
        include_internal: false,
        required_topics: vec!["records".to_owned()],
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::ListTopics {
        client_id: client(),
        operation_id: operation("admin-topics-1"),
        include_internal: false,
        timeout_ms: 1_000,
    };

    let action = encode_action(&action);
    let command = encode(&command);

    assert!(action.contains("kind = \"list_topics\""));
    assert!(action.contains("required_topics = [\"records\"]"));
    assert!(command.contains("kind = \"list_topics\""));
    assert!(command.contains("include_internal = false"));
    assert!(!command.contains("required_topics"));
}

#[test]
fn list_offsets_command_excludes_expected_offset() {
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client(),
        operation_id: operation("admin-offset-1"),
        topic: "records".to_owned(),
        partition: 0,
        position: AdminOffsetPosition::Latest,
        expected_offset: 3,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::ListOffsets {
        client_id: client(),
        operation_id: operation("admin-offset-1"),
        topic: "records".to_owned(),
        partition: 0,
        position: AdminOffsetPosition::Latest,
        timeout_ms: 1_000,
    };

    let action = encode_action(&action);
    let command = encode(&command);

    assert!(action.contains("kind = \"list_offsets\""));
    assert!(action.contains("expected_offset = 3"));
    assert!(command.contains("kind = \"list_offsets\""));
    assert!(command.contains("position = \"latest\""));
    assert!(!command.contains("expected_offset"));
}

#[test]
fn admin_query_events_report_only_observed_facts() {
    let described = encode(&AdapterEvent::TopicDescribed {
        operation_id: operation("admin-describe-1"),
        topic: "records".to_owned(),
        partitions: vec![0, 1],
    });
    let listed = encode(&AdapterEvent::TopicsListed {
        operation_id: operation("admin-topics-1"),
        topics: vec!["records".to_owned()],
    });
    let offset = encode(&AdapterEvent::OffsetListed {
        operation_id: operation("admin-offset-1"),
        topic: "records".to_owned(),
        partition: 0,
        offset: Some(3),
    });

    assert!(described.contains("kind = \"topic_described\""));
    assert!(described.contains("partitions = [0, 1]"));
    assert!(!described.contains("expected_partitions"));
    assert!(listed.contains("kind = \"topics_listed\""));
    assert!(listed.contains("topics = [\"records\"]"));
    assert!(!listed.contains("required_topics"));
    assert!(offset.contains("kind = \"offset_listed\""));
    assert!(offset.contains("offset = 3"));
    assert!(!offset.contains("expected_offset"));
}

#[test]
fn list_offsets_rejects_an_earliest_position() {
    let latest = encode(&AdapterCommand::ListOffsets {
        client_id: client(),
        operation_id: operation("admin-offset-1"),
        topic: "records".to_owned(),
        partition: 0,
        position: AdminOffsetPosition::Latest,
        timeout_ms: 1_000,
    });
    let earliest = latest.replace("position = \"latest\"", "position = \"earliest\"");

    assert!(toml::from_str::<AdapterCommand>(&earliest).is_err());
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize admin schema: {error}"))
}

fn encode_action(action: &ScenarioAction) -> String {
    let encoded = encode(action);
    let decoded = toml::from_str::<ScenarioAction>(&encoded)
        .unwrap_or_else(|error| panic!("deserialize admin action: {error}"));
    assert_eq!(decoded, *action);
    encoded
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
