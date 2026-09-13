//! Read-only admin wire tests separate verifier expectations from public facts.

use super::{
    AdapterCommand, AdapterEvent, AdminOffsetListing, AdminOffsetSelector, AdminTopicDescription,
    AdminTopicsListing, ClientId, DescribeTopicAction, DescribeTopicCommand, ListOffsetsAction,
    ListOffsetsCommand, ListTopicsAction, ListTopicsCommand, OperationId, PROTOCOL_VERSION,
    ROUTING_ERROR_CODE, SCENARIO_SCHEMA_VERSION, ScenarioAction, TopicDescriptionApi,
    UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

#[test]
fn admin_query_versions_are_exact() {
    assert_eq!(PROTOCOL_VERSION, 117);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 120);
}

#[test]
fn describe_topic_command_excludes_expected_partitions() {
    let action = ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation("admin-describe-1"),
        topic: "records".to_owned(),
        api: TopicDescriptionApi::DescribeTopicPartitions,
        expected_partitions: Some(vec![0, 1]),
        expected_error_code: None,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeTopic(DescribeTopicCommand {
        client_id: client(),
        operation_id: operation("admin-describe-1"),
        topic: "records".to_owned(),
        api: TopicDescriptionApi::DescribeTopicPartitions,
        timeout_ms: 1_000,
    });

    let action = encode_action(&action);
    let command = encode(&command);

    assert!(action.contains("kind = \"describe_topic\""));
    assert!(action.contains("api = \"describe_topic_partitions\""));
    assert!(action.contains("expected_partitions = [0, 1]"));
    assert!(command.contains("kind = \"describe_topic\""));
    assert!(command.contains("api = \"describe_topic_partitions\""));
    assert!(!command.contains("expected_partitions"));
}

#[test]
fn describe_topic_action_defaults_to_metadata_api() {
    let action = toml::from_str::<ScenarioAction>(
        r#"
kind = "describe_topic"
client_id = "client-1"
operation_id = "admin-describe-default"
topic = "records"
expected_partitions = [0]
timeout_ms = 1000
"#,
    )
    .unwrap_or_else(|error| panic!("deserialize metadata-backed description: {error}"));

    assert!(matches!(
        action,
        ScenarioAction::DescribeTopic(DescribeTopicAction {
            api: TopicDescriptionApi::Metadata,
            ..
        })
    ));
}

#[test]
fn list_topics_command_excludes_required_topics() {
    let action = ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id: operation("admin-topics-1"),
        include_internal: false,
        include_authorized_operations: true,
        required_topics: vec!["records".to_owned()],
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::ListTopics(ListTopicsCommand {
        client_id: client(),
        operation_id: operation("admin-topics-1"),
        include_internal: false,
        include_authorized_operations: true,
        timeout_ms: 1_000,
    });

    let action = encode_action(&action);
    let command = encode(&command);

    assert!(action.contains("kind = \"list_topics\""));
    assert!(action.contains("required_topics = [\"records\"]"));
    assert!(command.contains("kind = \"list_topics\""));
    assert!(command.contains("include_internal = false"));
    assert!(command.contains("include_authorized_operations = true"));
    assert!(!command.contains("required_topics"));
}

#[test]
fn list_offsets_command_excludes_expected_offset() {
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client(),
        operation_id: operation("admin-offset-1"),
        topic: "records".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::Latest,
        timestamp_millis: None,
        expected_offset: Some(3),
        expected_error_code: None,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::ListOffsets(ListOffsetsCommand {
        client_id: client(),
        operation_id: operation("admin-offset-1"),
        topic: "records".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::Latest,
        timestamp_millis: None,
        timeout_ms: 1_000,
    });

    let action = encode_action(&action);
    let command = encode(&command);

    assert!(action.contains("kind = \"list_offsets\""));
    assert!(action.contains("expected_offset = 3"));
    assert!(command.contains("kind = \"list_offsets\""));
    assert!(command.contains("position = \"latest\""));
    assert!(!command.contains("timestamp_millis"));
    assert!(!command.contains("expected_offset"));
}

#[test]
fn admin_query_events_report_only_observed_facts() {
    let described = encode(&AdapterEvent::TopicDescribed(AdminTopicDescription {
        operation_id: operation("admin-describe-1"),
        topic: "records".to_owned(),
        partitions: vec![0, 1],
    }));
    let listed = encode(&AdapterEvent::TopicsListed(AdminTopicsListing {
        operation_id: operation("admin-topics-1"),
        outcomes: Vec::new(),
    }));
    let offset = encode(&AdapterEvent::OffsetListed(AdminOffsetListing {
        operation_id: operation("admin-offset-1"),
        topic: "records".to_owned(),
        partition: 0,
        offset: Some(3),
        timestamp_millis: None,
    }));

    assert!(described.contains("kind = \"topic_described\""));
    assert!(described.contains("partitions = [0, 1]"));
    assert!(!described.contains("expected_partitions"));
    assert!(listed.contains("kind = \"topics_listed\""));
    assert!(listed.contains("outcomes = []"));
    assert!(!listed.contains("required_topics"));
    assert!(offset.contains("kind = \"offset_listed\""));
    assert!(offset.contains("offset = 3"));
    assert!(!offset.contains("expected_offset"));
}

#[test]
fn list_offsets_accepts_an_earliest_position() {
    let latest = encode(&AdapterCommand::ListOffsets(ListOffsetsCommand {
        client_id: client(),
        operation_id: operation("admin-offset-1"),
        topic: "records".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::Latest,
        timestamp_millis: None,
        timeout_ms: 1_000,
    }));
    let earliest = latest.replace("position = \"latest\"", "position = \"earliest\"");

    let decoded = toml::from_str::<AdapterCommand>(&earliest)
        .unwrap_or_else(|error| panic!("deserialize earliest offset command: {error}"));
    assert!(matches!(
        decoded,
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            position: AdminOffsetSelector::Earliest,
            ..
        })
    ));
}

#[test]
fn list_offsets_preserves_a_timestamp_selector_and_result() {
    let command = AdapterCommand::ListOffsets(ListOffsetsCommand {
        client_id: client(),
        operation_id: operation("admin-offset-timestamp"),
        topic: "records".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::Timestamp,
        timestamp_millis: Some(1_700_000_000_123),
        timeout_ms: 1_000,
    });
    let encoded = encode(&command);
    let decoded = toml::from_str::<AdapterCommand>(&encoded)
        .unwrap_or_else(|error| panic!("deserialize timestamp offset command: {error}"));
    assert_eq!(decoded, command);
    assert!(encoded.contains("position = \"timestamp\""));
    assert!(encoded.contains("timestamp_millis = 1700000000123"));

    let event = encode(&AdapterEvent::OffsetListed(AdminOffsetListing {
        operation_id: operation("admin-offset-timestamp"),
        topic: "records".to_owned(),
        partition: 0,
        offset: Some(1),
        timestamp_millis: Some(1_700_000_000_123),
    }));
    assert!(event.contains("timestamp_millis = 1700000000123"));
}

#[test]
fn list_offsets_preserves_a_max_timestamp_selector() {
    let command = AdapterCommand::ListOffsets(ListOffsetsCommand {
        client_id: client(),
        operation_id: operation("admin-offset-max-timestamp"),
        topic: "records".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::MaxTimestamp,
        timestamp_millis: None,
        timeout_ms: 1_000,
    });
    let encoded = encode(&command);
    assert_eq!(
        toml::from_str::<AdapterCommand>(&encoded)
            .unwrap_or_else(|error| panic!("deserialize max timestamp command: {error}")),
        command
    );
    assert!(encoded.contains("position = \"max_timestamp\""));
    assert!(!encoded.contains("timestamp_millis"));
}

#[test]
fn query_error_expectations_do_not_cross_the_wire_boundary() {
    let described = ScenarioAction::DescribeTopic(DescribeTopicAction {
        client_id: client(),
        operation_id: operation("admin-describe-missing"),
        topic: "missing".to_owned(),
        api: TopicDescriptionApi::Metadata,
        expected_partitions: None,
        expected_error_code: Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE.to_owned()),
        timeout_ms: 1_000,
    });
    let offset = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client(),
        operation_id: operation("admin-offset-missing"),
        topic: "records".to_owned(),
        partition: 1,
        position: AdminOffsetSelector::Latest,
        timestamp_millis: None,
        expected_offset: None,
        expected_error_code: Some(ROUTING_ERROR_CODE.to_owned()),
        timeout_ms: 1_000,
    });
    let describe_command = AdapterCommand::DescribeTopic(DescribeTopicCommand {
        client_id: client(),
        operation_id: operation("admin-describe-missing"),
        topic: "missing".to_owned(),
        api: TopicDescriptionApi::Metadata,
        timeout_ms: 1_000,
    });
    let offset_command = AdapterCommand::ListOffsets(ListOffsetsCommand {
        client_id: client(),
        operation_id: operation("admin-offset-missing"),
        topic: "records".to_owned(),
        partition: 1,
        position: AdminOffsetSelector::Latest,
        timestamp_millis: None,
        timeout_ms: 1_000,
    });

    assert!(encode_action(&described).contains("expected_error_code = \"broker:broker_3\""));
    assert!(encode_action(&offset).contains("expected_error_code = \"routing\""));
    assert!(!encode(&describe_command).contains("expected_error_code"));
    assert!(!encode(&offset_command).contains("expected_error_code"));
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
