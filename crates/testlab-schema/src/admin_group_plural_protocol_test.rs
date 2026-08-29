//! Plural group-admin protocol tests preserve ordered intent and public facts.

use super::{
    AdapterCommand, AdapterEvent, AdminClassicGroupDescriptionOutcome,
    AdminClassicGroupsDescription, AdminConsumerGroupOffsetMutationOutcome,
    AdminConsumerGroupOffsetOutcome, AdminConsumerGroupOffsetsListing,
    AdminConsumerGroupOffsetsMutation, AdminConsumerGroupOffsetsOutcome,
    AdminConsumerGroupsOffsetsListing, AlterConsumerGroupOffsetsCommand, ClassicGroupExpectation,
    ConsumerGroupOffsetAlteration, ConsumerGroupOffsetExpectation, ConsumerGroupOffsetSelection,
    ConsumerGroupOffsetsSelection, DeleteConsumerGroupOffsetsCommand, DescribeClassicGroupsAction,
    DescribeClassicGroupsCommand, EVIDENCE_SCHEMA_VERSION, ListConsumerGroupOffsetsBatchAction,
    ListConsumerGroupOffsetsBatchCommand, ListConsumerGroupsOffsetsCommand, OperationId,
    PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, ScenarioAction,
};

#[test]
fn versions_advance_without_changing_evidence_facts() {
    assert_eq!(PROTOCOL_VERSION, 34);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 37);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 26);
}

#[test]
fn batch_listing_expectations_stay_off_wire_and_order_is_stable() {
    let action =
        ScenarioAction::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchAction {
            client_id: client(),
            operation_id: operation("list-batch"),
            group_id: "group-1".to_owned(),
            require_stable: true,
            partitions: expectations(),
            timeout_ms: 1_000,
        });
    let command =
        AdapterCommand::ListConsumerGroupOffsetsBatch(ListConsumerGroupOffsetsBatchCommand {
            client_id: client(),
            operation_id: operation("list-batch"),
            group_id: "group-1".to_owned(),
            require_stable: true,
            partitions: selections(),
            timeout_ms: 1_000,
        });
    let event = AdapterEvent::ConsumerGroupOffsetsListed(AdminConsumerGroupOffsetsListing {
        operation_id: operation("list-batch"),
        group_id: "group-1".to_owned(),
        outcomes: vec![
            offset_outcome("topic-z", 1, Some(7)),
            offset_outcome("topic-a", 0, Some(9)),
        ],
    });

    let action_encoded = encode(&action);
    let command_encoded = encode(&command);

    assert!(action_encoded.contains("expected_offset = 7"));
    assert!(action_encoded.contains("expected_offset = 9"));
    assert!(!command_encoded.contains("expected_offset"));
    assert_order(&action_encoded, "topic-z", "topic-a");
    assert_order(&command_encoded, "topic-z", "topic-a");
    assert_eq!(decode::<ScenarioAction>(&action_encoded), action);
    assert_eq!(decode::<AdapterCommand>(&command_encoded), command);
    assert_eq!(decode::<AdapterEvent>(&encode(&event)), event);
}

#[test]
fn multi_group_command_and_event_preserve_nested_caller_order() {
    let command = AdapterCommand::ListConsumerGroupsOffsets(ListConsumerGroupsOffsetsCommand {
        client_id: client(),
        operation_id: operation("list-groups"),
        require_stable: false,
        groups: vec![
            group_selection("group-z", "topic-z", 1),
            group_selection("group-a", "topic-a", 0),
        ],
        timeout_ms: 1_000,
    });
    let event = AdapterEvent::ConsumerGroupsOffsetsListed(AdminConsumerGroupsOffsetsListing {
        operation_id: operation("list-groups"),
        groups: vec![
            group_outcome("group-z", "topic-z", 1, Some(7)),
            group_outcome("group-a", "topic-a", 0, None),
        ],
    });

    let command_encoded = encode(&command);
    let event_encoded = encode(&event);

    assert_order(&command_encoded, "group-z", "group-a");
    assert_order(&event_encoded, "group-z", "group-a");
    assert_eq!(decode::<AdapterCommand>(&command_encoded), command);
    assert_eq!(decode::<AdapterEvent>(&event_encoded), event);
}

#[test]
fn plural_mutation_payloads_round_trip_ordered_outcomes() {
    let alter = AdapterCommand::AlterConsumerGroupOffsets(AlterConsumerGroupOffsetsCommand {
        client_id: client(),
        operation_id: operation("alter-batch"),
        group_id: "group-1".to_owned(),
        offsets: vec![alteration("topic-z", 1, 7), alteration("topic-a", 0, 9)],
        timeout_ms: 1_000,
    });
    let delete = AdapterCommand::DeleteConsumerGroupOffsets(DeleteConsumerGroupOffsetsCommand {
        client_id: client(),
        operation_id: operation("delete-batch"),
        group_id: "group-1".to_owned(),
        partitions: selections(),
        timeout_ms: 1_000,
    });
    let event = AdapterEvent::ConsumerGroupOffsetsAltered(AdminConsumerGroupOffsetsMutation {
        operation_id: operation("alter-batch"),
        group_id: "group-1".to_owned(),
        outcomes: vec![mutation("topic-z", 1), mutation("topic-a", 0)],
    });

    for command in [alter, delete] {
        let encoded = encode(&command);
        assert_order(&encoded, "topic-z", "topic-a");
        assert_eq!(decode::<AdapterCommand>(&encoded), command);
    }
    let event_encoded = encode(&event);
    assert_order(&event_encoded, "topic-z", "topic-a");
    assert_eq!(decode::<AdapterEvent>(&event_encoded), event);
}

#[test]
fn classic_member_expectations_stay_out_of_the_wire_command() {
    let action = ScenarioAction::DescribeClassicGroups(DescribeClassicGroupsAction {
        client_id: client(),
        operation_id: operation("describe-classic"),
        groups: vec![ClassicGroupExpectation {
            group_id: "group-1".to_owned(),
            expected_member_count: 2,
        }],
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeClassicGroups(DescribeClassicGroupsCommand {
        client_id: client(),
        operation_id: operation("describe-classic"),
        group_ids: vec!["group-1".to_owned()],
        timeout_ms: 1_000,
    });
    let event = AdapterEvent::ClassicGroupsDescribed(AdminClassicGroupsDescription {
        operation_id: operation("describe-classic"),
        outcomes: vec![AdminClassicGroupDescriptionOutcome {
            group_id: "group-1".to_owned(),
            member_count: Some(2),
            error_code: None,
        }],
    });

    let action_encoded = encode(&action);
    let command_encoded = encode(&command);

    assert!(action_encoded.contains("expected_member_count = 2"));
    assert!(!command_encoded.contains("expected_member_count"));
    assert_eq!(decode::<ScenarioAction>(&action_encoded), action);
    assert_eq!(decode::<AdapterCommand>(&command_encoded), command);
    assert_eq!(decode::<AdapterEvent>(&encode(&event)), event);
}

fn expectations() -> Vec<ConsumerGroupOffsetExpectation> {
    vec![expectation("topic-z", 1, 7), expectation("topic-a", 0, 9)]
}

fn expectation(
    topic: &str,
    partition: i32,
    expected_offset: i64,
) -> ConsumerGroupOffsetExpectation {
    ConsumerGroupOffsetExpectation {
        topic: topic.to_owned(),
        partition,
        expected_offset,
    }
}

fn selections() -> Vec<ConsumerGroupOffsetSelection> {
    vec![selection("topic-z", 1), selection("topic-a", 0)]
}

fn selection(topic: &str, partition: i32) -> ConsumerGroupOffsetSelection {
    ConsumerGroupOffsetSelection {
        topic: topic.to_owned(),
        partition,
    }
}

fn group_selection(group_id: &str, topic: &str, partition: i32) -> ConsumerGroupOffsetsSelection {
    ConsumerGroupOffsetsSelection {
        group_id: group_id.to_owned(),
        partitions: vec![selection(topic, partition)],
    }
}

fn group_outcome(
    group_id: &str,
    topic: &str,
    partition: i32,
    offset: Option<i64>,
) -> AdminConsumerGroupOffsetsOutcome {
    AdminConsumerGroupOffsetsOutcome {
        group_id: group_id.to_owned(),
        error_code: None,
        offsets: vec![offset_outcome(topic, partition, offset)],
    }
}

fn offset_outcome(
    topic: &str,
    partition: i32,
    offset: Option<i64>,
) -> AdminConsumerGroupOffsetOutcome {
    AdminConsumerGroupOffsetOutcome {
        topic: topic.to_owned(),
        partition,
        offset,
        error_code: None,
    }
}

fn alteration(topic: &str, partition: i32, offset: i64) -> ConsumerGroupOffsetAlteration {
    ConsumerGroupOffsetAlteration {
        topic: topic.to_owned(),
        partition,
        offset,
    }
}

fn mutation(topic: &str, partition: i32) -> AdminConsumerGroupOffsetMutationOutcome {
    AdminConsumerGroupOffsetMutationOutcome {
        topic: topic.to_owned(),
        partition,
        error_code: None,
    }
}

fn assert_order(encoded: &str, first: &str, second: &str) {
    assert!(encoded.find(first) < encoded.find(second), "{encoded}");
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize value: {error}"))
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> T {
    toml::from_str(value).unwrap_or_else(|error| panic!("deserialize value: {error}"))
}

fn client() -> super::ClientId {
    super::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
