//! Streams-group lifecycle protocol tests retain every public and independent fact.

use crate::*;

#[test]
fn lifecycle_payloads_round_trip_with_exact_order() {
    assert_eq!(PROTOCOL_VERSION, 103);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 106);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 92);
    let action = action();
    round_trip(&ScenarioAction::ExerciseStreamsGroupAdminLifecycle(
        action.clone(),
    ));
    let command = AdapterCommand::ExerciseStreamsGroupAdminLifecycle(command());
    round_trip(&command);
    let encoded =
        serde_json::to_string(&command).unwrap_or_else(|error| panic!("encode command: {error}"));
    assert!(!encoded.contains("expected_initial_offset"));
    assert!(!encoded.contains("output_topic"));
    round_trip(&AdapterEvent::StreamsGroupAdminLifecycleExercised(
        completion(),
    ));
    round_trip(&BrokerStateObservation::StreamsGroups(
        BrokerStreamsGroupsState {
            observation: 7,
            operation_id: operation(),
            group_ids: vec!["streams-secondary".to_owned(), "streams-primary".to_owned()],
            all_absent: true,
        },
    ));
}

fn command() -> ExerciseStreamsGroupAdminLifecycleCommand {
    ExerciseStreamsGroupAdminLifecycleCommand {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
        operation_id: operation(),
        primary_group_id: "streams-primary".to_owned(),
        secondary_group_id: "streams-secondary".to_owned(),
        input_topic: STREAMS_DEMO_INPUT_TOPIC.to_owned(),
        altered_offset: 0,
        timeout_ms: 60_000,
    }
}

fn action() -> ExerciseStreamsGroupAdminLifecycleAction {
    ExerciseStreamsGroupAdminLifecycleAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}")),
        operation_id: operation(),
        primary_group_id: "streams-primary".to_owned(),
        secondary_group_id: "streams-secondary".to_owned(),
        input_topic: STREAMS_DEMO_INPUT_TOPIC.to_owned(),
        output_topic: STREAMS_DEMO_OUTPUT_TOPIC.to_owned(),
        expected_initial_offset: 1,
        altered_offset: 0,
        timeout_ms: 60_000,
    }
}

fn completion() -> AdminStreamsGroupAdminLifecycle {
    let primary = description("streams-primary");
    let secondary = description("streams-secondary");
    AdminStreamsGroupAdminLifecycle {
        operation_id: operation(),
        singular_description: primary.clone(),
        plural_descriptions: vec![secondary.clone(), primary.clone()],
        singular_initial_offset: offset("streams-primary", Some(1)),
        plural_initial_offsets: vec![
            offset("streams-secondary", Some(1)),
            offset("streams-primary", Some(1)),
        ],
        offset_after_alter: offset("streams-primary", Some(0)),
        deleted_offset: AdminStreamsGroupPartition {
            group_id: "streams-primary".to_owned(),
            topic: STREAMS_DEMO_INPUT_TOPIC.to_owned(),
            partition: 0,
        },
        offset_after_delete: offset("streams-primary", None),
        deleted_group_ids: vec!["streams-secondary".to_owned(), "streams-primary".to_owned()],
        throttle_times_ms: [0; 9],
    }
}

fn description(group_id: &str) -> AdminStreamsGroupDescription {
    AdminStreamsGroupDescription {
        group_id: group_id.to_owned(),
        state: "Empty".to_owned(),
        group_epoch: 1,
        assignment_epoch: 1,
        topology_epoch: Some(0),
        topology_source_topics: vec![STREAMS_DEMO_INPUT_TOPIC.to_owned()],
        topology_subtopology_count: Some(1),
        member_count: 0,
        authorized_operations: Some(1),
        topology_description_status: Some(1),
        topology_description_subtopology_count: None,
    }
}

fn offset(group_id: &str, committed_offset: Option<i64>) -> AdminStreamsGroupOffset {
    AdminStreamsGroupOffset {
        group_id: group_id.to_owned(),
        topic: STREAMS_DEMO_INPUT_TOPIC.to_owned(),
        partition: 0,
        committed_offset,
        leader_epoch: None,
        metadata: None,
    }
}

fn operation() -> OperationId {
    OperationId::new("streams-group-admin-lifecycle")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Eq + std::fmt::Debug,
{
    let encoded = serde_json::to_vec(value).unwrap_or_else(|error| panic!("encode: {error}"));
    let decoded =
        serde_json::from_slice(&encoded).unwrap_or_else(|error| panic!("decode: {error}"));
    assert_eq!(value, &decoded);
}
