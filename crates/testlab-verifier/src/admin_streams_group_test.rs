//! Streams-group lifecycle matching rejects reordered or inconsistent evidence.

use testlab_schema::{
    AdminStreamsGroupAdminLifecycle, AdminStreamsGroupDescription, AdminStreamsGroupOffset,
    AdminStreamsGroupPartition, ClientId, ExerciseStreamsGroupAdminLifecycleAction, OperationId,
    STREAMS_DEMO_INPUT_TOPIC, STREAMS_DEMO_OUTPUT_TOPIC,
};

use super::{contract, public_matches};

#[test]
fn exact_lifecycle_matches_and_reordering_fails() {
    let action = action();
    let mut completion = completion();
    assert!(public_matches(&completion, &action));
    completion.plural_initial_offsets.swap(0, 1);
    assert!(!public_matches(&completion, &action));
}

#[test]
fn full_topology_status_must_match_payload_availability() {
    let action = action();
    let mut completion = completion();
    completion.singular_description.topology_description_status = Some(2);
    assert!(!public_matches(&completion, &action));
    completion.singular_description.topology_description_status = Some(3);
    completion
        .singular_description
        .topology_description_subtopology_count = Some(1);
    assert!(public_matches(&completion, &action));
}

#[test]
fn default_description_and_offset_options_remain_exact() {
    let mut action = action();
    action.include_authorized_operations = false;
    action.include_topology_description = false;
    action.require_stable = false;
    let mut completion = completion();
    completion.singular_description.authorized_operations = None;
    completion.singular_description.topology_description_status = Some(0);
    for description in &mut completion.plural_descriptions {
        description.authorized_operations = None;
        description.topology_description_status = Some(0);
    }
    assert_eq!(contract(&action), "ADMIN-092");
    assert!(public_matches(&completion, &action));
    completion.singular_description.authorized_operations = Some(1);
    assert!(!public_matches(&completion, &action));
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
        include_authorized_operations: true,
        include_topology_description: true,
        require_stable: true,
        timeout_ms: 60_000,
    }
}

fn completion() -> AdminStreamsGroupAdminLifecycle {
    AdminStreamsGroupAdminLifecycle {
        operation_id: operation(),
        singular_description: description("streams-primary"),
        plural_descriptions: vec![
            description("streams-secondary"),
            description("streams-primary"),
        ],
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
        topology_epoch: Some(1),
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
        leader_epoch: Some(1),
        metadata: Some(String::new()),
    }
}

fn operation() -> OperationId {
    OperationId::new("streams-group-admin-lifecycle")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
