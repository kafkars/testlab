//! Plural topic-deletion observation pins exact order and successful provisioning intent.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, ClientId, DeleteTopicExpectation, DeleteTopicsAction, DeleteTopicsCommand,
    OperationId, Scenario, ScenarioAction, TopicSelection, UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

use crate::observer_admin_target::{AdminTarget, ListTarget};

#[test]
fn exact_action_and_expectation_free_command_map_to_ordered_absence() {
    let action = ScenarioAction::DeleteTopics(action());
    let command = AdapterCommand::DeleteTopics(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map plural topic deletion: {error}"))
        .unwrap_or_else(|| panic!("plural topic-deletion target"));
    assert_eq!(
        target,
        AdminTarget::TopicDeletions(ListTarget {
            operation_id: operation(),
            names: topic_names(),
        })
    );
    assert_eq!(target.observation_count(), 3);
}

#[test]
fn altered_wire_order_is_rejected_before_observation() {
    let action = ScenarioAction::DeleteTopics(action());
    let mut command = command();
    command.topics.swap(0, 1);
    let error = AdminTarget::from_exact(&action, &AdapterCommand::DeleteTopics(command))
        .expect_err("mismatched plural topic-deletion order");
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn topic_id_selection_still_maps_to_ordered_absence_polling() {
    let mut action = action();
    action.selection = TopicSelection::TopicId;
    action.topics.remove(1);
    let mut command = command();
    command.selection = TopicSelection::TopicId;
    command.topics.remove(1);
    let target = AdminTarget::from_exact(
        &ScenarioAction::DeleteTopics(action),
        &AdapterCommand::DeleteTopics(command),
    )
    .unwrap_or_else(|error| panic!("map topic-ID deletions: {error}"))
    .unwrap_or_else(|| panic!("topic-ID deletion target"));
    assert_eq!(
        target,
        AdminTarget::TopicDeletions(ListTarget {
            operation_id: operation(),
            names: vec!["topic-z".to_owned(), "topic-a".to_owned()],
        })
    );
}

#[test]
fn provisioning_creates_only_topics_expected_to_delete_successfully() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-topics.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural topic-deletion scenario: {error}"));
    assert_eq!(
        crate::compose_provision_targets::topics(&scenario),
        BTreeMap::from([
            ("testlab-kafkars-admin-delete-topics-alpha".to_owned(), 1),
            ("testlab-kafkars-admin-delete-topics-zulu".to_owned(), 2),
        ])
    );
}

fn action() -> DeleteTopicsAction {
    DeleteTopicsAction {
        client_id: client(),
        operation_id: operation(),
        selection: TopicSelection::Name,
        topics: vec![
            expectation("topic-z", None),
            expectation("topic-missing", Some(UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE)),
            expectation("topic-a", None),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> DeleteTopicsCommand {
    DeleteTopicsCommand {
        client_id: client(),
        operation_id: operation(),
        selection: TopicSelection::Name,
        topics: topic_names(),
        timeout_ms: 1_000,
    }
}

fn expectation(topic: &str, error: Option<&str>) -> DeleteTopicExpectation {
    DeleteTopicExpectation {
        topic: topic.to_owned(),
        expected_error_code: error.map(str::to_owned),
    }
}

fn topic_names() -> Vec<String> {
    vec![
        "topic-z".to_owned(),
        "topic-missing".to_owned(),
        "topic-a".to_owned(),
    ]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("delete-topics").unwrap_or_else(|error| panic!("operation: {error}"))
}
