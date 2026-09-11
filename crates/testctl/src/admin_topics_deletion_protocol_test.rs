//! Plural topic-deletion protocol pins expectation-free caller-ordered identities.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminTopicDeletionOutcome, AdminTopicsDeletion, ClientId,
    DeleteTopicExpectation, DeleteTopicsAction, OperationId, ScenarioAction, TopicSelection,
    UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn translation_preserves_caller_order_without_expected_errors() {
    let action = ScenarioAction::DeleteTopics(action());
    let Some((AdapterCommand::DeleteTopics(command), expected)) =
        crate::session_command_admin_batch::translate(&action)
    else {
        panic!("plural topic-deletion translation");
    };
    assert_eq!(command.client_id, client());
    assert_eq!(command.operation_id, operation());
    assert_eq!(command.topics, topic_names());
    assert_eq!(command.timeout_ms, 1_000);
    assert!(matches!(expected, ExpectedEvent::TopicsDeleted { .. }));
}

#[test]
fn completion_requires_every_topic_identity_in_caller_order() {
    let expected = ExpectedEvent::TopicsDeleted {
        operation_id: operation(),
        topics: topic_names(),
    };
    let mut event = AdapterEvent::TopicsDeleted(completion());
    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classify plural topic deletion: {error}")),
        EventDisposition::Complete
    );
    let AdapterEvent::TopicsDeleted(actual) = &mut event else {
        panic!("plural topic-deletion event");
    };
    actual.outcomes.swap(0, 1);
    let error = expected
        .classify(&event)
        .expect_err("reordered topics must not complete");
    assert_eq!(error.harness_error().code, "event_identity_mismatch");
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

fn completion() -> AdminTopicsDeletion {
    AdminTopicsDeletion {
        operation_id: operation(),
        outcomes: topic_names()
            .into_iter()
            .map(|topic| AdminTopicDeletionOutcome {
                topic,
                topic_id: None,
                error_code: None,
            })
            .collect(),
    }
}

fn expectation(topic: &str, expected_error_code: Option<&str>) -> DeleteTopicExpectation {
    DeleteTopicExpectation {
        topic: topic.to_owned(),
        expected_error_code: expected_error_code.map(str::to_owned),
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
