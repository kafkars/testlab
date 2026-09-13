//! Topic-list wire tests keep inclusion expectations private to the scenario.

use super::*;
use crate::TopicListingExpectation;

#[test]
fn list_topics_command_excludes_expected_topics() {
    let action = ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id: operation("admin-topics-1"),
        include_internal: true,
        include_authorized_operations: true,
        expected_topics: vec![TopicListingExpectation {
            topic: "__consumer_offsets".to_owned(),
            internal: true,
            included: true,
        }],
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::ListTopics(ListTopicsCommand {
        client_id: client(),
        operation_id: operation("admin-topics-1"),
        include_internal: true,
        include_authorized_operations: true,
        timeout_ms: 1_000,
    });

    let action = encode_action(&action);
    let command = encode(&command);
    assert!(action.contains("kind = \"list_topics\""));
    assert!(action.contains("topic = \"__consumer_offsets\""));
    assert!(action.contains("internal = true"));
    assert!(action.contains("included = true"));
    assert!(command.contains("kind = \"list_topics\""));
    assert!(command.contains("include_internal = true"));
    assert!(command.contains("include_authorized_operations = true"));
    assert!(!command.contains("expected_topics"));
}
