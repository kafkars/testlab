//! Listing targets retain every independent inclusion expectation outside the wire command.

use super::*;
use testlab_schema::{ListTopicsAction, ListTopicsCommand, TopicListingExpectation};

#[test]
fn topic_listing_targets_every_expected_topic() {
    let list = ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id: operation("list-topics"),
        include_internal: false,
        include_authorized_operations: true,
        expected_topics: vec![
            expected("orders", false, true),
            expected("__consumer_offsets", true, false),
        ],
        timeout_ms: 500,
    });
    let AdminTarget::Topics(target) = exact(&list) else {
        panic!("list topics target kind");
    };
    assert_eq!(target.names, ["orders", "__consumer_offsets"]);
}

#[test]
fn duplicate_listing_targets_are_rejected() {
    let action = ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id: operation("list-topics"),
        include_internal: false,
        include_authorized_operations: false,
        expected_topics: vec![
            expected("orders", false, true),
            expected("orders", false, true),
        ],
        timeout_ms: 500,
    });
    let command = AdapterCommand::ListTopics(ListTopicsCommand {
        client_id: client(),
        operation_id: operation("list-topics"),
        include_internal: false,
        include_authorized_operations: false,
        timeout_ms: 500,
    });

    assert!(AdminTarget::from_exact(&action, &command).is_err());
}

fn expected(topic: &str, internal: bool, included: bool) -> TopicListingExpectation {
    TopicListingExpectation {
        topic: topic.to_owned(),
        internal,
        included,
    }
}
