//! Batched offset-listing tests pin ordered wire facts and validation bounds.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminOffsetListingOutcome, AdminOffsetPosition,
    AdminOffsetsListing, ClientId, ListOffsetsBatchAction, ListOffsetsBatchCommand,
    OffsetListingExpectation, OffsetListingSelection, OperationId, ScenarioAction,
};

#[test]
fn batch_offset_wire_omits_expectations_and_round_trips_order() {
    let action = ScenarioAction::ListOffsetsBatch(action());
    let command = AdapterCommand::ListOffsetsBatch(command());
    let event = AdapterEvent::OffsetsListed(AdminOffsetsListing {
        operation_id: operation(),
        outcomes: vec![
            outcome("records", 2, Some(1)),
            outcome("records", 0, Some(0)),
        ],
    });

    let encoded_action = encode(&action);
    let encoded_command = encode(&command);
    let encoded_event = encode(&event);
    assert!(encoded_action.contains("kind = \"list_offsets_batch\""));
    assert!(encoded_action.contains("expected_offset = 1"));
    assert!(!encoded_command.contains("expected_offset"));
    assert!(encoded_event.contains("kind = \"offsets_listed\""));
    assert_eq!(decode::<ScenarioAction>(&encoded_action), action);
    assert_eq!(decode::<AdapterCommand>(&encoded_command), command);
    assert_eq!(decode::<AdapterEvent>(&encoded_event), event);
}

#[test]
fn batch_offset_validation_rejects_duplicate_or_invalid_queries() {
    let mut action = action();
    action.queries[1] = OffsetListingExpectation {
        topic: "records".to_owned(),
        partition: 2,
        position: AdminOffsetPosition::Earliest,
        expected_offset: -1,
    };
    let clients = BTreeMap::from([(client(), false)]);
    let mut operations = BTreeSet::new();
    let mut problems = Vec::new();

    crate::admin_action_validation::validate(
        &ScenarioAction::ListOffsetsBatch(action),
        &clients,
        &mut operations,
        &mut problems,
    );

    assert!(
        problems
            .iter()
            .any(|value| value.contains("must be unique"))
    );
    assert!(
        problems
            .iter()
            .any(|value| value.contains("expected_offset must be nonnegative"))
    );
}

fn action() -> ListOffsetsBatchAction {
    ListOffsetsBatchAction {
        client_id: client(),
        operation_id: operation(),
        queries: vec![
            expectation("records", 2, AdminOffsetPosition::Latest, 1),
            expectation("records", 0, AdminOffsetPosition::Earliest, 0),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> ListOffsetsBatchCommand {
    ListOffsetsBatchCommand {
        client_id: client(),
        operation_id: operation(),
        queries: vec![
            selection("records", 2, AdminOffsetPosition::Latest),
            selection("records", 0, AdminOffsetPosition::Earliest),
        ],
        timeout_ms: 1_000,
    }
}

fn expectation(
    topic: &str,
    partition: i32,
    position: AdminOffsetPosition,
    expected_offset: i64,
) -> OffsetListingExpectation {
    OffsetListingExpectation {
        topic: topic.to_owned(),
        partition,
        position,
        expected_offset,
    }
}

fn selection(topic: &str, partition: i32, position: AdminOffsetPosition) -> OffsetListingSelection {
    OffsetListingSelection {
        topic: topic.to_owned(),
        partition,
        position,
    }
}

fn outcome(topic: &str, partition: i32, offset: Option<i64>) -> AdminOffsetListingOutcome {
    AdminOffsetListingOutcome {
        topic: topic.to_owned(),
        partition,
        offset,
        error_code: None,
    }
}

fn encode<T: serde::Serialize>(value: &T) -> String {
    toml::to_string(value).unwrap_or_else(|error| panic!("serialize batch offset: {error}"))
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> T {
    toml::from_str(value).unwrap_or_else(|error| panic!("deserialize batch offset: {error}"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-offsets-batch").unwrap_or_else(|error| panic!("operation: {error}"))
}
