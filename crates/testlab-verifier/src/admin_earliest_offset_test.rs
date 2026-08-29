//! Earliest-offset verification requires an independent low watermark.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminOffsetListing, AdminOffsetPosition, BrokerPartitionOffsets,
    BrokerStateObservation, HistoryEntry, HistoryPayload, ListOffsetsAction, ListOffsetsCommand,
    OperationId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn earliest_offset_matches_an_independently_observed_zero() {
    let scenario = earliest_scenario();
    let history = offset_history(Some(0), 0);

    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn earliest_offset_rejects_a_nonzero_independent_minimum() {
    let scenario = earliest_scenario();
    let history = offset_history(Some(0), 1);

    assert_admin_005(&violations(&scenario, &history));
}

#[test]
fn earliest_offset_rejects_a_nonzero_public_result() {
    let scenario = earliest_scenario();
    let history = offset_history(Some(1), 0);

    assert_admin_005(&violations(&scenario, &history));
}

fn earliest_scenario() -> testlab_schema::Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.steps.insert(
        2,
        step(
            "list-earliest-offset",
            ScenarioAction::ListOffsets(ListOffsetsAction {
                client_id: client(),
                operation_id: operation(),
                topic: "offsets".to_owned(),
                partition: 0,
                position: AdminOffsetPosition::Earliest,
                expected_offset: Some(0),
                expected_error_code: None,
                timeout_ms: 1_000,
            }),
        ),
    );
    value
}

fn offset_history(offset: Option<i64>, low_watermark: i64) -> [HistoryEntry; 2] {
    [
        event(
            1,
            AdapterEvent::OffsetListed(AdminOffsetListing {
                operation_id: operation(),
                topic: "offsets".to_owned(),
                partition: 0,
                offset,
            }),
        ),
        HistoryEntry {
            sequence: 2,
            observed_unix_ms: 2,
            payload: HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
                    observation: 2,
                    operation_id: operation(),
                    topic: "offsets".to_owned(),
                    partition: 0,
                    low_watermark,
                    high_watermark: 2,
                }),
            },
        },
    ]
}

fn violations(
    scenario: &testlab_schema::Scenario,
    history: &[testlab_schema::HistoryEntry],
) -> Vec<testlab_schema::Violation> {
    let action = &scenario.steps[2].action;
    let ScenarioAction::ListOffsets(action) = action else {
        panic!("fixture action must list offsets");
    };
    let mut entries = vec![command(
        0,
        AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
            position: action.position,
            timeout_ms: action.timeout_ms,
        }),
    )];
    entries.extend_from_slice(history);
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();
    verify_admin(scenario, &index, &[], &mut violations);
    violations
}

fn assert_admin_005(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-005"),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-earliest-offset")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
