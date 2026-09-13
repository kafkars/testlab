//! Earliest-offset verification requires an independent low watermark.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminOffsetListing, AdminOffsetSelector, AdminReadIsolation,
    BrokerPartitionOffsets, BrokerStateObservation, HistoryEntry, HistoryPayload,
    ListOffsetsAction, ListOffsetsCommand, OperationId, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn earliest_offset_matches_an_independently_observed_zero() {
    let scenario = earliest_scenario(AdminReadIsolation::ReadCommitted);
    let history = offset_history(Some(0), 0);

    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn earliest_offset_rejects_a_nonzero_independent_minimum() {
    let scenario = earliest_scenario(AdminReadIsolation::ReadCommitted);
    let history = offset_history(Some(0), 1);

    assert_contract(&violations(&scenario, &history), "ADMIN-005");
}

#[test]
fn earliest_offset_rejects_a_nonzero_public_result() {
    let scenario = earliest_scenario(AdminReadIsolation::ReadCommitted);
    let history = offset_history(Some(1), 0);

    assert_contract(&violations(&scenario, &history), "ADMIN-005");
}

#[test]
fn read_uncommitted_offset_mismatch_reports_its_exact_contract() {
    let scenario = earliest_scenario(AdminReadIsolation::ReadUncommitted);
    let history = offset_history(Some(0), 1);

    assert_contract(&violations(&scenario, &history), "ADMIN-088");
}

fn earliest_scenario(read_isolation: AdminReadIsolation) -> testlab_schema::Scenario {
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
                position: AdminOffsetSelector::Earliest,
                read_isolation,
                timestamp_millis: None,
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
                timestamp_millis: None,
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
            read_isolation: action.read_isolation,
            timestamp_millis: action.timestamp_millis,
            timeout_ms: action.timeout_ms,
        }),
    )];
    entries.extend_from_slice(history);
    let index = HistoryIndex::build(&entries);
    let mut violations = Vec::new();
    verify_admin(scenario, &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract_id: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract_id),
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
