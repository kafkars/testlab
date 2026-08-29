//! `DeleteRecords` verification pins exact public and independent watermark transitions.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminOffsetListing, AdminOffsetPosition, AdminRecordsDeleted,
    BrokerPartitionOffsets, BrokerStateObservation, DeleteRecordsAction, DeleteRecordsCommand,
    HistoryEntry, HistoryPayload, ListOffsetsAction, ListOffsetsCommand, OperationId,
    ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_record_prefix_deletion_passes() {
    let (scenario, history) = fixture(2, 3, true);

    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn public_low_or_changed_high_watermark_fails() {
    for (public_low, post_high) in [(1, 3), (2, 4)] {
        let (scenario, history) = fixture(public_low, post_high, true);

        assert_contract(&violations(&scenario, &history), "ADMIN-017");
    }
}

#[test]
fn deletion_requires_a_prior_independent_range() {
    let (scenario, history) = fixture(2, 3, false);

    assert_contract(&violations(&scenario, &history), "ADMIN-017");
}

fn fixture(
    public_low: i64,
    post_high: i64,
    include_baseline: bool,
) -> (testlab_schema::Scenario, Vec<HistoryEntry>) {
    let earliest = list_action("baseline-earliest", AdminOffsetPosition::Earliest, 0);
    let latest = list_action("baseline-latest", AdminOffsetPosition::Latest, 3);
    let deletion = ScenarioAction::DeleteRecords(DeleteRecordsAction {
        client_id: client(),
        operation_id: operation("delete-prefix"),
        topic: "records".to_owned(),
        partition: 0,
        before_offset: 2,
        expected_high_watermark: 3,
        timeout_ms: 1_000,
    });
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.steps.splice(
        2..2,
        [
            step("baseline-earliest", earliest.clone()),
            step("baseline-latest", latest.clone()),
            step("delete-prefix", deletion.clone()),
        ],
    );
    let mut history = Vec::new();
    if include_baseline {
        history.extend([
            command(0, wire(&earliest)),
            offset_event(1, "baseline-earliest", 0),
            offsets_state(2, "baseline-earliest", 0, 3),
            command(3, wire(&latest)),
            offset_event(4, "baseline-latest", 3),
            offsets_state(5, "baseline-latest", 0, 3),
        ]);
    }
    history.extend([
        command(6, wire(&deletion)),
        event(
            7,
            AdapterEvent::RecordsDeleted(AdminRecordsDeleted {
                operation_id: operation("delete-prefix"),
                topic: "records".to_owned(),
                partition: 0,
                low_watermark: public_low,
            }),
        ),
        offsets_state(8, "delete-prefix", 2, post_high),
    ]);
    (scenario, history)
}

fn list_action(id: &str, position: AdminOffsetPosition, expected_offset: i64) -> ScenarioAction {
    ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: client(),
        operation_id: operation(id),
        topic: "records".to_owned(),
        partition: 0,
        position,
        expected_offset: Some(expected_offset),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}

fn wire(action: &ScenarioAction) -> AdapterCommand {
    match action {
        ScenarioAction::ListOffsets(value) => AdapterCommand::ListOffsets(ListOffsetsCommand {
            client_id: value.client_id.clone(),
            operation_id: value.operation_id.clone(),
            topic: value.topic.clone(),
            partition: value.partition,
            position: value.position,
            timeout_ms: value.timeout_ms,
        }),
        ScenarioAction::DeleteRecords(value) => {
            AdapterCommand::DeleteRecords(DeleteRecordsCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                topic: value.topic.clone(),
                partition: value.partition,
                before_offset: value.before_offset,
                timeout_ms: value.timeout_ms,
            })
        }
        _ => panic!("fixture action must be an offset admin action"),
    }
}

fn offset_event(sequence: u64, id: &str, offset: i64) -> HistoryEntry {
    event(
        sequence,
        AdapterEvent::OffsetListed(AdminOffsetListing {
            operation_id: operation(id),
            topic: "records".to_owned(),
            partition: 0,
            offset: Some(offset),
        }),
    )
}

fn offsets_state(sequence: u64, id: &str, low: i64, high: i64) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
                observation: sequence,
                operation_id: operation(id),
                topic: "records".to_owned(),
                partition: 0,
                low_watermark: low,
                high_watermark: high,
            }),
        },
    }
}

fn violations(
    scenario: &testlab_schema::Scenario,
    history: &[HistoryEntry],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(scenario, &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
