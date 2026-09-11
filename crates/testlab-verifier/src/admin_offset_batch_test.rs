//! Batch offset tests pin caller order and independent watermark selection.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminOffsetListingOutcome, AdminOffsetPosition,
    AdminOffsetsListing, BrokerPartitionOffsets, BrokerStateObservation, HistoryEntry,
    HistoryPayload, ListOffsetsBatchAction, ListOffsetsBatchCommand, OffsetListingExpectation,
    OffsetListingSelection, OperationId, ScenarioAction, TerminalStatus, VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn ordered_batch_offsets_pass_with_matching_independent_watermarks() {
    let scenario = offset_batch_scenario();
    let history = offset_batch_history(outcomes(), [(4, 5), (0, 1), (0, 2)]);

    assert!(violations(&scenario, &history).is_empty());
}

#[test]
fn reordered_public_outcomes_fail_the_batch_contract() {
    let scenario = offset_batch_scenario();
    let mut reordered = outcomes();
    reordered.swap(0, 1);
    let history = offset_batch_history(reordered, [(4, 5), (0, 1), (0, 2)]);

    assert_admin_028(&violations(&scenario, &history));
}

#[test]
fn incorrect_independent_watermark_fails_the_batch_contract() {
    let scenario = offset_batch_scenario();
    let history = offset_batch_history(outcomes(), [(4, 6), (0, 1), (0, 2)]);

    assert_admin_028(&violations(&scenario, &history));
}

fn offset_batch_scenario() -> testlab_schema::Scenario {
    let mut value = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    value.steps.insert(
        2,
        step(
            "list-offsets-batch",
            ScenarioAction::ListOffsetsBatch(ListOffsetsBatchAction {
                client_id: client(),
                operation_id: operation(),
                queries: expectations(),
                timeout_ms: 1_000,
            }),
        ),
    );
    value
}

fn offset_batch_history(
    outcomes: Vec<AdminOffsetListingOutcome>,
    watermarks: [(i64, i64); 3],
) -> Vec<HistoryEntry> {
    let mut entries = vec![command(
        10,
        AdapterCommand::ListOffsetsBatch(ListOffsetsBatchCommand {
            client_id: client(),
            operation_id: operation(),
            queries: selections(),
            timeout_ms: 1_000,
        }),
    )];
    entries.push(event(
        11,
        AdapterEvent::OffsetsListed(AdminOffsetsListing {
            operation_id: operation(),
            outcomes,
        }),
    ));
    entries.extend(
        [("records", 2), ("records", 0), ("records", 1)]
            .into_iter()
            .zip(watermarks)
            .enumerate()
            .map(|(index, ((topic, partition), (low, high)))| {
                let index = u64::try_from(index)
                    .unwrap_or_else(|error| panic!("observation index: {error}"));
                partition_offsets(12 + index, topic, partition, low, high)
            }),
    );
    entries
}

fn partition_offsets(
    sequence: u64,
    topic: &str,
    partition: i32,
    low_watermark: i64,
    high_watermark: i64,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
                observation: sequence,
                operation_id: operation(),
                topic: topic.to_owned(),
                partition,
                low_watermark,
                high_watermark,
            }),
        },
    }
}

fn expectations() -> Vec<OffsetListingExpectation> {
    vec![
        expectation(2, AdminOffsetPosition::Latest, 5),
        expectation(0, AdminOffsetPosition::Earliest, 0),
        expectation(1, AdminOffsetPosition::Latest, 2),
    ]
}

fn selections() -> Vec<OffsetListingSelection> {
    expectations()
        .into_iter()
        .map(|value| OffsetListingSelection {
            topic: value.topic,
            partition: value.partition,
            position: value.position,
        })
        .collect()
}

fn outcomes() -> Vec<AdminOffsetListingOutcome> {
    expectations()
        .into_iter()
        .map(|value| AdminOffsetListingOutcome {
            topic: value.topic,
            partition: value.partition,
            offset: Some(value.expected_offset),
            error_code: None,
        })
        .collect()
}

fn expectation(
    partition: i32,
    position: AdminOffsetPosition,
    expected_offset: i64,
) -> OffsetListingExpectation {
    OffsetListingExpectation {
        topic: "records".to_owned(),
        partition,
        position,
        expected_offset,
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

fn assert_admin_028(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-028"),
        "{violations:?}"
    );
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-offsets-batch").unwrap_or_else(|error| panic!("operation: {error}"))
}
