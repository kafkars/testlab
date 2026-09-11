//! Batched record-deletion verification pins public order and both broker ranges.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminOffsetListingOutcome, AdminOffsetsListing,
    AdminRecordsBatchDeleted, AdminRecordsDeletionOutcome, BrokerPartitionOffsets,
    BrokerStateObservation, DeleteRecordsBatchCommand, DeleteRecordsBatchSelection, HistoryEntry,
    HistoryPayload, ListOffsetsBatchCommand, OffsetListingSelection, Scenario, ScenarioAction,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

#[test]
fn exact_caller_ordered_batch_and_sentinel_transition_pass() {
    let scenario = scenario();
    assert!(violations(&scenario, &history(&scenario)).is_empty());
}

#[test]
fn reordered_public_or_wrong_sentinel_transition_fails() {
    let scenario = scenario();
    let mut reordered = history(&scenario);
    let HistoryPayload::AdapterEvent { event } = &mut reordered[9].payload else {
        panic!("deletion completion fixture");
    };
    let AdapterEvent::RecordsBatchDeleted(completion) = &mut event.event else {
        panic!("deletion completion kind");
    };
    completion.outcomes.swap(0, 1);
    assert_contract(&violations(&scenario, &reordered));

    let mut wrong_range = history(&scenario);
    let HistoryPayload::BrokerStateObservation { observation } = &mut wrong_range[11].payload
    else {
        panic!("post-deletion observation fixture");
    };
    let BrokerStateObservation::PartitionOffsets(offsets) = observation else {
        panic!("post-deletion offsets kind");
    };
    offsets.low_watermark = 1;
    assert_contract(&violations(&scenario, &wrong_range));
}

fn history(scenario: &Scenario) -> Vec<HistoryEntry> {
    let actions = scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            action @ (ScenarioAction::ListOffsetsBatch(_)
            | ScenarioAction::DeleteRecordsBatch(_)) => Some(action),
            _ => None,
        })
        .collect::<Vec<_>>();
    let [earliest, latest, deletion] = actions.as_slice() else {
        panic!("expected two baselines and one deletion");
    };
    vec![
        command(0, wire(earliest)),
        event(1, listed(earliest)),
        offsets(2, earliest, 1, 0, 3),
        offsets(3, earliest, 0, 0, 2),
        command(4, wire(latest)),
        event(5, listed(latest)),
        offsets(6, latest, 1, 0, 3),
        offsets(7, latest, 0, 0, 2),
        command(8, wire(deletion)),
        event(9, deleted(deletion)),
        offsets(10, deletion, 1, 2, 3),
        offsets(11, deletion, 0, 2, 2),
    ]
}

fn wire(action: &ScenarioAction) -> AdapterCommand {
    match action {
        ScenarioAction::ListOffsetsBatch(action) => {
            AdapterCommand::ListOffsetsBatch(ListOffsetsBatchCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                queries: action
                    .queries
                    .iter()
                    .map(|query| OffsetListingSelection {
                        topic: query.topic.clone(),
                        partition: query.partition,
                        position: query.position,
                    })
                    .collect(),
                timeout_ms: action.timeout_ms,
            })
        }
        ScenarioAction::DeleteRecordsBatch(action) => {
            AdapterCommand::DeleteRecordsBatch(DeleteRecordsBatchCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                targets: action
                    .targets
                    .iter()
                    .map(|target| DeleteRecordsBatchSelection {
                        topic: target.topic.clone(),
                        partition: target.partition,
                        boundary: target.boundary,
                    })
                    .collect(),
                timeout_ms: action.timeout_ms,
            })
        }
        _ => panic!("unexpected admin action"),
    }
}

fn listed(action: &ScenarioAction) -> AdapterEvent {
    let ScenarioAction::ListOffsetsBatch(action) = action else {
        panic!("offset batch action");
    };
    AdapterEvent::OffsetsListed(AdminOffsetsListing {
        operation_id: action.operation_id.clone(),
        outcomes: action
            .queries
            .iter()
            .map(|query| AdminOffsetListingOutcome {
                topic: query.topic.clone(),
                partition: query.partition,
                offset: Some(query.expected_offset),
                error_code: None,
            })
            .collect(),
    })
}

fn deleted(action: &ScenarioAction) -> AdapterEvent {
    let ScenarioAction::DeleteRecordsBatch(action) = action else {
        panic!("record deletion batch action");
    };
    AdapterEvent::RecordsBatchDeleted(AdminRecordsBatchDeleted {
        operation_id: action.operation_id.clone(),
        outcomes: action
            .targets
            .iter()
            .map(|target| AdminRecordsDeletionOutcome {
                topic: target.topic.clone(),
                partition: target.partition,
                low_watermark: Some(
                    target
                        .boundary
                        .expected_low_watermark(target.expected_high_watermark),
                ),
                error_code: None,
            })
            .collect(),
    })
}

fn offsets(
    sequence: u64,
    action: &ScenarioAction,
    partition: i32,
    low_watermark: i64,
    high_watermark: i64,
) -> HistoryEntry {
    let operation_id = match action {
        ScenarioAction::ListOffsetsBatch(action) => action.operation_id.clone(),
        ScenarioAction::DeleteRecordsBatch(action) => action.operation_id.clone(),
        _ => panic!("offset observation action"),
    };
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::PartitionOffsets(BrokerPartitionOffsets {
                observation: sequence,
                operation_id,
                topic: "testlab-kafkars-admin-delete-records-batch".to_owned(),
                partition,
                low_watermark,
                high_watermark,
            }),
        },
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-records-batch.toml"
    ))
    .unwrap_or_else(|error| panic!("parse batched record-deletion scenario: {error}"))
}

fn violations(scenario: &Scenario, history: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    verify_admin(scenario, &index, &[], &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation]) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-047"),
        "{violations:?}"
    );
}
