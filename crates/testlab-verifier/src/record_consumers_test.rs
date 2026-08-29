//! Consumer correlation tests pin group sets and Share records to broker truth.

use testlab_schema::{
    AdapterEvent, BrokerObservation, ByteString, ConsumedRecord, ConsumerId, GroupMembershipEpoch,
    GroupReceiveMemberCompletion, GroupReceiveSetAction, GroupReceiveSetCompletion, OperationId,
    ScenarioAction, ShareConsumedRecord, TerminalStatus, VisibilityExpectation,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{event, record, scenario, step};

#[test]
fn ordinary_group_bytes_must_match_independent_observation() {
    let mut scenario = base_scenario();
    let receive_id = operation("receive-group");
    scenario.steps.push(step(
        "receive-group",
        ScenarioAction::GroupReceive {
            consumer_id: consumer("group-1"),
            receive_id: receive_id.clone(),
            expected_operation_id: operation("op-1"),
            expected_error_code: None,
            timeout_ms: 1_000,
        },
    ));
    let history = [event(
        0,
        AdapterEvent::GroupReceiveCompleted {
            receive_id,
            records: vec![consumed("wrong", 0)],
            committed: true,
            group_epoch: Some(GroupMembershipEpoch::Classic { generation_id: 1 }),
        },
    )];

    let violations = verify(&scenario, &history, &[observed("op-1", "value", 0, 0)]);

    assert_contract(&violations, "CONS-012");
}

#[test]
fn group_receive_set_offsets_must_match_each_independent_record() {
    let (scenario, receive_id) = receive_set_scenario();
    let history = [event(
        0,
        AdapterEvent::GroupReceiveSetCompleted(GroupReceiveSetCompletion {
            receive_id,
            members: vec![GroupReceiveMemberCompletion {
                consumer_id: consumer("group-1"),
                records: vec![consumed("value", 0), consumed("second", 9)],
                committed: true,
                group_epoch: Some(GroupMembershipEpoch::Classic { generation_id: 1 }),
            }],
        }),
    )];
    let observations = [
        observed("op-1", "value", 0, 0),
        observed("op-2", "second", 1, 1),
    ];

    let violations = verify(&scenario, &history, &observations);

    assert_contract(&violations, "CONS-012");
}

#[test]
fn group_receive_set_bytes_must_correlate_to_each_operation() {
    let (scenario, receive_id) = receive_set_scenario();
    let history = [event(
        0,
        AdapterEvent::GroupReceiveSetCompleted(GroupReceiveSetCompletion {
            receive_id,
            members: vec![GroupReceiveMemberCompletion {
                consumer_id: consumer("group-1"),
                records: vec![consumed("value", 0), consumed("corrupt", 1)],
                committed: true,
                group_epoch: Some(GroupMembershipEpoch::Classic { generation_id: 1 }),
            }],
        }),
    )];
    let observations = [
        observed("op-1", "value", 0, 0),
        observed("op-2", "second", 1, 1),
    ];

    let violations = verify(&scenario, &history, &observations);

    assert_contract(&violations, "CONS-012");
}

#[test]
fn share_coordinates_and_bytes_must_match_independent_record() {
    let (scenario, receive_id) = share_scenario();
    let wrong_offset = share_event(receive_id.clone(), consumed("value", 8));
    let wrong_bytes = share_event(receive_id, consumed("wrong", 0));
    let observation = [observed("op-1", "value", 0, 0)];

    let offset_violations = verify(&scenario, &[event(0, wrong_offset)], &observation);
    let byte_violations = verify(&scenario, &[event(0, wrong_bytes)], &observation);

    assert_contract(&offset_violations, "SHARE-006");
    assert_contract(&byte_violations, "SHARE-006");
}

#[test]
fn exact_group_set_and_share_records_pass_coordinate_contracts() {
    let (group_scenario, group_receive_id) = receive_set_scenario();
    let group_history = [event(
        0,
        AdapterEvent::GroupReceiveSetCompleted(GroupReceiveSetCompletion {
            receive_id: group_receive_id,
            members: vec![GroupReceiveMemberCompletion {
                consumer_id: consumer("group-1"),
                records: vec![consumed("value", 0), consumed("second", 1)],
                committed: true,
                group_epoch: Some(GroupMembershipEpoch::Classic { generation_id: 1 }),
            }],
        }),
    )];
    let observations = [
        observed("op-1", "value", 0, 0),
        observed("op-2", "second", 1, 1),
    ];
    let (share_scenario, share_receive_id) = share_scenario();
    let share_history = [event(
        0,
        share_event(share_receive_id, consumed("value", 0)),
    )];

    let group_violations = verify(&group_scenario, &group_history, &observations);
    let share_violations = verify(&share_scenario, &share_history, &observations[..1]);

    assert!(!violates(&group_violations, "CONS-012"));
    assert!(!violates(&share_violations, "SHARE-006"));
}

fn receive_set_scenario() -> (testlab_schema::Scenario, OperationId) {
    let mut scenario = base_scenario();
    scenario.steps.insert(
        4,
        step(
            "send-2",
            ScenarioAction::Send {
                producer_id: testlab_schema::ProducerId::new("producer-1")
                    .unwrap_or_else(|error| panic!("producer id: {error}")),
                operation_id: operation("op-2"),
                record: record("second"),
            },
        ),
    );
    let receive_id = operation("receive-set");
    scenario.steps.push(step(
        "receive-set",
        ScenarioAction::GroupReceiveSet(GroupReceiveSetAction {
            receive_id: receive_id.clone(),
            consumer_ids: vec![consumer("group-1")],
            expected_operation_ids: vec![operation("op-1"), operation("op-2")],
            timeout_ms: 1_000,
        }),
    ));
    (scenario, receive_id)
}

fn share_scenario() -> (testlab_schema::Scenario, OperationId) {
    let mut scenario = base_scenario();
    let receive_id = operation("receive-share");
    scenario.steps.push(step(
        "receive-share",
        ScenarioAction::ShareReceive {
            consumer_id: consumer("share-1"),
            receive_id: receive_id.clone(),
            expected_operation_ids: vec![operation("op-1")],
            minimum_delivery_count: 1,
            expected_acquisition_count: None,
            timeout_ms: 1_000,
        },
    ));
    (scenario, receive_id)
}

fn base_scenario() -> testlab_schema::Scenario {
    scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    )
}

fn share_event(receive_id: OperationId, record: ConsumedRecord) -> AdapterEvent {
    AdapterEvent::ShareReceiveCompleted {
        consumer_id: consumer("share-1"),
        receive_id,
        records: vec![ShareConsumedRecord {
            record,
            delivery_count: 1,
        }],
        acquisition_count: 1,
        member_epoch: Some(1),
        assignment_epoch: Some(1),
    }
}

fn consumed(value: &str, offset: i64) -> ConsumedRecord {
    ConsumedRecord {
        topic: "records".to_owned(),
        partition: 0,
        offset,
        timestamp_millis: None,
        key: None,
        value: Some(ByteString::utf8(value)),
        headers: Vec::new(),
    }
}

fn observed(operation_id: &str, value: &str, offset: i64, observation: u64) -> BrokerObservation {
    let record = record(value);
    let digest = record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    BrokerObservation {
        observation,
        offset,
        operation_id: operation(operation_id),
        record,
        digest,
    }
}

fn verify(
    scenario: &testlab_schema::Scenario,
    history: &[testlab_schema::HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::record_offsets::verify(scenario, &index, observations, &mut violations);
    violations
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(violates(violations, contract), "violations: {violations:?}");
}

fn violates(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
