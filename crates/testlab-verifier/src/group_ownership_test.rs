//! Ownership verifier tests prove complete disjoint assignment and record attribution.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterEvent, ByteString, ConsumedRecord, ConsumerId, GroupAssignmentsObservation,
    GroupConsumerAssignment, GroupMembershipEpoch, GroupProtocol, GroupReceiveMemberCompletion,
    GroupReceiveSetAction, GroupReceiveSetCompletion, ObserveGroupAssignmentsAction, OperationId,
    RecordSpec, Scenario, ScenarioAction, ScenarioId, TopicPartitionIdentity,
};

use crate::group_ownership::verify;
use crate::index::HistoryIndex;
use crate::verify_fixture::{event, step};

#[test]
fn disjoint_assignments_and_owned_records_pass() {
    let (scenario, history) = fixture();
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn record_from_another_members_partition_fails() {
    let (scenario, mut history) = fixture();
    let testlab_schema::HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("expected adapter event");
    };
    let AdapterEvent::GroupReceiveSetCompleted(completion) = &mut event.event else {
        panic!("expected receive set");
    };
    completion.members[1].records[0].partition = 0;
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert!(
        violations
            .iter()
            .any(|violation| { violation.contract_id.as_str() == "CONS-009" })
    );
}

#[test]
fn missing_assignment_observation_fails() {
    let (scenario, mut history) = fixture();
    history.remove(0);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert_contract(&violations, "CONS-005");
}

#[test]
fn overlapping_assignments_fail() {
    let (scenario, mut history) = fixture();
    assignments_mut(&mut history)[1].partitions[0] = partition(0);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert_contract(&violations, "CONS-006");
}

#[test]
fn invalid_assignment_fence_fails() {
    let (scenario, mut history) = fixture();
    assignments_mut(&mut history)[0].assignment_epoch = 0;
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert_contract(&violations, "CONS-007");
}

#[test]
fn uncommitted_receive_set_fails() {
    let (scenario, mut history) = fixture();
    let testlab_schema::HistoryPayload::AdapterEvent { event } = &mut history[1].payload else {
        panic!("expected adapter event");
    };
    let AdapterEvent::GroupReceiveSetCompleted(completion) = &mut event.event else {
        panic!("expected receive set");
    };
    completion.members[0].committed = false;
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert_contract(&violations, "CONS-008");
}

fn assignments_mut(
    history: &mut [testlab_schema::HistoryEntry],
) -> &mut Vec<GroupConsumerAssignment> {
    let testlab_schema::HistoryPayload::AdapterEvent { event } = &mut history[0].payload else {
        panic!("expected adapter event");
    };
    let AdapterEvent::GroupAssignmentsObserved(observation) = &mut event.event else {
        panic!("expected assignment observation");
    };
    &mut observation.assignments
}

fn assert_contract(violations: &[testlab_schema::Violation], contract_id: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract_id),
        "missing {contract_id}: {violations:?}"
    );
}

fn fixture() -> (Scenario, Vec<testlab_schema::HistoryEntry>) {
    let first = consumer("consumer-1");
    let second = consumer("consumer-2");
    let observe = operation("observe-1");
    let receive = operation("receive-set-1");
    let send_one = operation("send-1");
    let send_two = operation("send-2");
    let scenario = Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("consumer.ownership")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "ownership".to_owned(),
        description: "ownership fixture".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::new(),
        steps: vec![
            group_step("create-1", &first),
            group_step("create-2", &second),
            step(
                "send-1",
                ScenarioAction::Send {
                    producer_id: testlab_schema::ProducerId::new("producer-1")
                        .unwrap_or_else(|error| panic!("producer id: {error}")),
                    operation_id: send_one.clone(),
                    record: record(0, "one"),
                },
            ),
            step(
                "send-2",
                ScenarioAction::Send {
                    producer_id: testlab_schema::ProducerId::new("producer-1")
                        .unwrap_or_else(|error| panic!("producer id: {error}")),
                    operation_id: send_two.clone(),
                    record: record(1, "two"),
                },
            ),
            step(
                "observe",
                ScenarioAction::ObserveGroupAssignments(ObserveGroupAssignmentsAction {
                    operation_id: observe.clone(),
                    consumer_ids: vec![first.clone(), second.clone()],
                    partitions: vec![partition(0), partition(1)],
                    timeout_ms: 1_000,
                }),
            ),
            step(
                "receive",
                ScenarioAction::GroupReceiveSet(GroupReceiveSetAction {
                    receive_id: receive.clone(),
                    consumer_ids: vec![first.clone(), second.clone()],
                    expected_operation_ids: vec![send_one, send_two],
                    timeout_ms: 1_000,
                }),
            ),
        ],
        assertions: Vec::new(),
    };
    let assignments = vec![assignment(&first, 0, 1), assignment(&second, 1, 2)];
    let history = vec![
        event(
            0,
            AdapterEvent::GroupAssignmentsObserved(GroupAssignmentsObservation {
                operation_id: observe,
                transitions: Vec::new(),
                assignments,
            }),
        ),
        event(
            1,
            AdapterEvent::GroupReceiveSetCompleted(GroupReceiveSetCompletion {
                receive_id: receive,
                members: vec![
                    member(&first, consumed(0, "one")),
                    member(&second, consumed(1, "two")),
                ],
            }),
        ),
    ];
    (scenario, history)
}

fn group_step(id: &str, consumer_id: &ConsumerId) -> testlab_schema::ScenarioStep {
    step(
        id,
        ScenarioAction::CreateGroupConsumer {
            client_id: testlab_schema::ClientId::new("client-1")
                .unwrap_or_else(|error| panic!("client id: {error}")),
            consumer_id: consumer_id.clone(),
            group_id: "group-1".to_owned(),
            topic: "records".to_owned(),
            protocol: GroupProtocol::Classic,
            configuration: None,
        },
    )
}

fn assignment(
    consumer_id: &ConsumerId,
    partition_index: i32,
    epoch: u64,
) -> GroupConsumerAssignment {
    GroupConsumerAssignment {
        consumer_id: consumer_id.clone(),
        group_id: "group-1".to_owned(),
        member_id: format!("member-{partition_index}"),
        group_epoch: GroupMembershipEpoch::Classic { generation_id: 1 },
        assignment_epoch: epoch,
        partitions: vec![partition(partition_index)],
    }
}

fn member(consumer_id: &ConsumerId, record: ConsumedRecord) -> GroupReceiveMemberCompletion {
    GroupReceiveMemberCompletion {
        consumer_id: consumer_id.clone(),
        records: vec![record],
        committed: true,
        group_epoch: Some(GroupMembershipEpoch::Classic { generation_id: 1 }),
    }
}

fn record(partition: i32, value: &str) -> RecordSpec {
    RecordSpec {
        topic: "records".to_owned(),
        partition,
        sequence: u64::from(partition.unsigned_abs()),
        key: None,
        value: Some(ByteString::utf8(value)),
        headers: Vec::new(),
    }
}

fn consumed(partition: i32, value: &str) -> ConsumedRecord {
    ConsumedRecord {
        topic: "records".to_owned(),
        partition,
        offset: 0,
        timestamp_millis: None,
        key: None,
        value: Some(ByteString::hex(value.as_bytes())),
        headers: Vec::new(),
    }
}

fn partition(partition: i32) -> TopicPartitionIdentity {
    TopicPartitionIdentity {
        topic: "records".to_owned(),
        partition,
    }
}

fn consumer(value: &str) -> ConsumerId {
    ConsumerId::new(value).unwrap_or_else(|error| panic!("consumer id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}
