//! Redistribution tests require partition ownership changes, not metadata-only churn.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterEvent, ConsumerId, GroupAssignmentTransition, GroupAssignmentTransitionKind,
    GroupAssignmentsObservation, GroupConsumerAssignment, GroupMembershipEpoch,
    ObserveGroupAssignmentsAction, OperationId, Scenario, ScenarioAction, ScenarioId,
    TopicPartitionIdentity,
};

use crate::group_redistribution::verify;
use crate::index::HistoryIndex;
use crate::verify_fixture::{event, step};

#[test]
fn join_and_leave_owner_changes_pass() {
    let (scenario, history) = fixture(true);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn metadata_only_member_change_fails() {
    let (scenario, history) = fixture(false);
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "CONS-010")
    );
}

fn fixture(redistributed: bool) -> (Scenario, Vec<testlab_schema::HistoryEntry>) {
    let first = consumer("consumer-1");
    let second = consumer("consumer-2");
    let initial = operation("observe-initial");
    let joined = operation("observe-joined");
    let left = operation("observe-left");
    let expected = vec![partition(0), partition(1)];
    let scenario = Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("consumer.redistribution")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "redistribution".to_owned(),
        description: "redistribution fixture".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::new(),
        steps: vec![
            observation_step("initial", &initial, vec![first.clone()], expected.clone()),
            observation_step(
                "joined",
                &joined,
                vec![first.clone(), second.clone()],
                expected.clone(),
            ),
            observation_step("left", &left, vec![first.clone()], expected),
        ],
        assertions: Vec::new(),
    };
    let joined_assignments = if redistributed {
        vec![
            assignment(&first, vec![partition(0)]),
            assignment(&second, vec![partition(1)]),
        ]
    } else {
        vec![
            assignment(&first, vec![partition(0), partition(1)]),
            assignment(&second, Vec::new()),
        ]
    };
    let history = vec![
        assignment_event(
            0,
            initial,
            vec![assignment(&first, vec![partition(0), partition(1)])],
            false,
        ),
        assignment_event(1, joined, joined_assignments, true),
        assignment_event(
            2,
            left,
            vec![assignment(&first, vec![partition(0), partition(1)])],
            true,
        ),
    ];
    (scenario, history)
}

fn observation_step(
    id: &str,
    operation_id: &OperationId,
    consumer_ids: Vec<ConsumerId>,
    partitions: Vec<TopicPartitionIdentity>,
) -> testlab_schema::ScenarioStep {
    step(
        id,
        ScenarioAction::ObserveGroupAssignments(ObserveGroupAssignmentsAction {
            operation_id: operation_id.clone(),
            consumer_ids,
            partitions,
            timeout_ms: 1_000,
        }),
    )
}

fn assignment_event(
    sequence: u64,
    operation_id: OperationId,
    assignments: Vec<GroupConsumerAssignment>,
    transitioned: bool,
) -> testlab_schema::HistoryEntry {
    let transitions = transitioned
        .then(|| GroupAssignmentTransition {
            consumer_id: assignments[0].consumer_id.clone(),
            kind: GroupAssignmentTransitionKind::Assigned,
            assignment_epoch: 1,
            partitions: assignments[0].partitions.clone(),
        })
        .into_iter()
        .collect();
    event(
        sequence,
        AdapterEvent::GroupAssignmentsObserved(GroupAssignmentsObservation {
            operation_id,
            transitions,
            assignments,
        }),
    )
}

fn assignment(
    consumer_id: &ConsumerId,
    partitions: Vec<TopicPartitionIdentity>,
) -> GroupConsumerAssignment {
    GroupConsumerAssignment {
        consumer_id: consumer_id.clone(),
        group_id: "group-1".to_owned(),
        member_id: format!("member-{consumer_id}"),
        group_epoch: GroupMembershipEpoch::Classic { generation_id: 1 },
        assignment_epoch: 1,
        partitions,
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
