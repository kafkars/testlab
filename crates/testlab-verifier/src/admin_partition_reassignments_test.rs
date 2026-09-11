//! Reassignment verdicts pin caller order, exact assignments, and CLI timing.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminPartitionReassignmentOutcome,
    AdminPartitionReassignmentsAlteration, AdminPartitionReassignmentsListing,
    AlterPartitionReassignmentsAction, AlterPartitionReassignmentsCommand,
    BrokerPartitionAssignment, BrokerPartitionAssignmentsState, BrokerPartitionReassignmentsState,
    BrokerStateObservation, HistoryEntry, HistoryPayload, ListPartitionReassignmentsAction,
    ListPartitionReassignmentsCommand, OperationId, PartitionReassignmentChangeSpec,
    PartitionReassignmentSelection, PartitionReassignmentSnapshot, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event, scenario, step};

#[test]
fn exact_mutation_and_selected_listing_pass() {
    assert!(alter_violations(alter_history()).is_empty());
    assert!(list_violations(list_history(), Some(targets())).is_empty());
}

#[test]
fn exact_all_active_listing_passes() {
    let rows = canonical_rows();
    let history = vec![
        command(0, list_command(None)),
        event(1, listed_event(rows.clone())),
        state(2, listed_state(rows)),
    ];
    assert!(list_violations(history, None).is_empty());
}

#[test]
fn mutation_order_assignment_and_timing_mismatches_fail() {
    let mut reordered = alter_history();
    altered(&mut reordered).outcomes.reverse();
    assert_contract(&alter_violations(reordered), "ADMIN-058");

    let mut wrong_assignment = alter_history();
    assignments(&mut wrong_assignment).assignments[0]
        .replicas
        .reverse();
    assert_contract(&alter_violations(wrong_assignment), "ADMIN-058");

    let mut delayed = alter_history();
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    assert_contract(&alter_violations(delayed), "ADMIN-058");
}

#[test]
fn listing_public_order_cli_state_and_timing_mismatches_fail() {
    let mut reordered = list_history();
    listed(&mut reordered).reassignments.reverse();
    assert_contract(&list_violations(reordered, Some(targets())), "ADMIN-059");

    let mut missing = list_history();
    listed_cli(&mut missing).reassignments.pop();
    assert_contract(&list_violations(missing, Some(targets())), "ADMIN-059");

    let mut delayed = list_history();
    delayed.insert(2, command(2, AdapterCommand::Finish));
    delayed[3].sequence = 3;
    assert_contract(&list_violations(delayed, Some(targets())), "ADMIN-059");
}

fn alter_history() -> Vec<HistoryEntry> {
    let changes = changes();
    vec![
        command(
            0,
            AdapterCommand::AlterPartitionReassignments(AlterPartitionReassignmentsCommand {
                client_id: client(),
                operation_id: alter_operation(),
                changes: changes.clone(),
                allow_replication_factor_change: true,
                timeout_ms: 20_000,
            }),
        ),
        event(
            1,
            AdapterEvent::PartitionReassignmentsAltered(AdminPartitionReassignmentsAlteration {
                operation_id: alter_operation(),
                throttle_time_ms: 3,
                outcomes: changes
                    .iter()
                    .map(|change| AdminPartitionReassignmentOutcome {
                        topic: change.topic.clone(),
                        partition: change.partition,
                        error_code: None,
                    })
                    .collect(),
            }),
        ),
        state(
            2,
            BrokerStateObservation::PartitionAssignments(BrokerPartitionAssignmentsState {
                observation: 7,
                operation_id: alter_operation(),
                assignments: changes
                    .iter()
                    .map(|change| BrokerPartitionAssignment {
                        topic: change.topic.clone(),
                        partition: change.partition,
                        leader_id: change.replicas[0],
                        replicas: change.replicas.clone(),
                        in_sync_replicas: sorted(change.replicas.clone()),
                    })
                    .collect(),
            }),
        ),
    ]
}

fn list_history() -> Vec<HistoryEntry> {
    let canonical = canonical_rows();
    let selected = vec![canonical[1].clone(), canonical[0].clone()];
    vec![
        command(0, list_command(Some(targets()))),
        event(1, listed_event(selected)),
        state(2, listed_state(canonical)),
    ]
}

fn alter_violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let action = ScenarioAction::AlterPartitionReassignments(AlterPartitionReassignmentsAction {
        client_id: client(),
        operation_id: alter_operation(),
        changes: changes(),
        allow_replication_factor_change: true,
        timeout_ms: 20_000,
    });
    violations(history, action)
}

fn list_violations(
    history: Vec<HistoryEntry>,
    targets: Option<Vec<PartitionReassignmentSelection>>,
) -> Vec<testlab_schema::Violation> {
    let expected_reassignments = if targets.is_some() {
        let rows = canonical_rows();
        vec![rows[1].clone(), rows[0].clone()]
    } else {
        canonical_rows()
    };
    violations(
        history,
        ScenarioAction::ListPartitionReassignments(ListPartitionReassignmentsAction {
            client_id: client(),
            operation_id: list_operation(),
            targets,
            expected_reassignments,
            timeout_ms: 20_000,
        }),
    )
}

fn violations(
    history: Vec<HistoryEntry>,
    action: ScenarioAction,
) -> Vec<testlab_schema::Violation> {
    let mut fixture = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    fixture
        .steps
        .insert(2, step("partition-reassignments", action));
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&fixture, &index, &[], &mut violations);
    violations
}
fn changes() -> Vec<PartitionReassignmentChangeSpec> {
    vec![change("zeta", 1, &[3, 2]), change("alpha", 0, &[2, 1])]
}

fn change(topic: &str, partition: i32, replicas: &[i32]) -> PartitionReassignmentChangeSpec {
    PartitionReassignmentChangeSpec {
        topic: topic.to_owned(),
        partition,
        replicas: replicas.to_vec(),
    }
}
fn targets() -> Vec<PartitionReassignmentSelection> {
    vec![target("zeta", 1), target("alpha", 0)]
}

fn target(topic: &str, partition: i32) -> PartitionReassignmentSelection {
    PartitionReassignmentSelection {
        topic: topic.to_owned(),
        partition,
    }
}
fn canonical_rows() -> Vec<PartitionReassignmentSnapshot> {
    vec![snapshot("alpha", 0, &[2, 1]), snapshot("zeta", 1, &[3, 2])]
}

fn snapshot(topic: &str, partition: i32, replicas: &[i32]) -> PartitionReassignmentSnapshot {
    PartitionReassignmentSnapshot {
        topic: topic.to_owned(),
        partition,
        replicas: replicas.to_vec(),
        adding_replicas: vec![replicas[0]],
        removing_replicas: vec![1],
    }
}
fn list_command(targets: Option<Vec<PartitionReassignmentSelection>>) -> AdapterCommand {
    AdapterCommand::ListPartitionReassignments(ListPartitionReassignmentsCommand {
        client_id: client(),
        operation_id: list_operation(),
        targets,
        timeout_ms: 20_000,
    })
}
fn listed_event(reassignments: Vec<PartitionReassignmentSnapshot>) -> AdapterEvent {
    AdapterEvent::PartitionReassignmentsListed(AdminPartitionReassignmentsListing {
        operation_id: list_operation(),
        throttle_time_ms: 2,
        reassignments,
    })
}
fn listed_state(reassignments: Vec<PartitionReassignmentSnapshot>) -> BrokerStateObservation {
    BrokerStateObservation::PartitionReassignments(BrokerPartitionReassignmentsState {
        observation: 8,
        operation_id: list_operation(),
        reassignments,
    })
}
fn state(sequence: u64, observation: BrokerStateObservation) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation { observation },
    }
}
fn altered(entries: &mut [HistoryEntry]) -> &mut AdminPartitionReassignmentsAlteration {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("alteration event");
    };
    let AdapterEvent::PartitionReassignmentsAltered(value) = &mut event.event else {
        panic!("alteration payload");
    };
    value
}
fn assignments(entries: &mut [HistoryEntry]) -> &mut BrokerPartitionAssignmentsState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2].payload else {
        panic!("assignment observation");
    };
    let BrokerStateObservation::PartitionAssignments(value) = observation else {
        panic!("assignment state");
    };
    value
}
fn listed(entries: &mut [HistoryEntry]) -> &mut AdminPartitionReassignmentsListing {
    let HistoryPayload::AdapterEvent { event } = &mut entries[1].payload else {
        panic!("listing event");
    };
    let AdapterEvent::PartitionReassignmentsListed(value) = &mut event.event else {
        panic!("listing payload");
    };
    value
}
fn listed_cli(entries: &mut [HistoryEntry]) -> &mut BrokerPartitionReassignmentsState {
    let HistoryPayload::BrokerStateObservation { observation } = &mut entries[2].payload else {
        panic!("listing observation");
    };
    let BrokerStateObservation::PartitionReassignments(value) = observation else {
        panic!("listing state");
    };
    value
}
fn sorted(mut values: Vec<i32>) -> Vec<i32> {
    values.sort_unstable();
    values
}
fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}
fn alter_operation() -> OperationId {
    OperationId::new("admin-alter-reassignments")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
fn list_operation() -> OperationId {
    OperationId::new("admin-list-reassignments")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}
