//! Manual partition-placement verifier tests pin exact observation contracts.

use testlab_schema::{
    AdapterCommand, AdapterEvent, BrokerPartitionAssignment, BrokerPartitionAssignmentsState,
    BrokerStateObservation, Capability, CreatePartitionsAction, CreatePartitionsCommand,
    HistoryEntry, HistoryPayload, OperationId, ScenarioAction, TerminalStatus,
    VisibilityExpectation,
};

use crate::admin::test_verdict as admin_verdict;
use crate::verify_fixture::{command, event, history, scenario, step};

#[test]
fn exact_manual_partition_expansion_passes() {
    let (scenario, events) = fixture();

    let verdict = admin_verdict(&scenario, &events);

    assert!(verdict.is_passed(), "{verdict:?}");
}

#[test]
fn changed_replica_order_fails_manual_partition_expansion() {
    let (scenario, mut events) = fixture();
    assignments(&mut events)[0].replicas.reverse();

    let verdict = admin_verdict(&scenario, &events);

    assert_contract(&verdict);
}

#[test]
fn incomplete_isr_fails_manual_partition_expansion() {
    let (scenario, mut events) = fixture();
    assignments(&mut events)[1].in_sync_replicas.pop();

    let verdict = admin_verdict(&scenario, &events);

    assert_contract(&verdict);
}

#[test]
fn extra_topic_partition_fails_manual_partition_expansion() {
    let (scenario, mut events) = fixture();
    topic_partitions(&mut events).push(3);

    let verdict = admin_verdict(&scenario, &events);

    assert_contract(&verdict);
}

fn fixture() -> (testlab_schema::Scenario, Vec<HistoryEntry>) {
    let mut scenario = scenario(
        TerminalStatus::Acknowledged,
        VisibilityExpectation::ExactlyOnce,
    );
    scenario.requires.insert(Capability::Admin);
    let operation_id = id(OperationId::new("manual-expand"));
    let expected = vec![vec![3, 1], vec![2, 3]];
    scenario.steps.insert(
        2,
        step(
            "manual-expand",
            ScenarioAction::CreatePartitions(CreatePartitionsAction {
                client_id: id(testlab_schema::ClientId::new("client-1")),
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                total_count: 3,
                replica_assignments: Some(expected.clone()),
                validate_only: false,
                expected_current_count: Some(1),
                expected_error_code: None,
                timeout_ms: 1_000,
            }),
        ),
    );
    let mut events = history(TerminalStatus::Acknowledged);
    events.push(command(
        10,
        AdapterCommand::CreatePartitions(CreatePartitionsCommand {
            client_id: id(testlab_schema::ClientId::new("client-1")),
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
            total_count: 3,
            replica_assignments: Some(expected),
            validate_only: false,
            timeout_ms: 1_000,
        }),
    ));
    events.push(event(
        11,
        AdapterEvent::TopicPartitionsCreated(testlab_schema::AdminTopicCompletion {
            operation_id: operation_id.clone(),
            topic: "orders".to_owned(),
        }),
    ));
    events.push(assignment_state(12, operation_id));
    (scenario, events)
}

fn assignment_state(sequence: u64, operation_id: OperationId) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::PartitionAssignments(
                BrokerPartitionAssignmentsState {
                    observation: sequence,
                    operation_id,
                    topic_partitions: vec![testlab_schema::BrokerTopicPartitionSet {
                        topic: "orders".to_owned(),
                        partitions: vec![0, 1, 2],
                    }],
                    assignments: vec![broker_assignment(1, &[3, 1]), broker_assignment(2, &[2, 3])],
                },
            ),
        },
    }
}

fn broker_assignment(partition: i32, replicas: &[i32]) -> BrokerPartitionAssignment {
    let mut in_sync_replicas = replicas.to_vec();
    in_sync_replicas.sort_unstable();
    BrokerPartitionAssignment {
        topic: "orders".to_owned(),
        partition,
        leader_id: replicas[0],
        replicas: replicas.to_vec(),
        in_sync_replicas,
    }
}

fn assignments(events: &mut [HistoryEntry]) -> &mut Vec<BrokerPartitionAssignment> {
    events
        .iter_mut()
        .find_map(|entry| match &mut entry.payload {
            HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::PartitionAssignments(state),
            } => Some(&mut state.assignments),
            _ => None,
        })
        .unwrap_or_else(|| panic!("manual partition assignment observation"))
}

fn topic_partitions(events: &mut [HistoryEntry]) -> &mut Vec<i32> {
    events
        .iter_mut()
        .find_map(|entry| match &mut entry.payload {
            HistoryPayload::BrokerStateObservation {
                observation: BrokerStateObservation::PartitionAssignments(state),
            } => state
                .topic_partitions
                .first_mut()
                .map(|topic| &mut topic.partitions),
            _ => None,
        })
        .unwrap_or_else(|| panic!("manual partition topic set"))
}

fn assert_contract(verdict: &testlab_schema::Verdict) {
    assert!(
        verdict
            .violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == "ADMIN-085"),
        "{verdict:?}"
    );
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture ID: {error}"))
}
