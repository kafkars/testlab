use crate::{
    AdapterCommand, AdapterEvent, AlterPartitionReassignmentsAction,
    AlterPartitionReassignmentsCommand, BrokerPartitionAssignmentsState,
    BrokerPartitionReassignmentsState, BrokerStateObservation, ClientId,
    ListPartitionReassignmentsAction, ListPartitionReassignmentsCommand, OperationId,
    PartitionReassignmentChangeSpec, PartitionReassignmentSelection, Scenario, ScenarioAction,
};

#[test]
fn checked_in_reassignment_scenario_is_valid() {
    checked_in_scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate reassignment scenario: {error}"));
}

#[test]
fn reassignment_validation_rejects_duplicate_targets_and_implicit_factor_changes() {
    let mut duplicate = checked_in_scenario();
    let alter = duplicate
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::AlterPartitionReassignments(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("alter reassignment action"));
    alter.changes.push(alter.changes[0].clone());
    let problems = validation_problems(&duplicate);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("repeats a reassignment target"))
    );

    let mut factor = checked_in_scenario();
    let alter = factor
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::AlterPartitionReassignments(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("alter reassignment action"));
    alter.allow_replication_factor_change = false;
    let problems = validation_problems(&factor);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("changes replication factor without permission"))
    );
}

#[test]
fn reassignment_payloads_round_trip_without_fixture_expectations_on_wire() {
    let alter = AlterPartitionReassignmentsAction {
        client_id: client(),
        operation_id: operation("alter"),
        changes: vec![change("orders", 1, &[3, 2])],
        allow_replication_factor_change: true,
        timeout_ms: 20_000,
    };
    round_trip(&ScenarioAction::AlterPartitionReassignments(alter.clone()));
    round_trip(&AdapterCommand::AlterPartitionReassignments(
        AlterPartitionReassignmentsCommand {
            client_id: alter.client_id,
            operation_id: alter.operation_id,
            changes: alter.changes,
            allow_replication_factor_change: alter.allow_replication_factor_change,
            timeout_ms: alter.timeout_ms,
        },
    ));
    let list = ListPartitionReassignmentsAction {
        client_id: client(),
        operation_id: operation("list"),
        targets: Some(vec![PartitionReassignmentSelection {
            topic: "orders".to_owned(),
            partition: 1,
        }]),
        expected_reassignments: Vec::new(),
        timeout_ms: 20_000,
    };
    round_trip(&ScenarioAction::ListPartitionReassignments(list.clone()));
    round_trip(&AdapterCommand::ListPartitionReassignments(
        ListPartitionReassignmentsCommand {
            client_id: list.client_id,
            operation_id: list.operation_id,
            targets: list.targets,
            timeout_ms: list.timeout_ms,
        },
    ));
}

#[test]
fn reassignment_evidence_variants_round_trip() {
    round_trip(&BrokerStateObservation::PartitionAssignments(
        BrokerPartitionAssignmentsState {
            observation: 7,
            operation_id: operation("alter"),
            topic_partitions: Vec::new(),
            assignments: Vec::new(),
        },
    ));
    round_trip(&BrokerStateObservation::PartitionReassignments(
        BrokerPartitionReassignmentsState {
            observation: 8,
            operation_id: operation("list"),
            reassignments: Vec::new(),
        },
    ));
    let value = serde_json::to_value(AdapterEvent::PartitionReassignmentsListed(
        crate::AdminPartitionReassignmentsListing {
            operation_id: operation("list"),
            throttle_time_ms: 0,
            reassignments: Vec::new(),
        },
    ))
    .unwrap_or_else(|error| panic!("serialize reassignment event: {error}"));
    let _: AdapterEvent = serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("deserialize reassignment event: {error}"));
}

fn change(topic: &str, partition: i32, replicas: &[i32]) -> PartitionReassignmentChangeSpec {
    PartitionReassignmentChangeSpec {
        topic: topic.to_owned(),
        partition,
        replicas: replicas.to_vec(),
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(format!("reassignment-{value}"))
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn checked_in_scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-partition-reassignments.toml"
    ))
    .unwrap_or_else(|error| panic!("parse reassignment scenario: {error}"))
}

fn validation_problems(scenario: &Scenario) -> Vec<String> {
    match scenario.validate() {
        Ok(()) => panic!("invalid reassignment scenario passed"),
        Err(error) => error.problems,
    }
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Eq + std::fmt::Debug,
{
    let json = serde_json::to_vec(value)
        .unwrap_or_else(|error| panic!("serialize reassignment payload: {error}"));
    let decoded = serde_json::from_slice::<T>(&json)
        .unwrap_or_else(|error| panic!("deserialize reassignment payload: {error}"));
    assert_eq!(&decoded, value);
}
