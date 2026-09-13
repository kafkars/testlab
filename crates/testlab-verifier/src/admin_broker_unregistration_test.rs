//! Broker-unregistration tests keep public, independent, and restoration evidence distinct.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdminBrokerUnregistration, AdminClusterDescription,
    BrokerClusterState, BrokerStateObservation, DescribeClusterCommand, EnvironmentOperation,
    EnvironmentOperationId, EnvironmentOperationKind, EnvironmentOperationStatus, HistoryEntry,
    HistoryPayload, Scenario, UnregisterBrokerCommand,
};

use crate::admin::verify_admin;
use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

const BASELINE: &str = "admin-unregister-broker-baseline";
const UNREGISTER: &str = "admin-unregister-broker-3";
const RESTORED: &str = "admin-unregister-broker-restored";

#[test]
fn exact_public_unregistration_and_restoration_pass() {
    assert!(violations(history(false)).is_empty());
}

#[test]
fn remaining_cluster_observation_must_precede_the_next_command() {
    assert_contract(&violations(history(true)), "ADMIN-076");
}

#[test]
fn wrong_public_broker_identity_fails() {
    let mut history = history(false);
    let Some(entry) = history.iter_mut().find(|entry| {
        matches!(
            &entry.payload,
            HistoryPayload::AdapterEvent { event }
                if matches!(&event.event, AdapterEvent::BrokerUnregistered(_))
        )
    }) else {
        panic!("missing public broker-unregistration completion");
    };
    let HistoryPayload::AdapterEvent { event } = &mut entry.payload else {
        unreachable!();
    };
    let AdapterEvent::BrokerUnregistered(value) = &mut event.event else {
        unreachable!();
    };
    value.broker_id = 2;

    assert_contract(&violations(history), "ADMIN-076");
}

#[test]
fn failed_broker_stop_terminal_fails() {
    let mut history = history(false);
    let Some(entry) = history.iter_mut().find(|entry| {
        matches!(
            &entry.payload,
            HistoryPayload::EnvironmentOperation { operation }
                if operation.kind == EnvironmentOperationKind::BrokerStop
        )
    }) else {
        panic!("missing broker stop terminal");
    };
    let HistoryPayload::EnvironmentOperation { operation } = &mut entry.payload else {
        unreachable!();
    };
    operation.status = EnvironmentOperationStatus::Failed;
    assert_contract(&violations(history), "ADMIN-076");
}

fn history(intervening_command: bool) -> Vec<HistoryEntry> {
    let client_id = client();
    let baseline = operation(BASELINE);
    let unregister = operation(UNREGISTER);
    let restored = operation(RESTORED);
    let mut history = vec![
        command(
            0,
            AdapterCommand::DescribeCluster(DescribeClusterCommand {
                client_id: client_id.clone(),
                operation_id: baseline.clone(),
                include_authorized_operations: false,
                timeout_ms: 20_000,
            }),
        ),
        event(
            1,
            AdapterEvent::ClusterDescribed(AdminClusterDescription {
                operation_id: baseline.clone(),
                cluster_id: Some("cluster-a".to_owned()),
                broker_ids: vec![1, 2, 3],
                authorized_operations: None,
            }),
        ),
        state(2, baseline, vec![1, 2, 3]),
        environment(3, true),
        command(
            4,
            AdapterCommand::UnregisterBroker(UnregisterBrokerCommand {
                client_id: client_id.clone(),
                operation_id: unregister.clone(),
                broker_id: 3,
                timeout_ms: 30_000,
            }),
        ),
        event(
            5,
            AdapterEvent::BrokerUnregistered(AdminBrokerUnregistration {
                operation_id: unregister.clone(),
                broker_id: 3,
                throttle_time_ms: 0,
            }),
        ),
    ];
    let mut sequence = 6;
    if intervening_command {
        history.push(command(sequence, AdapterCommand::Finish));
        sequence += 1;
    }
    history.extend([
        state(sequence, unregister, vec![1, 2]),
        environment(sequence + 1, false),
        command(
            sequence + 2,
            AdapterCommand::DescribeCluster(DescribeClusterCommand {
                client_id,
                operation_id: restored.clone(),
                include_authorized_operations: false,
                timeout_ms: 30_000,
            }),
        ),
        event(
            sequence + 3,
            AdapterEvent::ClusterDescribed(AdminClusterDescription {
                operation_id: restored.clone(),
                cluster_id: Some("cluster-a".to_owned()),
                broker_ids: vec![1, 2, 3],
                authorized_operations: None,
            }),
        ),
        state(sequence + 4, restored, vec![1, 2, 3]),
    ]);
    history
}

fn environment(sequence: u64, stop: bool) -> HistoryEntry {
    let action = if stop { "stop" } else { "start" };
    let kind = if stop {
        EnvironmentOperationKind::BrokerStop
    } else {
        EnvironmentOperationKind::BrokerStart
    };
    let mut args = [
        "compose",
        "--project-name",
        "testlab-unregister",
        "--file",
        "cluster.yml",
    ]
    .map(str::to_owned)
    .to_vec();
    if stop {
        args.push("stop".to_owned());
    } else {
        args.extend(["restart".to_owned(), "--no-deps".to_owned()]);
    }
    args.push("broker-3".to_owned());
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::EnvironmentOperation {
            operation: EnvironmentOperation {
                id: EnvironmentOperationId::new(format!("{action}-broker-3"))
                    .unwrap_or_else(|error| panic!("environment operation id: {error}")),
                kind,
                program: "docker".to_owned(),
                args,
                started_unix_ms: sequence,
                completed_unix_ms: sequence,
                status: EnvironmentOperationStatus::Succeeded,
                exit_code: Some(0),
                stdout_artifact: None,
                stderr_artifact: None,
                diagnostic: None,
            },
        },
    }
}

fn state(
    sequence: u64,
    operation_id: testlab_schema::OperationId,
    broker_ids: Vec<i32>,
) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::BrokerStateObservation {
            observation: BrokerStateObservation::Cluster(BrokerClusterState {
                observation: sequence,
                operation_id,
                cluster_id: Some("cluster-a".to_owned()),
                broker_ids,
            }),
        },
    }
}

fn violations(history: Vec<HistoryEntry>) -> Vec<testlab_schema::Violation> {
    let scenario = scenario();
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    violations
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-unregister-broker.toml"
    ))
    .unwrap_or_else(|error| panic!("parse broker-unregistration scenario: {error}"))
}

fn client() -> testlab_schema::ClientId {
    testlab_schema::ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> testlab_schema::OperationId {
    testlab_schema::OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn assert_contract(violations: &[testlab_schema::Violation], contract: &str) {
    assert!(
        violations
            .iter()
            .any(|violation| violation.contract_id.as_str() == contract),
        "{violations:?}"
    );
}
