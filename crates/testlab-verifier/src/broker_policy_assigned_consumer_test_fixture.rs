//! Scenario, evidence, and history fixtures for assigned-consumer policy recovery.

use testlab_schema::{
    AdapterCommand, AdapterEvent, AssignedConsumerEventExpectation,
    AssignedConsumerEventObservation, AssignedConsumerEventObservationKind,
    AssignedConsumerPositionFenceObservation, BrokerObservation, BrokerPolicy, BrokerPolicyState,
    ConsumedRecord, EnvironmentOperation, EnvironmentOperationId, EnvironmentOperationKind,
    EnvironmentOperationStatus, HistoryEntry, HistoryPayload, ObserveAssignedConsumerEventCommand,
    Scenario, ScenarioAction,
};

use crate::index::HistoryIndex;
use crate::verify_fixture::{command, event};

pub(super) fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/assigned-consumer-event-authorization-recovery.toml"
    ))
    .unwrap_or_else(|error| panic!("assigned event scenario: {error}"))
}

pub(super) fn history(scenario: &Scenario) -> Vec<HistoryEntry> {
    let policy = policy(scenario);
    let mut history = policy_entries(0, &policy, BrokerPolicyState::Present);
    for action in scenario.steps.iter().filter_map(|step| match &step.action {
        ScenarioAction::ObserveAssignedConsumerEvent(action) => Some(action),
        _ => None,
    }) {
        let sequence = history.len() as u64;
        history.push(command(
            sequence,
            AdapterCommand::ObserveAssignedConsumerEvent(ObserveAssignedConsumerEventCommand {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                method: action.method,
                timeout_ms: action.timeout_ms,
            }),
        ));
        history.push(event(
            sequence + 1,
            AdapterEvent::AssignedConsumerEventObserved(AssignedConsumerEventObservation {
                operation_id: action.operation_id.clone(),
                consumer_id: action.consumer_id.clone(),
                method: action.method,
                event: denied_event(&action.expected),
            }),
        ));
    }
    history.extend(policy_entries(7, &policy, BrokerPolicyState::Absent));
    let ScenarioAction::Receive {
        consumer_id,
        method,
        receive_id,
        timeout_ms,
        ..
    } = scenario
        .steps
        .iter()
        .find(|step| matches!(&step.action, ScenarioAction::Receive { .. }))
        .map(|step| &step.action)
        .unwrap_or_else(|| panic!("recovery receive missing"))
    else {
        panic!("recovery receive missing");
    };
    history.push(command(
        10,
        AdapterCommand::Receive {
            consumer_id: consumer_id.clone(),
            method: *method,
            receive_id: receive_id.clone(),
            timeout_ms: *timeout_ms,
        },
    ));
    history.push(event(
        11,
        AdapterEvent::ReceiveCompleted {
            receive_id: receive_id.clone(),
            records: vec![consumed(scenario)],
            fetch_evidence: None,
        },
    ));
    history
}

fn policy_entries(
    start: u64,
    policy: &BrokerPolicy,
    state: BrokerPolicyState,
) -> Vec<HistoryEntry> {
    let present = state == BrokerPolicyState::Present;
    vec![
        environment(
            start,
            EnvironmentOperationKind::BrokerPolicyAlter,
            alter_args(present),
            state,
        ),
        environment(
            start + 1,
            EnvironmentOperationKind::BrokerPolicyQuery,
            query_args(),
            state,
        ),
        environment(
            start + 2,
            EnvironmentOperationKind::BrokerPolicyObserve,
            policy.evidence_args(state),
            state,
        ),
    ]
}

fn denied_event(
    expected: &AssignedConsumerEventExpectation,
) -> AssignedConsumerEventObservationKind {
    let AssignedConsumerEventExpectation::PositionResolutionFailed {
        topic,
        partition,
        failure,
    } = expected
    else {
        panic!("position expectation missing");
    };
    AssignedConsumerEventObservationKind::PositionResolutionFailed {
        fence: AssignedConsumerPositionFenceObservation {
            topic: topic.clone(),
            partition: *partition,
            assignment_epoch: 1,
            position_epoch: 1,
        },
        failure: *failure,
    }
}

fn consumed(scenario: &Scenario) -> ConsumedRecord {
    let record = seed(scenario);
    ConsumedRecord {
        topic: record.topic.clone(),
        partition: record.partition,
        offset: 0,
        timestamp_millis: record.timestamp_millis,
        key: record.key.clone(),
        value: record.value.clone(),
        headers: record.headers.clone(),
    }
}

pub(super) fn observations(scenario: &Scenario) -> Vec<BrokerObservation> {
    let record = seed(scenario).clone();
    vec![BrokerObservation {
        observation: 0,
        offset: 0,
        operation_id: seed_operation(scenario).clone(),
        digest: record
            .digest()
            .unwrap_or_else(|error| panic!("record digest: {error}")),
        record,
    }]
}

fn seed(scenario: &Scenario) -> &testlab_schema::RecordSpec {
    scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::Send { record, .. } => Some(record),
            _ => None,
        })
        .unwrap_or_else(|| panic!("seed record missing"))
}

fn seed_operation(scenario: &Scenario) -> &testlab_schema::OperationId {
    scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::Send { operation_id, .. } => Some(operation_id),
            _ => None,
        })
        .unwrap_or_else(|| panic!("seed operation missing"))
}

fn policy(scenario: &Scenario) -> BrokerPolicy {
    scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::AlterBrokerPolicy(action)
                if action.state == BrokerPolicyState::Present =>
            {
                Some(action.policy.clone())
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("policy missing"))
}

fn environment(
    sequence: u64,
    kind: EnvironmentOperationKind,
    args: Vec<String>,
    state: BrokerPolicyState,
) -> HistoryEntry {
    let label = match (kind, state) {
        (EnvironmentOperationKind::BrokerPolicyAlter, BrokerPolicyState::Present) => "apply",
        (EnvironmentOperationKind::BrokerPolicyQuery, BrokerPolicyState::Present) => "apply-query",
        (EnvironmentOperationKind::BrokerPolicyObserve, BrokerPolicyState::Present) => {
            "apply-observe"
        }
        (EnvironmentOperationKind::BrokerPolicyAlter, BrokerPolicyState::Absent) => "remove",
        (EnvironmentOperationKind::BrokerPolicyQuery, BrokerPolicyState::Absent) => "remove-query",
        (EnvironmentOperationKind::BrokerPolicyObserve, BrokerPolicyState::Absent) => {
            "remove-observe"
        }
        _ => panic!("unsupported policy operation"),
    };
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::EnvironmentOperation {
            operation: EnvironmentOperation {
                id: EnvironmentOperationId::new(label)
                    .unwrap_or_else(|error| panic!("environment operation id: {error}")),
                kind,
                program: if kind == EnvironmentOperationKind::BrokerPolicyObserve {
                    "testlab-kafka-policy-observer/1"
                } else {
                    "docker"
                }
                .to_owned(),
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

fn alter_args(present: bool) -> Vec<String> {
    strings(&[
        "exec",
        "--no-TTY",
        "broker",
        "/opt/kafka/bin/kafka-acls.sh",
        "--bootstrap-server",
        "localhost:19092",
        if present { "--add" } else { "--remove" },
        "--deny-principal",
        "User:kafkars",
        "--operation",
        "Read",
        "--topic",
        "testlab-kafkars-assigned-event-policy",
    ])
}

fn query_args() -> Vec<String> {
    strings(&[
        "exec",
        "--no-TTY",
        "broker",
        "/opt/kafka/bin/kafka-acls.sh",
        "--bootstrap-server",
        "localhost:19092",
        "--list",
        "--topic",
        "testlab-kafkars-assigned-event-policy",
    ])
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

pub(super) fn violations(
    scenario: &Scenario,
    history: &[HistoryEntry],
    observations: &[BrokerObservation],
) -> Vec<testlab_schema::Violation> {
    let index = HistoryIndex::build(history);
    let mut violations = Vec::new();
    crate::broker_policy::verify(scenario, &index, observations, &mut violations);
    violations
}

pub(super) fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
