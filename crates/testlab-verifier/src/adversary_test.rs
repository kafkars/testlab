//! Adversary verifier tests join declared controls, wire facts, process truth, and public results.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdversaryOutcome, ClientId,
    CommandEnvelope, CommandId, DescribeTopicAction, DescribeTopicCommand, EnvironmentOperation,
    EnvironmentOperationId, EnvironmentOperationKind, EnvironmentOperationStatus, HistoryEntry,
    HistoryPayload, KafkaApi, OperationId, ProtocolAdversaryObservation, ProtocolFault,
    ProtocolFaultAction, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId,
    ScenarioStep, StepId,
};

use crate::adversary::{verify, verify_admin_failure};
use crate::index::HistoryIndex;

#[test]
fn exact_control_observation_and_successful_process_pass_all_adversary_contracts() {
    let scenario = controlled_scenario();
    let control = control();
    let history = vec![
        entry(0, HistoryPayload::AdversaryControl { control }),
        entry(
            1,
            HistoryPayload::AdversaryObservation {
                observation: observation(0, 7),
            },
        ),
        entry(
            2,
            HistoryPayload::EnvironmentOperation {
                operation: process(EnvironmentOperationStatus::Succeeded),
            },
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&scenario, &index, &mut violations);

    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn missing_application_and_failed_process_are_distinct_contract_failures() {
    let history = vec![
        entry(0, HistoryPayload::AdversaryControl { control: control() }),
        entry(
            1,
            HistoryPayload::EnvironmentOperation {
                operation: process(EnvironmentOperationStatus::Failed),
            },
        ),
    ];
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();

    verify(&controlled_scenario(), &index, &mut violations);

    assert!(has(&violations, "ADV-002"), "{violations:?}");
    assert!(has(&violations, "ADV-003"), "{violations:?}");
}

#[test]
fn metadata_fault_requires_the_exact_declared_public_error() {
    let scenario = metadata_scenario("transport");
    let describe = &scenario.steps[1].action;
    let history = metadata_history("transport");
    let index = HistoryIndex::build(&history);
    let mut violations = Vec::new();
    assert!(verify_admin_failure(
        &scenario,
        describe,
        &index,
        &mut violations
    ));
    assert!(violations.is_empty(), "{violations:?}");

    let wrong = HistoryIndex::build(&metadata_history("timeout"));
    verify_admin_failure(&scenario, describe, &wrong, &mut violations);
    assert!(has(&violations, "ADV-004"), "{violations:?}");
}

fn controlled_scenario() -> Scenario {
    scenario(vec![step(
        "arm",
        ScenarioAction::ArmProtocolFault(control()),
    )])
}

fn metadata_scenario(expected: &str) -> Scenario {
    scenario(vec![
        step("arm", ScenarioAction::ArmProtocolFault(control())),
        step(
            "describe",
            ScenarioAction::DescribeTopic(DescribeTopicAction {
                client_id: client(),
                operation_id: operation(),
                topic: "orders".to_owned(),
                expected_partitions: None,
                expected_error_code: Some(expected.to_owned()),
                timeout_ms: 1_000,
            }),
        ),
    ])
}

fn scenario(steps: Vec<ScenarioStep>) -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: ScenarioId::new("adversary.verifier")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "adversary verifier".to_owned(),
        description: "adversary verifier fixture".to_owned(),
        timeout_ms: 5_000,
        requires: BTreeSet::new(),
        steps,
        assertions: Vec::new(),
    }
}

fn metadata_history(code: &str) -> Vec<HistoryEntry> {
    let command_id =
        CommandId::new("describe-command").unwrap_or_else(|error| panic!("command id: {error}"));
    vec![
        entry(
            0,
            HistoryPayload::HarnessCommand {
                command: CommandEnvelope::new(
                    command_id.clone(),
                    AdapterCommand::DescribeTopic(DescribeTopicCommand {
                        client_id: client(),
                        operation_id: operation(),
                        topic: "orders".to_owned(),
                        timeout_ms: 1_000,
                    }),
                ),
            },
        ),
        entry(
            1,
            HistoryPayload::AdapterEvent {
                event: AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::CommandFailed {
                        code: code.to_owned(),
                        diagnostic: "public failure".to_owned(),
                    },
                ),
            },
        ),
    ]
}

fn control() -> ProtocolFaultAction {
    ProtocolFaultAction {
        operation_id: environment_operation(),
        api: KafkaApi::Metadata,
        applications: 1,
        fault: ProtocolFault::PartialFrame { bytes: 7 },
    }
}

fn observation(observation: u64, response_bytes: u32) -> ProtocolAdversaryObservation {
    ProtocolAdversaryObservation {
        observation,
        connection: 0,
        request: 1,
        api: KafkaApi::Metadata,
        api_version: 8,
        correlation_id: 3,
        request_bytes: 21,
        response_bytes,
        control_id: Some(environment_operation()),
        outcome: AdversaryOutcome::FaultApplied {
            fault: ProtocolFault::PartialFrame { bytes: 7 },
        },
    }
}

fn process(status: EnvironmentOperationStatus) -> EnvironmentOperation {
    EnvironmentOperation {
        id: EnvironmentOperationId::new("worker")
            .unwrap_or_else(|error| panic!("process id: {error}")),
        kind: EnvironmentOperationKind::ProtocolAdversary,
        program: "testctl".to_owned(),
        args: vec!["adversary-worker".to_owned()],
        started_unix_ms: 1,
        completed_unix_ms: 2,
        status,
        exit_code: Some(if status == EnvironmentOperationStatus::Succeeded {
            0
        } else {
            2
        }),
        stdout_artifact: Some("protocol-adversary.jsonl".to_owned()),
        stderr_artifact: Some("protocol-adversary.stderr.txt".to_owned()),
        diagnostic: None,
    }
}

fn entry(sequence: u64, payload: HistoryPayload) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload,
    }
}

fn step(id: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: StepId::new(id).unwrap_or_else(|error| panic!("step id: {error}")),
        action,
    }
}

fn environment_operation() -> EnvironmentOperationId {
    EnvironmentOperationId::new("metadata-fault")
        .unwrap_or_else(|error| panic!("environment operation id: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-orders").unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn has(violations: &[testlab_schema::Violation], contract: &str) -> bool {
    violations
        .iter()
        .any(|violation| violation.contract_id.as_str() == contract)
}
