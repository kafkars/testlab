//! Scenario ownership and assertion validation evidence.

use std::collections::BTreeSet;

use super::{
    BatchRecord, Capability, ClientId, OperationAssertion, OperationId, ProducerId,
    SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId, ScenarioStep, StepId,
    TerminalStatus, VisibilityExpectation,
};

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}

#[test]
fn open_handles_are_rejected() {
    let scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("lifecycle.open")),
        title: "open".to_owned(),
        description: "open handles".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([Capability::Lifecycle]),
        steps: vec![ScenarioStep {
            id: id(StepId::new("create")),
            action: ScenarioAction::CreateClient {
                client_id: id(ClientId::new("client-1")),
            },
        }],
        assertions: Vec::new(),
    };

    assert!(scenario.validate().is_err());
}

#[test]
fn rejected_admission_must_not_expect_a_terminal() {
    let operation = id(OperationId::new("op-1"));
    let scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("producer.bad-assertion")),
        title: "bad assertion".to_owned(),
        description: "rejected with terminal".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([Capability::Producer, Capability::Lifecycle]),
        steps: lifecycle_steps(operation.clone()),
        assertions: vec![OperationAssertion {
            operation_id: operation,
            accepted: false,
            terminal: Some(TerminalStatus::DefinitelyNotSent),
            visibility: VisibilityExpectation::Absent,
            expected_error_code: None,
        }],
    };

    assert!(scenario.validate().is_err());
}

#[test]
fn empty_batch_is_rejected() {
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    let scenario = Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("producer.empty-batch")),
        title: "empty batch".to_owned(),
        description: "batch requires records".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([
            Capability::Producer,
            Capability::ProducerBatch,
            Capability::Lifecycle,
        ]),
        steps: vec![
            step(
                "client",
                ScenarioAction::CreateClient {
                    client_id: client.clone(),
                },
            ),
            step(
                "producer",
                ScenarioAction::CreateProducer {
                    client_id: client.clone(),
                    producer_id: producer.clone(),
                },
            ),
            step(
                "batch",
                ScenarioAction::SendBatch {
                    producer_id: producer.clone(),
                    operations: Vec::<BatchRecord>::new(),
                },
            ),
            step(
                "close",
                ScenarioAction::CloseProducer {
                    producer_id: producer,
                },
            ),
            step(
                "shutdown",
                ScenarioAction::ShutdownClient { client_id: client },
            ),
        ],
        assertions: Vec::new(),
    };

    assert!(scenario.validate().is_err());
}

#[test]
fn fencing_requires_the_original_transactional_identity() {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/transaction-fencing.toml"
    ))
    .unwrap_or_else(|error| panic!("parse fencing scenario: {error}"));
    scenario.schema_version = SCENARIO_SCHEMA_VERSION;
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate fencing scenario: {error}"));
    let Some(ScenarioAction::FenceTransaction {
        transactional_id, ..
    }) = scenario.steps.get_mut(3).map(|step| &mut step.action)
    else {
        panic!("fencing action missing");
    };
    *transactional_id = "different-owner".to_owned();

    let error = match scenario.validate() {
        Ok(()) => panic!("mismatched transactional identity must fail"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("must reuse"))
    );
}

#[test]
fn broker_restart_requires_a_one_based_target_and_bounded_timeout() {
    let mut scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/producer-broker-restart.toml"
    ))
    .unwrap_or_else(|error| panic!("parse broker restart scenario: {error}"));
    scenario.schema_version = SCENARIO_SCHEMA_VERSION;
    let Some(ScenarioAction::RestartBroker {
        broker_ordinal,
        timeout_ms,
    }) = scenario.steps.get_mut(4).map(|step| &mut step.action)
    else {
        panic!("broker restart action missing");
    };
    *broker_ordinal = 0;
    *timeout_ms = 99;

    let error = match scenario.validate() {
        Ok(()) => panic!("invalid broker restart must fail"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("ordinal must be one-based"))
    );
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("restart timeout_ms"))
    );
}

#[test]
fn broker_and_role_stops_require_exact_restoration() {
    let source = r#"
schema_version = 37
id = "environment.paired-control"
title = "paired control"
description = "every retained broker control is restored"
timeout_ms = 1000
requires = []
assertions = []

[[steps]]
id = "stop-broker"
kind = "stop_broker"
broker_ordinal = 1
timeout_ms = 500

[[steps]]
id = "start-broker"
kind = "start_broker"
broker_ordinal = 1
timeout_ms = 500

[[steps]]
id = "stop-controller"
kind = "stop_broker_role"
timeout_ms = 500

[steps.target]
role = "controller"

[[steps]]
id = "restore-controller"
kind = "restore_broker_role"
timeout_ms = 500

[steps.target]
role = "controller"
"#;
    let mut scenario: Scenario =
        toml::from_str(source).unwrap_or_else(|error| panic!("parse controls: {error}"));
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("paired controls: {error}"));
    scenario.steps.pop();

    let error = match scenario.validate() {
        Ok(()) => panic!("unrestored broker role must fail"),
        Err(error) => error,
    };

    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("was not restored"))
    );
}

fn lifecycle_steps(operation_id: OperationId) -> Vec<ScenarioStep> {
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    vec![
        step(
            "client",
            ScenarioAction::CreateClient {
                client_id: client.clone(),
            },
        ),
        step(
            "producer",
            ScenarioAction::CreateProducer {
                client_id: client.clone(),
                producer_id: producer.clone(),
            },
        ),
        step(
            "send",
            ScenarioAction::Send {
                producer_id: producer.clone(),
                operation_id,
                record: super::RecordSpec {
                    topic: "records".to_owned(),
                    partition: 0,
                    sequence: 1,
                    key: None,
                    value: None,
                    headers: Vec::new(),
                },
            },
        ),
        step(
            "close",
            ScenarioAction::CloseProducer {
                producer_id: producer,
            },
        ),
        step(
            "shutdown",
            ScenarioAction::ShutdownClient { client_id: client },
        ),
    ]
}

fn step(id_value: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: id(StepId::new(id_value)),
        action,
    }
}
