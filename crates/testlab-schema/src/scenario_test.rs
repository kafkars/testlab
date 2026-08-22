//! Scenario ownership and assertion validation evidence.

use std::collections::BTreeSet;

use super::{
    BatchRecord, Capability, ClientId, OperationAssertion, OperationId, ProducerId, Scenario,
    ScenarioAction, ScenarioId, ScenarioStep, StepId, TerminalStatus, VisibilityExpectation,
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
        schema_version: 4,
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
        schema_version: 4,
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
        }],
    };

    assert!(scenario.validate().is_err());
}

#[test]
fn empty_batch_is_rejected() {
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    let scenario = Scenario {
        schema_version: 4,
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
