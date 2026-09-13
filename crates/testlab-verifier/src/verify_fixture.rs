//! Shared verifier fixtures keep semantic tests focused on contract outcomes.

use std::collections::BTreeSet;

use testlab_schema::{
    AdapterCommand, AdapterDescriptor, AdapterEvent, AdapterEventEnvelope, AdapterId,
    BrokerObservation, ByteString, Capability, ClientId, CommandEnvelope, CommandId, HistoryEntry,
    HistoryPayload, OperationAssertion, OperationId, ProducerHandleConfigurationObservation,
    ProducerId, RecordSpec, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction, ScenarioId,
    ScenarioStep, StepId, TerminalStatus, VisibilityExpectation,
};

pub(crate) fn scenario(terminal: TerminalStatus, visibility: VisibilityExpectation) -> Scenario {
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    let operation = id(OperationId::new("op-1"));
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("producer.verifier")),
        title: "verifier".to_owned(),
        description: "verifier fixture".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([
            Capability::Producer,
            Capability::Lifecycle,
            Capability::ClientReadiness,
            Capability::AssignedConsumer,
            Capability::ModelBroker,
        ]),
        steps: vec![
            step(
                "client",
                ScenarioAction::CreateClient(testlab_schema::CreateClientAction {
                    client_id: client.clone(),
                    expected_cluster_id: None,
                    expected_error_code: None,
                }),
            ),
            step(
                "ready",
                ScenarioAction::AwaitClientReady {
                    client_id: client.clone(),
                },
            ),
            step(
                "producer",
                ScenarioAction::CreateProducer {
                    client_id: client.clone(),
                    producer_id: producer.clone(),
                    ownership: testlab_schema::ChildHandleOwnership::default(),
                    delivery_timeout_ms: None,
                },
            ),
            step(
                "send",
                ScenarioAction::Send {
                    producer_id: producer.clone(),
                    operation_id: operation.clone(),
                    method: testlab_schema::ProducerSendMethod::default(),
                    partitioning: testlab_schema::ProducerPartitioning::Explicit,
                    topic_identity_operation_id: None,
                    record: record("value"),
                },
            ),
            step(
                "flush",
                ScenarioAction::Flush {
                    producer_id: producer.clone(),
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
        assertions: vec![OperationAssertion {
            operation_id: operation,
            accepted: true,
            terminal: Some(terminal),
            visibility,
            expected_error_code: None,
        }],
    }
}

pub(crate) fn record(value: &str) -> RecordSpec {
    RecordSpec {
        topic: "records".to_owned(),
        partition: 0,
        sequence: 1,
        timestamp_millis: None,
        key: None,
        value: Some(ByteString::utf8(value)),
        headers: Vec::new(),
    }
}

pub(crate) fn adapter() -> AdapterDescriptor {
    AdapterDescriptor {
        id: id(AdapterId::new("reference-rust")),
        implementation: "fixture".to_owned(),
        version: "0.1.0".to_owned(),
        protocol_version: testlab_schema::PROTOCOL_VERSION,
        capabilities: BTreeSet::from([
            Capability::Producer,
            Capability::Lifecycle,
            Capability::ClientReadiness,
            Capability::ModelBroker,
        ]),
    }
}

pub(crate) fn history(status: TerminalStatus) -> Vec<HistoryEntry> {
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    let operation = id(OperationId::new("op-1"));
    vec![
        event(
            0,
            AdapterEvent::Ready {
                descriptor: adapter(),
            },
        ),
        event(
            1,
            AdapterEvent::ClientCreated(testlab_schema::ClientConfigurationObservation {
                client_id: client.clone(),
                observed_client_id: Some(client.as_str().to_owned()),
                observed_bootstrap_servers: vec!["127.0.0.1:9092".to_owned()],
                observed_expected_cluster_id: None,
                selected_producer_configuration: None,
            }),
        ),
        event(
            2,
            AdapterEvent::ClientReady {
                client_id: client.clone(),
            },
        ),
        event(
            3,
            AdapterEvent::ProducerCreated(ProducerHandleConfigurationObservation {
                producer_id: producer.clone(),
                selected_delivery_timeout_ms: 30_000,
            }),
        ),
        event(
            4,
            AdapterEvent::OperationAccepted {
                operation_id: operation.clone(),
            },
        ),
        event(
            5,
            AdapterEvent::OperationTerminal {
                operation_id: operation,
                status,
                code: None,
                partition: (status == TerminalStatus::Acknowledged).then_some(0),
                offset: (status == TerminalStatus::Acknowledged).then_some(0),
                timestamp_millis: None,
                receipt: None,
            },
        ),
        event(
            6,
            AdapterEvent::FlushCompleted {
                producer_id: producer.clone(),
            },
        ),
        event(
            7,
            AdapterEvent::ProducerClosed {
                producer_id: producer,
            },
        ),
        event(8, AdapterEvent::ClientShutdown { client_id: client }),
        event(9, AdapterEvent::Finished),
    ]
}

pub(crate) fn rejected_history() -> Vec<HistoryEntry> {
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    vec![
        event(
            0,
            AdapterEvent::Ready {
                descriptor: adapter(),
            },
        ),
        event(
            1,
            AdapterEvent::ClientCreated(testlab_schema::ClientConfigurationObservation {
                client_id: client.clone(),
                observed_client_id: Some(client.as_str().to_owned()),
                observed_bootstrap_servers: vec!["127.0.0.1:9092".to_owned()],
                observed_expected_cluster_id: None,
                selected_producer_configuration: None,
            }),
        ),
        event(
            2,
            AdapterEvent::ClientReady {
                client_id: client.clone(),
            },
        ),
        event(
            3,
            AdapterEvent::ProducerCreated(ProducerHandleConfigurationObservation {
                producer_id: producer.clone(),
                selected_delivery_timeout_ms: 30_000,
            }),
        ),
        event(
            4,
            AdapterEvent::OperationRejected {
                operation_id: id(OperationId::new("op-1")),
                code: "queue_full".to_owned(),
            },
        ),
        event(
            5,
            AdapterEvent::FlushCompleted {
                producer_id: producer.clone(),
            },
        ),
        event(
            6,
            AdapterEvent::ProducerClosed {
                producer_id: producer,
            },
        ),
        event(7, AdapterEvent::ClientShutdown { client_id: client }),
        event(8, AdapterEvent::Finished),
    ]
}

pub(crate) fn observation(index: u64, value: &str) -> BrokerObservation {
    let record = record(value);
    let offset = match i64::try_from(index) {
        Ok(offset) => offset,
        Err(error) => panic!("fixture offset: {error}"),
    };
    let digest = match record.digest() {
        Ok(digest) => digest,
        Err(error) => panic!("fixture digest: {error}"),
    };
    BrokerObservation {
        observation: index,
        offset,
        operation_id: id(OperationId::new("op-1")),
        digest,
        record,
    }
}

pub(crate) fn step(id_value: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: id(StepId::new(id_value)),
        action,
    }
}

pub(crate) fn event(sequence: u64, event: AdapterEvent) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(id(CommandId::new("cmd-fixture")), event),
        },
    }
}

pub(crate) fn command(sequence: u64, command: AdapterCommand) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: sequence,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(id(CommandId::new("cmd-fixture")), command),
        },
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    match result {
        Ok(value) => value,
        Err(error) => panic!("fixture id: {error}"),
    }
}
