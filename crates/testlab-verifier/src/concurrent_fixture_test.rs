//! Concurrent verifier fixtures build one complete correlated producer and consumer history.

use std::collections::BTreeSet;

use testlab_schema::{
    ActorId, AdapterDescriptor, AdapterId, BrokerObservation, ByteString, Capability, ClientId,
    ConcurrencyId, ConcurrentActor, ConsumerId, HeaderSpec, HistoryEntry,
    JoinConcurrentActorsAction, OperationAssertion, OperationId, ProducerId, RecordSpec, Scenario,
    ScenarioAction, ScenarioId, ScenarioStep, StartConcurrentActorsAction, StepId, TerminalStatus,
    VisibilityExpectation,
};

pub(crate) struct ConcurrentFixture {
    pub(crate) scenario: Scenario,
    pub(crate) adapter: AdapterDescriptor,
    pub(crate) history: Vec<HistoryEntry>,
    pub(crate) observations: Vec<BrokerObservation>,
}

pub(crate) fn fixture() -> ConcurrentFixture {
    let record = record();
    let scenario = scenario(&record);
    let adapter = adapter();
    let operation = id(OperationId::new("op-concurrent"));
    let history = crate::concurrent_fixture_history_test::history(&adapter, &record);
    let digest = record
        .digest()
        .unwrap_or_else(|error| panic!("record digest: {error}"));
    ConcurrentFixture {
        scenario,
        adapter,
        history,
        observations: vec![BrokerObservation {
            observation: 0,
            offset: 0,
            operation_id: operation,
            record,
            digest,
        }],
    }
}

fn scenario(record: &RecordSpec) -> Scenario {
    let scenario = Scenario {
        schema_version: testlab_schema::SCENARIO_SCHEMA_VERSION,
        id: id(ScenarioId::new("kafka.concurrent-producer-consumer")),
        title: "concurrent fixture".to_owned(),
        description: "exact concurrent producer and consumer fixture".to_owned(),
        timeout_ms: 120_000,
        requires: BTreeSet::from([
            Capability::Producer,
            Capability::AssignedConsumer,
            Capability::ConcurrentActors,
            Capability::Lifecycle,
            Capability::ClientReadiness,
        ]),
        steps: scenario_steps(record),
        assertions: vec![OperationAssertion {
            operation_id: id(OperationId::new("op-concurrent")),
            accepted: true,
            terminal: Some(TerminalStatus::Acknowledged),
            visibility: VisibilityExpectation::ExactlyOnce,
            expected_error_code: None,
        }],
    };
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate concurrent fixture: {error}"));
    scenario
}

fn scenario_steps(record: &RecordSpec) -> Vec<ScenarioStep> {
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    let consumer = id(ConsumerId::new("consumer-1"));
    vec![
        step(
            "create-client",
            ScenarioAction::CreateClient {
                client_id: client.clone(),
            },
        ),
        step(
            "ready-client",
            ScenarioAction::AwaitClientReady {
                client_id: client.clone(),
            },
        ),
        step(
            "create-producer",
            ScenarioAction::CreateProducer {
                client_id: client.clone(),
                producer_id: producer.clone(),
            },
        ),
        step(
            "create-consumer",
            ScenarioAction::CreateAssignedConsumer {
                client_id: client.clone(),
                consumer_id: consumer.clone(),
            },
        ),
        step(
            "assign-consumer",
            ScenarioAction::AssignBeginning {
                consumer_id: consumer.clone(),
                topic: record.topic.clone(),
                partition: record.partition,
            },
        ),
        concurrent_start(record, &producer, &consumer),
        step(
            "join-produce-consume",
            ScenarioAction::JoinConcurrentActors(JoinConcurrentActorsAction {
                concurrency_id: id(ConcurrencyId::new("produce-consume")),
                timeout_ms: 40_000,
            }),
        ),
        step(
            "close-consumer",
            ScenarioAction::CloseAssignedConsumer {
                consumer_id: consumer,
            },
        ),
        step(
            "flush-producer",
            ScenarioAction::Flush {
                producer_id: producer.clone(),
            },
        ),
        step(
            "close-producer",
            ScenarioAction::CloseProducer {
                producer_id: producer,
            },
        ),
        step(
            "shutdown-client",
            ScenarioAction::ShutdownClient { client_id: client },
        ),
    ]
}

fn concurrent_start(
    record: &RecordSpec,
    producer: &ProducerId,
    consumer: &ConsumerId,
) -> ScenarioStep {
    step(
        "start-produce-consume",
        ScenarioAction::StartConcurrentActors(StartConcurrentActorsAction {
            concurrency_id: id(ConcurrencyId::new("produce-consume")),
            actors: vec![
                ConcurrentActor::AssignedReceive {
                    actor_id: id(ActorId::new("receive-actor")),
                    consumer_id: consumer.clone(),
                    receive_id: id(OperationId::new("receive-concurrent")),
                    expected_operation_id: id(OperationId::new("op-concurrent")),
                    timeout_ms: 30_000,
                },
                ConcurrentActor::ProducerSend {
                    actor_id: id(ActorId::new("send-actor")),
                    producer_id: producer.clone(),
                    operation_id: id(OperationId::new("op-concurrent")),
                    record: record.clone(),
                },
            ],
        }),
    )
}

fn record() -> RecordSpec {
    RecordSpec {
        topic: "testlab-kafkars-concurrent-producer-consumer".to_owned(),
        partition: 0,
        sequence: 1,
        key: Some(ByteString::utf8("same-client")),
        value: Some(ByteString::hex(b"\0concurrent\xff")),
        headers: vec![
            HeaderSpec {
                name: "testlab-operation-id".to_owned(),
                value: Some(ByteString::utf8("op-concurrent")),
            },
            HeaderSpec {
                name: "testlab-sequence".to_owned(),
                value: Some(ByteString::utf8("1")),
            },
        ],
    }
}

fn step(id_value: &str, action: ScenarioAction) -> ScenarioStep {
    ScenarioStep {
        id: id(StepId::new(id_value)),
        action,
    }
}

fn adapter() -> AdapterDescriptor {
    AdapterDescriptor {
        id: id(AdapterId::new("kafkars-rust")),
        implementation: "fixture".to_owned(),
        version: "0.0.1".to_owned(),
        protocol_version: testlab_schema::PROTOCOL_VERSION,
        capabilities: BTreeSet::from([
            Capability::Producer,
            Capability::AssignedConsumer,
            Capability::ConcurrentActors,
            Capability::Lifecycle,
            Capability::ClientReadiness,
        ]),
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
