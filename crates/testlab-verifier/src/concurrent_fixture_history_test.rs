//! Concurrent verifier history fixtures retain every command and correlated event.

use testlab_schema::{
    ActorId, AdapterCommand, AdapterDescriptor, AdapterEvent, AdapterEventEnvelope,
    AdapterSecurity, ClientId, CommandEnvelope, CommandId, ConcurrencyId, ConcurrentActorCommand,
    ConsumedRecord, ConsumerId, HistoryEntry, HistoryPayload, OperationId, ProducerId, RecordSpec,
    RunId, ScenarioId, StartConcurrentActorsCommand, TerminalStatus,
};

pub(crate) fn history(adapter: &AdapterDescriptor, record: &RecordSpec) -> Vec<HistoryEntry> {
    let client = id(ClientId::new("client-1"));
    let producer = id(ProducerId::new("producer-1"));
    let consumer = id(ConsumerId::new("consumer-1"));
    let concurrency = id(ConcurrencyId::new("produce-consume"));
    let receive_actor = id(ActorId::new("receive-actor"));
    let send_actor = id(ActorId::new("send-actor"));
    let receive = id(OperationId::new("receive-concurrent"));
    let operation = id(OperationId::new("op-concurrent"));
    let mut history = HistoryBuilder::default();
    history.command(
        "hello",
        AdapterCommand::Hello {
            run_id: id(RunId::new("run-concurrent")),
            scenario_id: id(ScenarioId::new("kafka.concurrent-producer-consumer")),
            broker_endpoints: vec!["127.0.0.1:9092".to_owned()],
            security: AdapterSecurity::Plaintext,
        },
    );
    history.event(
        "hello",
        AdapterEvent::Ready {
            descriptor: adapter.clone(),
        },
    );
    create_handles(&mut history, &client, &producer, &consumer, record);
    history.command(
        "start",
        AdapterCommand::StartConcurrentActors(StartConcurrentActorsCommand {
            concurrency_id: concurrency.clone(),
            actors: vec![
                ConcurrentActorCommand::AssignedReceive {
                    actor_id: receive_actor.clone(),
                    consumer_id: consumer.clone(),
                    receive_id: receive.clone(),
                    timeout_ms: 30_000,
                },
                ConcurrentActorCommand::ProducerSend {
                    actor_id: send_actor.clone(),
                    producer_id: producer.clone(),
                    operation_id: operation.clone(),
                    record: record.clone(),
                },
            ],
        }),
    );
    history.event(
        "start",
        AdapterEvent::ConcurrentActorsStarted {
            concurrency_id: concurrency.clone(),
            actor_ids: vec![receive_actor.clone(), send_actor.clone()],
        },
    );
    history.command(
        "join",
        AdapterCommand::JoinConcurrentActors {
            concurrency_id: concurrency.clone(),
            timeout_ms: 40_000,
        },
    );
    joined_events(
        &mut history,
        concurrency,
        receive_actor,
        send_actor,
        receive,
        operation,
        record,
    );
    crate::concurrent_fixture_lifecycle_test::settle(&mut history, client, producer, consumer);
    history.entries
}

fn create_handles(
    history: &mut HistoryBuilder,
    client: &ClientId,
    producer: &ProducerId,
    consumer: &ConsumerId,
    record: &RecordSpec,
) {
    history.command(
        "create-client",
        AdapterCommand::CreateClient {
            client_id: client.clone(),
        },
    );
    history.event(
        "create-client",
        AdapterEvent::ClientCreated {
            client_id: client.clone(),
        },
    );
    history.command(
        "ready-client",
        AdapterCommand::AwaitClientReady {
            client_id: client.clone(),
        },
    );
    history.event(
        "ready-client",
        AdapterEvent::ClientReady {
            client_id: client.clone(),
        },
    );
    history.command(
        "create-producer",
        AdapterCommand::CreateProducer {
            client_id: client.clone(),
            producer_id: producer.clone(),
        },
    );
    history.event(
        "create-producer",
        AdapterEvent::ProducerCreated {
            producer_id: producer.clone(),
        },
    );
    history.command(
        "create-consumer",
        AdapterCommand::CreateAssignedConsumer {
            client_id: client.clone(),
            consumer_id: consumer.clone(),
        },
    );
    history.event(
        "create-consumer",
        AdapterEvent::AssignedConsumerCreated {
            consumer_id: consumer.clone(),
        },
    );
    history.command(
        "assign-consumer",
        AdapterCommand::AssignBeginning {
            consumer_id: consumer.clone(),
            topic: record.topic.clone(),
            partition: record.partition,
        },
    );
    history.event(
        "assign-consumer",
        AdapterEvent::AssignmentCompleted {
            consumer_id: consumer.clone(),
        },
    );
}

#[allow(
    clippy::too_many_arguments,
    reason = "the fixture preserves every independent actor identity explicitly"
)]
fn joined_events(
    history: &mut HistoryBuilder,
    concurrency: ConcurrencyId,
    receive_actor: ActorId,
    send_actor: ActorId,
    receive: OperationId,
    operation: OperationId,
    record: &RecordSpec,
) {
    history.event(
        "join",
        AdapterEvent::ReceiveCompleted {
            receive_id: receive.clone(),
            records: vec![consumed(record)],
        },
    );
    history.event(
        "join",
        AdapterEvent::ConcurrentActorCompleted {
            concurrency_id: concurrency.clone(),
            actor_id: receive_actor.clone(),
            operation_id: receive,
        },
    );
    history.event(
        "join",
        AdapterEvent::OperationAccepted {
            operation_id: operation.clone(),
        },
    );
    history.event(
        "join",
        AdapterEvent::OperationTerminal {
            operation_id: operation.clone(),
            status: TerminalStatus::Acknowledged,
            code: None,
            offset: Some(0),
        },
    );
    history.event(
        "join",
        AdapterEvent::ConcurrentActorCompleted {
            concurrency_id: concurrency.clone(),
            actor_id: send_actor.clone(),
            operation_id: operation,
        },
    );
    history.event(
        "join",
        AdapterEvent::ConcurrentActorsCompleted {
            concurrency_id: concurrency,
            actor_ids: vec![receive_actor, send_actor],
        },
    );
}

fn consumed(record: &RecordSpec) -> ConsumedRecord {
    ConsumedRecord {
        topic: record.topic.clone(),
        partition: record.partition,
        offset: 0,
        timestamp_millis: None,
        key: record.key.clone(),
        value: record.value.clone(),
        headers: record.headers.clone(),
    }
}

#[derive(Default)]
pub(crate) struct HistoryBuilder {
    pub(crate) entries: Vec<HistoryEntry>,
}

impl HistoryBuilder {
    pub(crate) fn command(&mut self, id_value: &str, command: AdapterCommand) {
        self.push(HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(id(CommandId::new(id_value)), command),
        });
    }

    pub(crate) fn event(&mut self, id_value: &str, event: AdapterEvent) {
        self.push(HistoryPayload::AdapterEvent {
            event: AdapterEventEnvelope::new(id(CommandId::new(id_value)), event),
        });
    }

    fn push(&mut self, payload: HistoryPayload) {
        let sequence = u64::try_from(self.entries.len())
            .unwrap_or_else(|error| panic!("history sequence: {error}"));
        self.entries.push(HistoryEntry {
            sequence,
            observed_unix_ms: sequence,
            payload,
        });
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
