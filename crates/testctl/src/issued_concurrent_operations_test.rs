//! Issued-operation tests retain concurrent sends for independent broker observation.

use testlab_schema::{
    ActorId, AdapterCommand, CommandEnvelope, CommandId, ConcurrencyId, ConcurrentActorCommand,
    HistoryEntry, HistoryPayload, OperationId, ProducerId, RecordSpec,
    StartConcurrentActorsCommand,
};

use crate::issued_operations::from_history;

#[test]
fn concurrent_start_contributes_every_send_operation() {
    let command = AdapterCommand::StartConcurrentActors(StartConcurrentActorsCommand {
        concurrency_id: id(ConcurrencyId::new("group-1")),
        actors: vec![actor("actor-1", "op-1", 0), actor("actor-2", "op-2", 1)],
    });
    let history = vec![HistoryEntry {
        sequence: 0,
        observed_unix_ms: 0,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(id(CommandId::new("start")), command),
        },
    }];

    assert_eq!(
        from_history(&history).record_operations,
        ["op-1", "op-2"]
            .into_iter()
            .map(|value| id(OperationId::new(value)))
            .collect()
    );
}

fn actor(actor: &str, operation: &str, partition: i32) -> ConcurrentActorCommand {
    ConcurrentActorCommand::ProducerSend {
        actor_id: id(ActorId::new(actor)),
        producer_id: id(ProducerId::new("producer-1")),
        operation_id: id(OperationId::new(operation)),
        record: RecordSpec {
            topic: "records".to_owned(),
            partition,
            sequence: 1,
            key: None,
            value: None,
            headers: Vec::new(),
        },
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
