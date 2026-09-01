//! Issued-operation tests retain record and independently observed admin identities.

use testlab_schema::{
    AdapterCommand, BatchRecord, ClientId, CommandEnvelope, CommandId, ConsumerId,
    CreatePartitionsCommand, HistoryEntry, HistoryPayload, ListConsumerGroupOffsetsCommand,
    OperationId, ProducerId, RecordSpec, TransactionDisposition, TransactionalTransformCommand,
};

use crate::issued_operations::from_history;

#[test]
fn recorded_commands_retain_every_observed_operation() {
    let history = vec![
        entry(0, "admin", create_partitions()),
        entry(
            1,
            "send",
            AdapterCommand::Send {
                producer_id: id(ProducerId::new("producer-1")),
                operation_id: id(OperationId::new("send-1")),
                record: record_spec(2),
            },
        ),
        entry(
            2,
            "batch",
            AdapterCommand::SendBatch {
                producer_id: id(ProducerId::new("producer-1")),
                operations: vec![record("batch-1", 0), record("batch-2", 1)],
            },
        ),
        entry(
            3,
            "transaction",
            AdapterCommand::ExecuteTransaction {
                producer_id: id(ProducerId::new("transactional-1")),
                transaction_id: id(OperationId::new("transaction-1")),
                operations: vec![record("transaction-record-1", 0)],
                disposition: TransactionDisposition::Commit,
                timeout_ms: 1_000,
            },
        ),
        entry(4, "fence", fence_transaction()),
        entry(5, "transform", transactional_transform()),
        entry(6, "group-offsets", group_offset_command("group-1")),
        entry(
            7,
            "group-offsets-duplicate",
            group_offset_command("other-group"),
        ),
    ];

    let issued = from_history(&history);

    assert_eq!(
        issued.record_operations,
        [
            "batch-1",
            "batch-2",
            "fenced-record-1",
            "send-1",
            "transaction-record-1",
            "transform-record-1",
        ]
        .into_iter()
        .map(|value| id(OperationId::new(value)))
        .collect()
    );
    assert_eq!(
        issued.group_offset_commands,
        [
            group_offset_payload("group-1"),
            group_offset_payload("other-group"),
        ]
    );
}

fn transactional_transform() -> AdapterCommand {
    AdapterCommand::ExecuteTransactionalTransform(TransactionalTransformCommand {
        producer_id: id(ProducerId::new("transactional-1")),
        consumer_id: id(ConsumerId::new("consumer-1")),
        transaction_id: id(OperationId::new("transform-1")),
        operations: vec![record("transform-record-1", 0)],
        disposition: TransactionDisposition::Commit,
        timeout_ms: 1_000,
    })
}

fn create_partitions() -> AdapterCommand {
    AdapterCommand::CreatePartitions(CreatePartitionsCommand {
        client_id: id(ClientId::new("client-1")),
        operation_id: id(OperationId::new("admin-partitions-1")),
        topic: "records".to_owned(),
        total_count: 3,
        validate_only: false,
        timeout_ms: 1_000,
    })
}

fn fence_transaction() -> AdapterCommand {
    AdapterCommand::FenceTransaction {
        producer_id: id(ProducerId::new("transactional-2")),
        transaction_id: id(OperationId::new("transaction-2")),
        operation: record("fenced-record-1", 0),
        replacement_client_id: id(ClientId::new("client-1")),
        replacement_producer_id: id(ProducerId::new("replacement-1")),
        transactional_id: "shared-owner".to_owned(),
        transaction_timeout_ms: 1_000,
        initialization_timeout_ms: 1_000,
        timeout_ms: 1_000,
    }
}

fn group_offset_command(group_id: &str) -> AdapterCommand {
    AdapterCommand::ListConsumerGroupOffsets(group_offset_payload(group_id))
}

fn group_offset_payload(group_id: &str) -> ListConsumerGroupOffsetsCommand {
    ListConsumerGroupOffsetsCommand {
        client_id: id(ClientId::new("client-1")),
        operation_id: id(OperationId::new("admin-group-offsets-1")),
        group_id: group_id.to_owned(),
        topic: "records".to_owned(),
        partition: 0,
        require_stable: true,
        timeout_ms: 1_000,
    }
}

fn entry(sequence: u64, command_id: &str, command: AdapterCommand) -> HistoryEntry {
    HistoryEntry {
        sequence,
        observed_unix_ms: 0,
        payload: HistoryPayload::HarnessCommand {
            command: CommandEnvelope::new(id(CommandId::new(command_id)), command),
        },
    }
}

fn record(operation_id: &str, partition: i32) -> BatchRecord {
    BatchRecord {
        operation_id: id(OperationId::new(operation_id)),
        record: record_spec(partition),
    }
}

fn record_spec(partition: i32) -> RecordSpec {
    RecordSpec {
        topic: "records".to_owned(),
        partition,
        sequence: 1,
        key: None,
        value: None,
        headers: Vec::new(),
    }
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture identity: {error}"))
}
