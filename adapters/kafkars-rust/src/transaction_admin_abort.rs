//! Admin partition abort proves broker state changes before transaction cleanup.

use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminProducersDescription, BatchRecord,
    CommandId, OperationId, ProducerStateSnapshot, TransactionDisposition,
};

use crate::AdapterError;
use crate::kafkars_api::{
    AbortTransactionSpec, Client, RetryAdvice, TopicPartition, Transaction, TransactionalProducer,
};
use crate::protocol::emit;
use crate::state::AdapterState;
use crate::{protocol_admin_producers, transaction_execute};

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    let AdapterCommand::ExecuteTransaction {
        producer_id,
        transaction_id,
        operations,
        disposition: TransactionDisposition::AdminPartitionAbort,
        timeout_ms,
    } = command
    else {
        return Err(invalid("non-admin-abort command reached dispatcher"));
    };
    let operation = singleton(operations)?;
    let mut owner = state.take_transactional_producer(&producer_id)?;
    let result = state
        .client(&owner.client_id)
        .map(Clone::clone)
        .map_err(AdapterError::from)
        .and_then(|client| {
            execute(
                &mut owner.producer,
                &client,
                writer,
                command_id,
                transaction_id,
                operation,
                Duration::from_millis(timeout_ms),
            )
        });
    let restored = state.restore_transactional_producer(producer_id, owner);
    match (result, restored) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn execute<W: Write>(
    producer: &mut TransactionalProducer,
    client: &Client,
    writer: &mut W,
    command_id: CommandId,
    transaction_id: OperationId,
    operation: BatchRecord,
    timeout: Duration,
) -> Result<(), AdapterError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| invalid("command deadline overflow"))?;
    loop {
        match producer.begin() {
            Ok(transaction) => {
                return execute_started(
                    transaction,
                    client,
                    writer,
                    command_id,
                    transaction_id,
                    operation,
                    deadline,
                );
            }
            Err(error)
                if error.retry_advice() == RetryAdvice::RetrySafe && Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(AdapterError::Client(error)),
        }
    }
}

fn execute_started<W: Write>(
    mut transaction: Transaction<'_>,
    client: &Client,
    writer: &mut W,
    command_id: CommandId,
    transaction_id: OperationId,
    operation: BatchRecord,
    deadline: Instant,
) -> Result<(), AdapterError> {
    let operation_id = operation.operation_id.clone();
    let topic = operation.record.topic.clone();
    let partition = operation.record.partition;
    transaction_execute::send(&mut transaction, writer, &command_id, operation, deadline)?;
    let before = protocol_admin_producers::query(client, &topic, partition, deadline)?;
    let spec = abort_spec(&before, &topic, partition)?;
    let expected = before
        .first()
        .ok_or_else(|| invalid("expected one active partition producer"))?
        .clone();
    emit_state(
        writer,
        &command_id,
        operation_id,
        &topic,
        partition,
        before.clone(),
    )?;
    client
        .admin()
        .abort_transaction(spec)
        .deadline_after(transaction_execute::remaining(deadline)?)
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let after = await_cleared(client, &topic, partition, &expected, deadline)?;
    emit_state(
        writer,
        &command_id,
        transaction_id.clone(),
        &topic,
        partition,
        after,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TransactionCompleted {
                transaction_id,
                disposition: TransactionDisposition::AdminPartitionAbort,
            },
        ),
    )
}

fn abort_spec(
    producers: &[ProducerStateSnapshot],
    topic: &str,
    partition: i32,
) -> Result<AbortTransactionSpec, AdapterError> {
    let [producer] = producers else {
        return Err(invalid("expected one active partition producer"));
    };
    if producer.current_transaction_start_offset.is_none() {
        return Err(invalid("staged producer had no open transaction"));
    }
    let producer_epoch = i16::try_from(producer.producer_epoch)
        .map_err(|_| invalid("producer epoch exceeded Kafka's i16 range"))?;
    Ok(AbortTransactionSpec::new(
        TopicPartition::new(topic.to_owned(), partition),
        producer.producer_id,
        producer_epoch,
        producer.coordinator_epoch,
    ))
}

fn await_cleared(
    client: &Client,
    topic: &str,
    partition: i32,
    expected: &ProducerStateSnapshot,
    deadline: Instant,
) -> Result<Vec<ProducerStateSnapshot>, AdapterError> {
    loop {
        let observed = protocol_admin_producers::query(client, topic, partition, deadline)?;
        if matches!(observed.as_slice(), [producer]
            if producer.producer_id == expected.producer_id
                && producer.producer_epoch == expected.producer_epoch
                && producer.current_transaction_start_offset.is_none())
        {
            return Ok(observed);
        }
        if Instant::now() >= deadline {
            return Err(invalid("transaction remained open through the deadline"));
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn emit_state<W: Write>(
    writer: &mut W,
    command_id: &CommandId,
    operation_id: OperationId,
    topic: &str,
    partition: i32,
    producers: Vec<ProducerStateSnapshot>,
) -> Result<(), AdapterError> {
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id.clone(),
            AdapterEvent::ProducersDescribed(AdminProducersDescription {
                operation_id,
                topic: topic.to_owned(),
                partition,
                producers,
            }),
        ),
    )
}

fn singleton(operations: Vec<BatchRecord>) -> Result<BatchRecord, AdapterError> {
    <[BatchRecord; 1]>::try_from(operations)
        .map(|[operation]| operation)
        .map_err(|_| invalid("requires exactly one staged record"))
}

fn invalid(detail: &str) -> AdapterError {
    AdapterError::TransactionResult(format!("admin partition abort {detail}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abort_spec_uses_exact_public_producer_identity() {
        let spec = abort_spec(&[producer(Some(17))], "orders", 2)
            .unwrap_or_else(|error| panic!("abort spec: {error}"));
        assert_eq!(spec.topic_partition().topic(), "orders");
        assert_eq!(spec.topic_partition().partition(), 2);
        assert_eq!(spec.producer_id(), 71);
        assert_eq!(spec.producer_epoch(), 3);
        assert_eq!(spec.coordinator_epoch(), 4);
        assert_eq!(spec.requested_transaction_version(), 0);
    }

    #[test]
    fn abort_spec_requires_one_open_transaction() {
        assert!(abort_spec(&[], "orders", 2).is_err());
        assert!(abort_spec(&[producer(None)], "orders", 2).is_err());
        assert!(abort_spec(&[producer(Some(17)), producer(Some(18))], "orders", 2).is_err());
    }

    fn producer(start: Option<i64>) -> ProducerStateSnapshot {
        ProducerStateSnapshot {
            producer_id: 71,
            producer_epoch: 3,
            last_sequence: 0,
            last_timestamp: 1_700_000_000_000,
            coordinator_epoch: 4,
            current_transaction_start_offset: start,
        }
    }
}
