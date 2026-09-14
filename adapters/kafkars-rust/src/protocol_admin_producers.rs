//! Active-producer discovery preserves every exact public producer-state field.

use std::io::Write;
use std::time::Instant;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminProducersDescription, CommandId,
    DescribeProducersCommand, ProducerStateSnapshot,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::{Client, TopicPartition};
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeProducersCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let producers = query_with_broker(
        client,
        &command.topic,
        command.partition,
        command.broker_id,
        deadline,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::ProducersDescribed(AdminProducersDescription {
                operation_id: command.operation_id,
                topic: command.topic,
                partition: command.partition,
                producers,
            }),
        ),
    )
}

pub(crate) fn query(
    client: &Client,
    topic: &str,
    partition: i32,
    deadline: Instant,
) -> Result<Vec<ProducerStateSnapshot>, AdapterError> {
    query_with_broker(client, topic, partition, None, deadline)
}

fn query_with_broker(
    client: &Client,
    topic: &str,
    partition: i32,
    broker_id: Option<i32>,
    deadline: Instant,
) -> Result<Vec<ProducerStateSnapshot>, AdapterError> {
    let target = TopicPartition::new(topic.to_owned(), partition);
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            let request = client.admin().describe_producers([target.clone()]);
            let request = match broker_id {
                Some(broker_id) => request.broker_id(broker_id),
                None => request,
            };
            request.deadline_after(remaining).submit().wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let mut entries = result.into_partitions().into_entries();
    if entries.len() != 1 {
        return Err(invalid("returned a non-singleton partition result"));
    }
    let Some((returned, result)) = entries.pop() else {
        return Err(invalid("returned no partition result"));
    };
    if returned.topic() != topic
        || returned.partition() != partition
        || returned.start_position().is_some()
    {
        return Err(invalid("returned a mismatched partition identity"));
    }
    let producers = result
        .map_err(AdapterError::Client)?
        .into_iter()
        .map(|producer| ProducerStateSnapshot {
            producer_id: producer.producer_id(),
            producer_epoch: producer.producer_epoch(),
            last_sequence: producer.last_sequence(),
            last_timestamp: producer.last_timestamp(),
            coordinator_epoch: producer.coordinator_epoch(),
            current_transaction_start_offset: producer.current_transaction_start_offset(),
        })
        .collect::<Vec<_>>();
    validate_producers(&producers)?;
    Ok(producers)
}

fn validate_producers(producers: &[ProducerStateSnapshot]) -> Result<(), AdapterError> {
    if producers.iter().any(|producer| {
        producer.producer_id < 0
            || producer.producer_epoch < 0
            || producer.last_sequence < -1
            || producer.last_timestamp < -1
            || producer.coordinator_epoch < -1
            || producer
                .current_transaction_start_offset
                .is_some_and(|offset| offset < 0)
    }) || producers
        .windows(2)
        .any(|pair| pair[0].producer_id >= pair[1].producer_id)
    {
        return Err(invalid(
            "returned malformed or noncanonical active-producer state",
        ));
    }
    Ok(())
}

fn invalid(detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("DescribeProducers {detail}"))
}
