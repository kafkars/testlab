//! Plural offset mutations retain every public per-partition outcome.

use std::io::Write;
use std::time::Duration;

use crate::kafkars_api::{ConsumerGroupOffsetAlteration as PublicAlteration, TopicPartition};
use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminConsumerGroupOffsetMutationOutcome,
    AdminConsumerGroupOffsetsMutation, AlterConsumerGroupOffsetsCommand, CommandId,
    ConsumerGroupOffsetSelection, DeleteConsumerGroupOffsetsCommand,
};

use crate::AdapterError;
use crate::protocol::emit;
use crate::protocol_admin_plural_result::{
    PartitionResult, ResourceResult, ordered_partition_results,
};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::AlterConsumerGroupOffsets(command) => {
            alter(state, writer, command_id, command)
        }
        AdapterCommand::DeleteConsumerGroupOffsets(command) => {
            delete(state, writer, command_id, command)
        }
        _ => Err(AdapterError::AdminResult(
            "non-plural-offset-mutation command reached its dispatcher".to_owned(),
        )),
    }
}

fn alter<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AlterConsumerGroupOffsetsCommand,
) -> Result<(), AdapterError> {
    let public_offsets = command
        .offsets
        .iter()
        .map(|offset| PublicAlteration::new(offset.topic.clone(), offset.partition, offset.offset));
    let result = state
        .client(&command.client_id)?
        .admin()
        .alter_consumer_group_offsets(command.group_id.clone(), public_offsets)
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let requested = command
        .offsets
        .iter()
        .map(|offset| (offset.topic.clone(), offset.partition))
        .collect::<Vec<_>>();
    let results = ordered_partition_results(
        result.into_offsets().into_entries(),
        &requested,
        &command.operation_id,
        "consumer-group offset alteration",
    )?;
    emit_mutation(
        writer,
        command_id,
        AdapterEvent::ConsumerGroupOffsetsAltered(AdminConsumerGroupOffsetsMutation {
            operation_id: command.operation_id,
            group_id: command.group_id,
            outcomes: mutation_outcomes(results),
        }),
    )
}

fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteConsumerGroupOffsetsCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .delete_consumer_group_offsets(
            command.group_id.clone(),
            public_partitions(&command.partitions),
        )
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let requested = command
        .partitions
        .iter()
        .map(|entry| (entry.topic.clone(), entry.partition))
        .collect::<Vec<_>>();
    let results = ordered_partition_results(
        result.into_offsets().into_entries(),
        &requested,
        &command.operation_id,
        "consumer-group offset deletion",
    )?;
    emit_mutation(
        writer,
        command_id,
        AdapterEvent::ConsumerGroupOffsetsDeleted(AdminConsumerGroupOffsetsMutation {
            operation_id: command.operation_id,
            group_id: command.group_id,
            outcomes: mutation_outcomes(results),
        }),
    )
}

pub(crate) fn mutation_outcomes(
    results: Vec<PartitionResult<()>>,
) -> Vec<AdminConsumerGroupOffsetMutationOutcome> {
    results
        .into_iter()
        .map(|result| AdminConsumerGroupOffsetMutationOutcome {
            topic: result.topic,
            partition: result.partition,
            error_code: match result.result {
                ResourceResult::Success(()) => None,
                ResourceResult::Failure(error_code) => Some(error_code),
            },
        })
        .collect()
}

fn public_partitions(
    partitions: &[ConsumerGroupOffsetSelection],
) -> impl Iterator<Item = TopicPartition> + '_ {
    partitions
        .iter()
        .map(|entry| TopicPartition::new(entry.topic.clone(), entry.partition))
}

fn emit_mutation<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    event: AdapterEvent,
) -> Result<(), AdapterError> {
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}
