//! Batched record deletion preserves caller order, sentinels, and per-target outcomes.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminRecordsBatchDeleted, AdminRecordsDeletionOutcome,
    CommandId, DeleteRecordsBatchCommand, DeleteRecordsBatchSelection, DeleteRecordsBoundary,
    OperationId,
};

use crate::AdapterError;
use crate::kafkars_api::{DeleteRecordsTarget, KafkaError, TopicPartition};
use crate::normalize;
use crate::protocol::emit;
use crate::state::AdapterState;

pub(crate) fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteRecordsBatchCommand,
) -> Result<(), AdapterError> {
    let result = state
        .client(&command.client_id)?
        .admin()
        .delete_records(public_targets(&command.targets))
        .deadline_after(Duration::from_millis(command.timeout_ms))
        .submit()
        .wait()
        .map_err(AdapterError::Client)?;
    let entries = result
        .into_records()
        .into_entries()
        .into_iter()
        .map(|(identity, result)| (identity, result.map(|value| value.low_watermark())))
        .collect();
    let outcomes = outcomes(entries, &command.targets, &command.operation_id)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::RecordsBatchDeleted(AdminRecordsBatchDeleted {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

pub(crate) fn public_targets(targets: &[DeleteRecordsBatchSelection]) -> Vec<DeleteRecordsTarget> {
    targets
        .iter()
        .map(|target| match target.boundary {
            DeleteRecordsBoundary::Offset { offset } => {
                DeleteRecordsTarget::before_offset(target.topic.clone(), target.partition, offset)
            }
            DeleteRecordsBoundary::HighWatermark => {
                DeleteRecordsTarget::before_high_watermark(target.topic.clone(), target.partition)
            }
        })
        .collect()
}

pub(crate) fn outcomes(
    entries: Vec<(TopicPartition, Result<i64, KafkaError>)>,
    expected: &[DeleteRecordsBatchSelection],
    operation_id: &OperationId,
) -> Result<Vec<AdminRecordsDeletionOutcome>, AdapterError> {
    if entries.len() != expected.len() {
        return Err(invalid(
            operation_id,
            "returned a different number of record-deletion outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected)
        .map(|((identity, result), expected)| {
            if identity.topic() != expected.topic.as_str()
                || identity.partition() != expected.partition
                || identity.start_position().is_some()
            {
                return Err(invalid(
                    operation_id,
                    "returned record-deletion outcomes outside caller order",
                ));
            }
            let (low_watermark, error_code) = match result {
                Ok(value) => (Some(value), None),
                Err(error) => (None, Some(normalize::error_code(&error))),
            };
            Ok(AdminRecordsDeletionOutcome {
                topic: identity.topic().to_owned(),
                partition: identity.partition(),
                low_watermark,
                error_code,
            })
        })
        .collect()
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
