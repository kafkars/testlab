//! Plural topic deletion preserves caller order and every public resource outcome.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, AdminTopicDeletionOutcome, AdminTopicsDeletion, CommandId,
    DeleteTopicsCommand, OperationId,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::kafkars_api::KafkaError;
use crate::normalize;
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn delete<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DeleteTopicsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .delete_topics(command.topics.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let outcomes = outcomes(
        result.into_entries(),
        &command.topics,
        &command.operation_id,
    )?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TopicsDeleted(AdminTopicsDeletion {
                operation_id: command.operation_id,
                outcomes,
            }),
        ),
    )
}

pub(crate) fn outcomes(
    entries: Vec<(String, Result<(), KafkaError>)>,
    expected: &[String],
    operation_id: &OperationId,
) -> Result<Vec<AdminTopicDeletionOutcome>, AdapterError> {
    if entries.len() != expected.len() {
        return Err(invalid(
            operation_id,
            "returned a different number of topic outcomes than requested",
        ));
    }
    entries
        .into_iter()
        .zip(expected)
        .map(|((topic, result), expected)| {
            if &topic != expected {
                return Err(invalid(
                    operation_id,
                    "returned topic outcomes outside caller order",
                ));
            }
            Ok(AdminTopicDeletionOutcome {
                topic,
                error_code: result.err().map(|error| normalize::error_code(&error)),
            })
        })
        .collect()
}

fn invalid(operation_id: &OperationId, detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("admin operation {operation_id} {detail}"))
}
