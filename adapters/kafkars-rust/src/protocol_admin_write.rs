//! Admin write commands require one exact public topic result before success.

use std::io::Write;
use std::time::{Duration, Instant};

use kafkars::{NewPartitions, NewTopic, RetryAdvice};
use testlab_schema::{AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_result::validate_single_topic_result;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::CreateTopic {
            client_id,
            operation_id,
            topic,
            partitions,
            replication_factor,
            timeout_ms,
        } => {
            let started = Instant::now();
            let deadline = started
                .checked_add(Duration::from_millis(timeout_ms))
                .unwrap_or(started);
            let client = state.client(&client_id)?;
            let result = retry_until_with_remaining(
                deadline,
                |remaining| {
                    let request = NewTopic::new(topic.clone(), partitions)
                        .replication_factor(replication_factor);
                    client
                        .admin()
                        .create_topics([request])
                        .deadline_after(remaining)
                        .submit()
                        .wait()
                },
                |error| error.retry_advice() == RetryAdvice::RetrySafe,
            )
            .map_err(AdapterError::Client)?;
            validate_single_topic_result(result.into_entries(), &operation_id, &topic)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::TopicCreated {
                        operation_id,
                        topic,
                    },
                ),
            )
        }
        AdapterCommand::CreatePartitions {
            client_id,
            operation_id,
            topic,
            total_count,
            timeout_ms,
        } => {
            let started = Instant::now();
            let deadline = started
                .checked_add(Duration::from_millis(timeout_ms))
                .unwrap_or(started);
            let client = state.client(&client_id)?;
            let result = retry_until_with_remaining(
                deadline,
                |remaining| {
                    let request = NewPartitions::new(topic.clone(), total_count);
                    client
                        .admin()
                        .create_partitions([request])
                        .deadline_after(remaining)
                        .submit()
                        .wait()
                },
                |error| error.retry_advice() == RetryAdvice::RetrySafe,
            )
            .map_err(AdapterError::Client)?;
            validate_single_topic_result(result.into_entries(), &operation_id, &topic)?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::TopicPartitionsCreated {
                        operation_id,
                        topic,
                    },
                ),
            )
        }
        _ => Err(AdapterError::AdminResult(
            "non-admin-write command reached admin write dispatcher".to_owned(),
        )),
    }
}
