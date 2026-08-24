//! Admin commands validate exact public batch outcomes before reporting success.

use std::io::Write;
use std::time::Duration;

use kafkars::{NewPartitions, NewTopic};
use testlab_schema::{AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId, OperationId};

use crate::AdapterError;
use crate::protocol::emit;
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
            let request =
                NewTopic::new(topic.clone(), partitions).replication_factor(replication_factor);
            let result = state
                .client(&client_id)?
                .admin()
                .create_topics([request])
                .deadline_after(Duration::from_millis(timeout_ms))
                .submit()
                .wait()
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
            let request = NewPartitions::new(topic.clone(), total_count);
            let result = state
                .client(&client_id)?
                .admin()
                .create_partitions([request])
                .deadline_after(Duration::from_millis(timeout_ms))
                .submit()
                .wait()
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
            "non-admin command reached admin dispatcher".to_owned(),
        )),
    }
}

pub(crate) fn validate_single_topic_result(
    entries: Vec<(String, Result<(), kafkars::KafkaError>)>,
    operation_id: &OperationId,
    expected_topic: &str,
) -> Result<(), AdapterError> {
    let mut entries = entries.into_iter();
    let Some((actual_topic, result)) = entries.next() else {
        return Err(AdapterError::AdminResult(format!(
            "admin operation {operation_id} returned no topic result"
        )));
    };
    if entries.next().is_some() || actual_topic != expected_topic {
        return Err(AdapterError::AdminResult(format!(
            "admin operation {operation_id} returned an unexpected topic result"
        )));
    }
    result.map_err(AdapterError::Client)
}
