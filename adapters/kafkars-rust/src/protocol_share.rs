//! Share protocol commands preserve batch identity and public terminal certainty.

use std::io::Write;
use std::time::Duration;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, CommandId, TerminalStatus,
};

use crate::AdapterError;
use crate::normalize;
use crate::protocol::emit;
use crate::share_consumers::ShareAcknowledgeOutcome;
use crate::share_consumers::ShareConsumerRegistration;
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &mut AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    let event = match command {
        AdapterCommand::CreateShareConsumer {
            client_id,
            consumer_id,
            group_id,
            topic,
            membership_timeout_ms,
            close_timeout_ms,
            configuration,
        } => {
            let client = state.client(&client_id)?.clone();
            state.share_consumers.create(
                &client,
                ShareConsumerRegistration {
                    client_id,
                    consumer_id: consumer_id.clone(),
                    group_id,
                    topic,
                    membership_timeout: Duration::from_millis(membership_timeout_ms),
                    close_timeout: Duration::from_millis(close_timeout_ms),
                    configuration,
                },
            )?;
            AdapterEvent::ShareConsumerCreated { consumer_id }
        }
        AdapterCommand::ShareReceive {
            consumer_id,
            receive_id,
            timeout_ms,
        } => {
            let facts = state.share_consumers.receive(
                &consumer_id,
                receive_id.clone(),
                Duration::from_millis(timeout_ms),
            )?;
            AdapterEvent::ShareReceiveCompleted {
                consumer_id,
                receive_id,
                records: facts.records,
                acquisition_count: facts.acquisition_count,
                member_epoch: facts.member_epoch,
                assignment_epoch: facts.assignment_epoch,
            }
        }
        AdapterCommand::ShareAcknowledge {
            consumer_id,
            receive_id,
            acknowledgement_id,
            dispositions,
            timeout_ms,
        } => {
            let outcome = state.share_consumers.acknowledge(
                &consumer_id,
                &receive_id,
                dispositions.clone(),
                Duration::from_millis(timeout_ms),
            )?;
            acknowledgement_event(acknowledgement_id, receive_id, dispositions, outcome)
        }
        AdapterCommand::DropShareBatch {
            consumer_id,
            receive_id,
        } => {
            state
                .share_consumers
                .drop_batch(&consumer_id, &receive_id)?;
            AdapterEvent::ShareBatchDropped { receive_id }
        }
        AdapterCommand::CloseShareConsumer { consumer_id } => {
            let outcome = state.share_consumers.close(&consumer_id)?;
            let (success, delivery, code) = match outcome.error {
                Some(error) => {
                    let failure = normalize::delivery_failure(&error);
                    (false, Some(failure.status), Some(failure.code))
                }
                None => (true, None, None),
            };
            AdapterEvent::ShareConsumerClosed {
                consumer_id,
                success,
                delivery,
                code,
            }
        }
        _ => {
            return Err(AdapterError::State(
                "non-share command reached share dispatcher".to_owned(),
            ));
        }
    };
    emit(writer, &AdapterEventEnvelope::new(command_id, event))
}

fn acknowledgement_event(
    acknowledgement_id: testlab_schema::OperationId,
    receive_id: testlab_schema::OperationId,
    dispositions: Vec<testlab_schema::ShareDisposition>,
    outcome: ShareAcknowledgeOutcome,
) -> AdapterEvent {
    let (success, delivery, code) = if let Some(error) = outcome.error {
        let failure = normalize::delivery_failure(&error);
        (false, Some(failure.status), Some(failure.code))
    } else if outcome.broker_codes.is_empty() {
        (true, None, None)
    } else {
        (
            false,
            Some(TerminalStatus::PossiblySent),
            Some(format!("broker_{:?}", outcome.broker_codes)),
        )
    };
    AdapterEvent::ShareAcknowledgementCompleted {
        acknowledgement_id,
        receive_id,
        dispositions,
        success,
        delivery,
        code,
    }
}
