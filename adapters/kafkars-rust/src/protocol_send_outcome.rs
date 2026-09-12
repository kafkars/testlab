//! Producer outcomes retain complete public acknowledgement receipts.

use std::io::Write;

use testlab_schema::{
    AdapterEvent, AdapterEventEnvelope, CommandId, OperationId, ProducerReceipt, TerminalStatus,
};

use crate::AdapterError;
use crate::kafkars_api::RecordMetadata;
use crate::protocol::emit;

#[derive(Debug)]
pub(crate) enum SendOutcome {
    Rejected {
        code: String,
    },
    Accepted {
        status: TerminalStatus,
        code: Option<String>,
        partition: Option<i32>,
        offset: Option<i64>,
        timestamp_millis: Option<i64>,
        receipt: Option<ProducerReceipt>,
    },
}

impl SendOutcome {
    pub(crate) fn acknowledged(metadata: RecordMetadata) -> Self {
        let receipt = metadata_receipt(&metadata);
        Self::Accepted {
            status: TerminalStatus::Acknowledged,
            code: None,
            partition: Some(metadata.partition()),
            offset: Some(metadata.offset()),
            timestamp_millis: metadata.timestamp_milliseconds(),
            receipt: Some(receipt),
        }
    }

    pub(crate) fn failed(status: TerminalStatus, code: String) -> Self {
        Self::Accepted {
            status,
            code: Some(code),
            partition: None,
            offset: None,
            timestamp_millis: None,
            receipt: None,
        }
    }
}

pub(crate) fn metadata_receipt(metadata: &RecordMetadata) -> ProducerReceipt {
    ProducerReceipt {
        topic: metadata.topic().to_owned(),
        topic_uuid: metadata.topic_uuid().map(|value| value.into_bytes()),
        leader_epoch: metadata.leader_epoch(),
        serialized_key_size: metadata.serialized_key_size(),
        serialized_value_size: metadata.serialized_value_size(),
    }
}

pub(crate) fn emit_send_outcome<W: Write>(
    writer: &mut W,
    command_id: CommandId,
    operation_id: OperationId,
    outcome: SendOutcome,
) -> Result<(), AdapterError> {
    match outcome {
        SendOutcome::Rejected { code } => emit(
            writer,
            &AdapterEventEnvelope::new(
                command_id,
                AdapterEvent::OperationRejected { operation_id, code },
            ),
        ),
        SendOutcome::Accepted {
            status,
            code,
            partition,
            offset,
            timestamp_millis,
            receipt,
        } => {
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id.clone(),
                    AdapterEvent::OperationAccepted {
                        operation_id: operation_id.clone(),
                    },
                ),
            )?;
            emit(
                writer,
                &AdapterEventEnvelope::new(
                    command_id,
                    AdapterEvent::OperationTerminal {
                        operation_id,
                        status,
                        code,
                        partition,
                        offset,
                        timestamp_millis,
                        receipt,
                    },
                ),
            )
        }
    }
}
