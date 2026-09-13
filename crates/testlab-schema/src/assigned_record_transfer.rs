//! Owned direct-consumer transfer keeps source and destination truth explicit.

use serde::{Deserialize, Serialize};

use crate::{
    ByteString, ConsumedRecord, ConsumerId, HeaderSpec, OperationId, ProducerId, ProducerReceipt,
    RecordSpec, TerminalStatus,
};

/// Broker-visible header that gives a transferred copy its own operation identity.
pub const RECORD_TRANSFER_OPERATION_HEADER: &str = "testlab-transfer-operation-id";

/// Public conversion used to obtain owned records from one retained batch.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedRecordConversionMethod {
    /// Calls `RecordBatch::into_owned`, then `OwnedConsumerBatch::into_records`.
    #[default]
    IntoOwnedBatch,
    /// Calls the direct `RecordBatch::into_owned_records` convenience path.
    IntoOwnedRecords,
}

/// Scenario intent for one owned direct-consumer record transfer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedRecordTransferAction {
    /// Existing directly assigned consumer.
    pub consumer_id: ConsumerId,
    /// Existing ordinary producer.
    pub producer_id: ProducerId,
    /// Stable identity for the transferred producer record.
    pub operation_id: OperationId,
    /// Exact prior producer operation expected as the source record.
    pub expected_input_operation_id: OperationId,
    /// Public batch-to-record ownership conversion to exercise.
    #[serde(default)]
    pub method: AssignedRecordConversionMethod,
    /// Destination topic supplied at the public ownership-transfer boundary.
    pub target_topic: String,
    /// Explicit destination partition applied after ownership transfer.
    pub target_partition: i32,
    /// Maximum wait for the source batch before ownership transfer.
    pub timeout_ms: u64,
}

/// Adapter command omits the harness-only expected input identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedRecordTransferCommand {
    /// Existing directly assigned consumer.
    pub consumer_id: ConsumerId,
    /// Existing ordinary producer.
    pub producer_id: ProducerId,
    /// Stable identity for the transferred producer record.
    pub operation_id: OperationId,
    /// Public batch-to-record ownership conversion to exercise.
    pub method: AssignedRecordConversionMethod,
    /// Destination topic supplied at the public ownership-transfer boundary.
    pub target_topic: String,
    /// Explicit destination partition applied after ownership transfer.
    pub target_partition: i32,
    /// Maximum wait for the source batch before ownership transfer.
    pub timeout_ms: u64,
}

/// Public completion proving source ownership survived the destination terminal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedRecordTransferCompletion {
    /// Stable identity for the transferred producer record.
    pub operation_id: OperationId,
    /// Source record observed after consuming the borrowed batch into owned records.
    pub source_record: ConsumedRecord,
    /// Same source record observed after the destination producer terminal.
    pub retained_source_record: ConsumedRecord,
    /// Public destination delivery outcome.
    pub status: TerminalStatus,
    /// Normalized destination delivery error code, when failed.
    pub code: Option<String>,
    /// Public destination partition, when acknowledged.
    pub partition: Option<i32>,
    /// Public destination offset, when acknowledged.
    pub offset: Option<i64>,
    /// Public destination timestamp, when acknowledged.
    pub timestamp_millis: Option<i64>,
    /// Complete public destination delivery receipt, when acknowledged.
    pub receipt: Option<ProducerReceipt>,
}

/// Derives the exact broker-visible destination record from scenario-owned source intent.
pub fn transferred_record(
    source: &RecordSpec,
    operation_id: &OperationId,
    target_topic: &str,
    target_partition: i32,
) -> RecordSpec {
    let mut record = source.clone();
    target_topic.clone_into(&mut record.topic);
    record.partition = target_partition;
    record.headers.push(HeaderSpec {
        name: RECORD_TRANSFER_OPERATION_HEADER.to_owned(),
        value: Some(ByteString::utf8(operation_id.as_str())),
    });
    record
}
