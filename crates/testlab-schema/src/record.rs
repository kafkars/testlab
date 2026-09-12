//! Canonical record inputs and digests anchor broker-visible integrity checks.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ByteString, ByteStringError, OperationId};

const RECORD_DIGEST_VERSION: &[u8] = b"testlab-record-v2";

/// One ordered Kafka record header.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderSpec {
    /// Header name as UTF-8 text.
    pub name: String,
    /// Nullable header value.
    pub value: Option<ByteString>,
}

/// One logical record offered by a scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordSpec {
    /// Destination topic.
    pub topic: String,
    /// Destination partition.
    pub partition: i32,
    /// Scenario-local logical sequence.
    pub sequence: u64,
    /// Optional caller-selected Kafka record timestamp in Unix milliseconds.
    #[serde(default)]
    pub timestamp_millis: Option<i64>,
    /// Nullable key.
    pub key: Option<ByteString>,
    /// Nullable value.
    pub value: Option<ByteString>,
    /// Ordered headers.
    #[serde(default)]
    pub headers: Vec<HeaderSpec>,
}

/// One exact record observed through a packaged consumer API.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumedRecord {
    /// Kafka topic returned by the client.
    pub topic: String,
    /// Kafka partition returned by the client.
    pub partition: i32,
    /// Absolute Kafka log offset returned by the client.
    pub offset: i64,
    /// Kafka timestamp when exposed by the client.
    pub timestamp_millis: Option<i64>,
    /// Nullable exact key bytes.
    pub key: Option<ByteString>,
    /// Nullable exact value bytes.
    pub value: Option<ByteString>,
    /// Ordered, duplicate-preserving headers.
    pub headers: Vec<HeaderSpec>,
}

/// Broker-correlated facts retained by one public assigned-consumer Fetch batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerFetchEvidence {
    /// Exact topic retained by the Fetch lease.
    pub topic: String,
    /// Nonzero broker-issued topic UUID.
    pub topic_uuid: [u8; 16],
    /// Exact source partition.
    pub partition: i32,
    /// Offset requested by this Fetch.
    pub requested_offset: i64,
    /// Exclusive next offset after complete broker progress.
    pub next_offset: i64,
    /// Next offset reported independently by the public batch checkpoint.
    pub checkpoint_next_offset: i64,
    /// Broker log-start offset when supplied.
    pub log_start_offset: Option<i64>,
    /// Broker last-stable offset when supplied.
    pub last_stable_offset: Option<i64>,
    /// Broker high watermark when supplied.
    pub high_watermark: Option<i64>,
    /// Exact stable Fetch-output bytes retained by the lease.
    pub retained_bytes: usize,
}

/// Complete public acknowledgement receipt for one produced record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerReceipt {
    /// Exact topic name retained by the public receipt.
    pub topic: String,
    /// Nonzero topic UUID proven before the Produce attempt, when requested.
    pub topic_uuid: Option<[u8; 16]>,
    /// Acknowledged leader epoch when supplied by Kafka.
    pub leader_epoch: Option<i32>,
    /// Exact serialized key length, distinguishing null from present empty bytes.
    pub serialized_key_size: Option<usize>,
    /// Exact serialized value length, distinguishing null from present empty bytes.
    pub serialized_value_size: Option<usize>,
}

impl RecordSpec {
    /// Validates portable Kafka record constraints used by testlab.
    pub fn validate(&self) -> Result<(), RecordError> {
        if self.topic.is_empty() {
            return Err(RecordError::EmptyTopic);
        }
        if self.topic.len() > 249 {
            return Err(RecordError::TopicTooLong);
        }
        if self.partition < 0 {
            return Err(RecordError::NegativePartition(self.partition));
        }
        for header in &self.headers {
            if header.name.is_empty() {
                return Err(RecordError::EmptyHeaderName);
            }
            if let Some(value) = &header.value {
                value.decode()?;
            }
        }
        if let Some(key) = &self.key {
            key.decode()?;
        }
        if let Some(value) = &self.value {
            value.decode()?;
        }
        Ok(())
    }

    pub(crate) fn validate_correlation(
        &self,
        operation_id: &OperationId,
    ) -> Result<(), RecordError> {
        required_header(
            &self.headers,
            "testlab-operation-id",
            operation_id.as_str().as_bytes(),
        )?;
        required_header(
            &self.headers,
            "testlab-sequence",
            self.sequence.to_string().as_bytes(),
        )
    }

    /// Calculates the canonical SHA-256 digest for external comparison.
    pub fn digest(&self) -> Result<String, RecordError> {
        self.validate()?;
        let mut digest = Sha256::new();
        digest.update(RECORD_DIGEST_VERSION);
        update_bytes(&mut digest, self.topic.as_bytes())?;
        digest.update(self.partition.to_be_bytes());
        digest.update(self.sequence.to_be_bytes());
        match self.timestamp_millis {
            Some(timestamp) => {
                digest.update([1]);
                digest.update(timestamp.to_be_bytes());
            }
            None => digest.update([0]),
        }
        update_optional(&mut digest, self.key.as_ref())?;
        update_optional(&mut digest, self.value.as_ref())?;
        digest.update(length(self.headers.len())?.to_be_bytes());
        for header in &self.headers {
            update_bytes(&mut digest, header.name.as_bytes())?;
            update_optional(&mut digest, header.value.as_ref())?;
        }
        Ok(hex::encode(digest.finalize()))
    }
}

fn required_header(
    headers: &[HeaderSpec],
    name: &'static str,
    expected: &[u8],
) -> Result<(), RecordError> {
    let mut matches = headers.iter().filter(|header| header.name == name);
    let header = matches
        .next()
        .ok_or(RecordError::MissingCorrelationHeader(name))?;
    if matches.next().is_some() {
        return Err(RecordError::DuplicateCorrelationHeader(name));
    }
    let actual = header
        .value
        .as_ref()
        .ok_or(RecordError::NullCorrelationHeader(name))?
        .decode()?;
    if actual != expected {
        return Err(RecordError::MismatchedCorrelationHeader(name));
    }
    Ok(())
}

fn update_optional(digest: &mut Sha256, value: Option<&ByteString>) -> Result<(), RecordError> {
    match value {
        Some(value) => {
            digest.update([1]);
            update_bytes(digest, &value.decode()?)?;
        }
        None => digest.update([0]),
    }
    Ok(())
}

fn update_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), RecordError> {
    digest.update(length(value.len())?.to_be_bytes());
    digest.update(value);
    Ok(())
}

fn length(value: usize) -> Result<u64, RecordError> {
    u64::try_from(value).map_err(|_| RecordError::LengthOverflow)
}

/// Failure while validating or hashing one record.
#[derive(Debug, Error)]
pub enum RecordError {
    /// Topic names must be nonempty.
    #[error("record topic must not be empty")]
    EmptyTopic,
    /// Kafka topic names are bounded.
    #[error("record topic exceeds 249 bytes")]
    TopicTooLong,
    /// Scenario records require an explicit nonnegative partition.
    #[error("record partition must be nonnegative, got {0}")]
    NegativePartition(i32),
    /// Header names must be nonempty.
    #[error("record header name must not be empty")]
    EmptyHeaderName,
    /// Independent Kafka observation requires one exact operation header.
    #[error("record is missing required correlation header {0}")]
    MissingCorrelationHeader(&'static str),
    /// Correlation headers are unique because observation must be deterministic.
    #[error("record repeats required correlation header {0}")]
    DuplicateCorrelationHeader(&'static str),
    /// Correlation headers carry non-null identity bytes.
    #[error("record correlation header {0} must not be null")]
    NullCorrelationHeader(&'static str),
    /// Correlation headers must agree with the enclosing operation and record.
    #[error("record correlation header {0} does not match its declared value")]
    MismatchedCorrelationHeader(&'static str),
    /// A collection length could not fit the canonical digest format.
    #[error("record field length exceeds the canonical u64 bound")]
    LengthOverflow,
    /// One byte field could not be decoded.
    #[error(transparent)]
    Bytes(#[from] ByteStringError),
}
