//! Canonical record inputs and digests anchor broker-visible integrity checks.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ByteString, ByteStringError};

const RECORD_DIGEST_VERSION: &[u8] = b"testlab-record-v1";

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
    /// Nullable key.
    pub key: Option<ByteString>,
    /// Nullable value.
    pub value: Option<ByteString>,
    /// Ordered headers.
    #[serde(default)]
    pub headers: Vec<HeaderSpec>,
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

    /// Calculates the canonical SHA-256 digest for external comparison.
    pub fn digest(&self) -> Result<String, RecordError> {
        self.validate()?;
        let mut digest = Sha256::new();
        digest.update(RECORD_DIGEST_VERSION);
        update_bytes(&mut digest, self.topic.as_bytes())?;
        digest.update(self.partition.to_be_bytes());
        digest.update(self.sequence.to_be_bytes());
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
    /// A collection length could not fit the canonical digest format.
    #[error("record field length exceeds the canonical u64 bound")]
    LengthOverflow,
    /// One byte field could not be decoded.
    #[error(transparent)]
    Bytes(#[from] ByteStringError),
}
