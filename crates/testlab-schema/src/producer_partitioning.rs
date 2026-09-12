//! Producer partition selection remains explicit in scenario intent and adapter commands.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ByteStringError, RecordSpec};

const JAVA_SIGNED_INT_MAX: u32 = i32::MAX.unsigned_abs();
const MURMUR2_SEED: u32 = 0x9747_b28c;
const MURMUR2_MULTIPLIER: u32 = 0x5bd1_e995;

/// Partition selection used by one ordinary producer send.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProducerPartitioning {
    /// Passes the scenario record's exact partition to the public producer.
    #[default]
    Explicit,
    /// Omits an explicit partition and expects Java-compatible keyed selection.
    JavaKeyed {
        /// Exact logical topic partition count used by the independent oracle.
        partition_count: u32,
    },
}

impl ProducerPartitioning {
    /// Returns whether this selection preserves the record's explicit partition.
    pub const fn is_explicit(&self) -> bool {
        matches!(self, Self::Explicit)
    }

    /// Returns the declared automatic-selection partition count, when present.
    pub const fn partition_count(&self) -> Option<u32> {
        match self {
            Self::Explicit => None,
            Self::JavaKeyed { partition_count } => Some(*partition_count),
        }
    }

    /// Resolves the independently expected partition for one scenario record.
    pub fn expected_partition(
        &self,
        record: &RecordSpec,
    ) -> Result<i32, ProducerPartitioningError> {
        let Self::JavaKeyed { partition_count } = self else {
            return Ok(record.partition);
        };
        if *partition_count == 0 || *partition_count > JAVA_SIGNED_INT_MAX {
            return Err(ProducerPartitioningError::InvalidPartitionCount(
                *partition_count,
            ));
        }
        let key = record
            .key
            .as_ref()
            .ok_or(ProducerPartitioningError::MissingKey)?
            .decode()?;
        let key_length = u32::try_from(key.len())
            .ok()
            .filter(|length| *length <= JAVA_SIGNED_INT_MAX)
            .ok_or(ProducerPartitioningError::KeyLengthUnrepresentable)?;
        let hash = murmur2(&key, key_length) & JAVA_SIGNED_INT_MAX;
        let partition = i32::try_from(hash % *partition_count)
            .map_err(|_| ProducerPartitioningError::InvalidPartitionCount(*partition_count))?;
        if record.partition != partition {
            return Err(ProducerPartitioningError::ExpectedPartition {
                declared: record.partition,
                calculated: partition,
            });
        }
        Ok(partition)
    }
}

fn murmur2(serialized_key: &[u8], key_length: u32) -> u32 {
    let mut hash = MURMUR2_SEED ^ key_length;
    let mut chunks = serialized_key.chunks_exact(4);
    for chunk in &mut chunks {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(chunk);
        let mut word = u32::from_le_bytes(bytes);
        word = word.wrapping_mul(MURMUR2_MULTIPLIER);
        word ^= word >> 24;
        word = word.wrapping_mul(MURMUR2_MULTIPLIER);
        hash = hash.wrapping_mul(MURMUR2_MULTIPLIER);
        hash ^= word;
    }
    let tail = chunks.remainder();
    let mut tail_word = 0_u32;
    for (index, byte) in tail.iter().enumerate() {
        tail_word |= u32::from(*byte) << (index * 8);
    }
    if !tail.is_empty() {
        hash ^= tail_word;
        hash = hash.wrapping_mul(MURMUR2_MULTIPLIER);
    }
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(MURMUR2_MULTIPLIER);
    hash ^ (hash >> 15)
}

/// Invalid automatic producer partitioning intent.
#[derive(Debug, Error)]
pub enum ProducerPartitioningError {
    /// Java-compatible hashing requires a positive signed partition count.
    #[error("automatic partition count must be between 1 and {JAVA_SIGNED_INT_MAX}, got {0}")]
    InvalidPartitionCount(u32),
    /// The deterministic automatic oracle requires a present key.
    #[error("Java-keyed automatic partitioning requires a present record key")]
    MissingKey,
    /// Java clients cannot represent a serialized key longer than a signed array.
    #[error("serialized record key exceeds Java's signed array-length domain")]
    KeyLengthUnrepresentable,
    /// The declared observation target must agree with the independent oracle.
    #[error(
        "declared partition {declared} does not match calculated Java-keyed partition {calculated}"
    )]
    ExpectedPartition {
        /// Partition retained by the scenario record for observation.
        declared: i32,
        /// Partition calculated from the serialized key.
        calculated: i32,
    },
    /// One portable key could not be decoded.
    #[error(transparent)]
    Bytes(#[from] ByteStringError),
}

#[cfg(test)]
#[path = "producer_partitioning_test.rs"]
mod tests;

#[path = "producer_partitioning_validation.rs"]
pub(crate) mod validation;
