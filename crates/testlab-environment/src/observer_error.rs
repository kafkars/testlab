//! Observer errors keep failed capture distinct from a valid empty snapshot.

use thiserror::Error;

/// One independent Kafka observation failure.
#[derive(Debug, Error)]
pub(super) enum ObserverError {
    #[error("Kafka observer failed: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Kafka observer deadline overflowed")]
    DeadlineOverflow,
    #[error("Kafka observer deadline expired")]
    Deadline,
    #[error("Kafka observer received an invalid record: {0}")]
    InvalidRecord(String),
    #[error("Kafka observer received an unexpected topic-partition {0}:{1}")]
    UnexpectedPartition(String, i32),
    #[error("Kafka observer offset overflowed")]
    OffsetOverflow,
    #[error("Kafka observer ordinal overflowed")]
    ObservationOverflow,
}

impl ObserverError {
    pub(super) fn is_timeout(&self) -> bool {
        matches!(self, Self::Deadline)
    }
}
