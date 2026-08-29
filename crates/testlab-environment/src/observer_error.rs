//! Observer errors keep failed capture distinct from a valid empty snapshot.

use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
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
    #[error("Kafka observer received invalid broker state: {0}")]
    InvalidBrokerState(String),
    #[error("Kafka admin observation target is invalid: {0}")]
    InvalidTarget(String),
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
            || matches!(self, Self::Kafka(error) if Self::kafka_is_timeout(error))
    }

    pub(super) fn kafka_is_timeout(error: &KafkaError) -> bool {
        matches!(
            error,
            KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut)
                | KafkaError::GroupListFetch(RDKafkaErrorCode::OperationTimedOut)
                | KafkaError::OffsetFetch(RDKafkaErrorCode::OperationTimedOut)
        )
    }
}
