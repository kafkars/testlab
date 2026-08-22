//! Observed Kafka bytes become records only when correlation metadata is valid.

use testlab_schema::{
    BrokerObservation, ByteEncoding, ByteString, HeaderSpec, OperationId, RecordSpec,
};

use crate::observer_error::ObserverError;

pub(super) const OPERATION_HEADER: &str = "testlab-operation-id";
pub(super) const SEQUENCE_HEADER: &str = "testlab-sequence";

pub(super) struct CapturedRecord<'a> {
    pub(super) topic: &'a str,
    pub(super) partition: i32,
    pub(super) offset: i64,
    pub(super) key: Option<&'a [u8]>,
    pub(super) value: Option<&'a [u8]>,
    pub(super) headers: Vec<(&'a str, Option<&'a [u8]>)>,
}

pub(super) fn normalize(
    observation: u64,
    captured: CapturedRecord<'_>,
) -> Result<BrokerObservation, ObserverError> {
    let operation = required_header(&captured.headers, OPERATION_HEADER)?;
    let operation =
        std::str::from_utf8(operation).map_err(|error| invalid(OPERATION_HEADER, error))?;
    let operation_id =
        OperationId::new(operation).map_err(|error| invalid(OPERATION_HEADER, error))?;
    let sequence = required_header(&captured.headers, SEQUENCE_HEADER)?;
    let sequence = std::str::from_utf8(sequence)
        .map_err(|error| invalid(SEQUENCE_HEADER, error))?
        .parse::<u64>()
        .map_err(|error| invalid(SEQUENCE_HEADER, error))?;
    let record = RecordSpec {
        topic: captured.topic.to_owned(),
        partition: captured.partition,
        sequence,
        key: captured.key.map(bytes),
        value: captured.value.map(bytes),
        headers: captured
            .headers
            .into_iter()
            .map(|(name, value)| HeaderSpec {
                name: name.to_owned(),
                value: value.map(bytes),
            })
            .collect(),
    };
    let digest = record
        .digest()
        .map_err(|error| ObserverError::InvalidRecord(error.to_string()))?;
    Ok(BrokerObservation {
        observation,
        offset: captured.offset,
        operation_id,
        record,
        digest,
    })
}

fn required_header<'a>(
    headers: &'a [(&str, Option<&[u8]>)],
    name: &str,
) -> Result<&'a [u8], ObserverError> {
    let mut values = headers
        .iter()
        .filter(|(candidate, _)| *candidate == name)
        .map(|(_, value)| *value);
    let value = values.next().ok_or_else(|| invalid(name, "missing"))?;
    if values.next().is_some() {
        return Err(invalid(name, "duplicated"));
    }
    value.ok_or_else(|| invalid(name, "null"))
}

fn bytes(value: &[u8]) -> ByteString {
    ByteString {
        encoding: ByteEncoding::Hex,
        data: hex::encode(value),
    }
}

fn invalid(name: &str, error: impl std::fmt::Display) -> ObserverError {
    ObserverError::InvalidRecord(format!("header {name} is {error}"))
}
