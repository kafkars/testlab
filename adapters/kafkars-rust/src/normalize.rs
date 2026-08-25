//! Public Kafkars values map to protocol records without inferred certainty.

use kafkars::{DeliveryStatus, ErrorKind, Header, KafkaError, Record};
use testlab_schema::{RecordSpec, TerminalStatus};

use crate::AdapterError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeliveryFailure {
    pub(crate) status: TerminalStatus,
    pub(crate) code: String,
}

pub(crate) fn record(spec: RecordSpec) -> Result<Record, AdapterError> {
    let mut record = Record::to(spec.topic).partition(spec.partition);
    if let Some(key) = spec.key {
        record = record.key(key.decode()?);
    }
    if let Some(value) = spec.value {
        record = record.value(value.decode()?);
    }
    for header in spec.headers {
        let header = match header.value {
            Some(value) => Header::new(header.name, value.decode()?),
            None => Header::null(header.name),
        };
        record = record.with_header(header);
    }
    Ok(record)
}

pub(crate) fn delivery_failure(error: &KafkaError) -> DeliveryFailure {
    let status = match error.delivery_status() {
        Some(DeliveryStatus::NotSent) => TerminalStatus::DefinitelyNotSent,
        Some(DeliveryStatus::PossiblySent) | None => TerminalStatus::PossiblySent,
    };
    DeliveryFailure {
        status,
        code: error_code(error),
    }
}

pub(crate) fn error_code(error: &KafkaError) -> String {
    let kind = match error.kind() {
        ErrorKind::Configuration => "configuration",
        ErrorKind::Backpressure => "backpressure",
        ErrorKind::Access => "access",
        ErrorKind::Broker => "broker",
        ErrorKind::Compatibility => "compatibility",
        ErrorKind::Fenced => "fenced",
        #[cfg(kafkars_share_candidate)]
        ErrorKind::Identity => "identity",
        ErrorKind::InvalidRecord => "invalid_record",
        ErrorKind::Routing => "routing",
        ErrorKind::Transport => "transport",
        ErrorKind::Timeout => "timeout",
        ErrorKind::Cancelled => "cancelled",
        ErrorKind::State => "state",
        ErrorKind::Internal => "internal",
    };
    match error.broker_code() {
        Some(code) => format!("{kind}:broker_{code}"),
        None => kind.to_owned(),
    }
}
