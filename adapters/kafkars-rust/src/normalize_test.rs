//! Public Kafkars normalization tests preserve bytes and delivery uncertainty.

use crate::kafkars_api::{DeliveryStatus, ErrorKind, KafkaError};
use testlab_schema::{ByteString, HeaderSpec, RecordSpec, TerminalStatus};

use super::normalize::error_code;
use super::normalize::{delivery_failure, record};

#[test]
fn record_conversion_preserves_nullable_and_binary_fields() {
    let converted = record(RecordSpec {
        topic: "records".to_owned(),
        partition: 3,
        sequence: 9,
        key: Some(ByteString {
            encoding: testlab_schema::ByteEncoding::Hex,
            data: "00ff".to_owned(),
        }),
        value: None,
        headers: vec![HeaderSpec {
            name: "nullable".to_owned(),
            value: None,
        }],
    })
    .unwrap_or_else(|error| panic!("convert record: {error}"));

    assert_eq!(converted.topic(), "records");
    assert_eq!(converted.explicit_partition(), Some(3));
    assert_eq!(
        converted.key_bytes().map(AsRef::as_ref),
        Some(&[0, 255][..])
    );
    assert!(converted.value_bytes().is_none());
    assert!(converted.headers()[0].value().is_none());
}

#[test]
fn missing_public_certainty_remains_possibly_sent() {
    let error = KafkaError::new(ErrorKind::Transport, "connection lost");
    let failure = delivery_failure(&error);

    assert_eq!(failure.status, TerminalStatus::PossiblySent);
}

#[test]
fn explicit_not_sent_certainty_is_preserved() {
    let error = KafkaError::new(ErrorKind::Timeout, "deadline")
        .with_delivery_status(DeliveryStatus::NotSent);
    let failure = delivery_failure(&error);

    assert_eq!(failure.status, TerminalStatus::DefinitelyNotSent);
}

#[test]
fn candidate_identity_error_has_a_stable_protocol_code() {
    let error = KafkaError::new(ErrorKind::Identity, "topic identity mismatch");

    assert_eq!(error_code(&error), "identity");
}
