//! Observer normalization tests prove exact bytes and required correlation metadata.

use testlab_schema::ByteEncoding;

use crate::observer_record::{CapturedRecord, normalize};

#[test]
fn observation_preserves_null_binary_and_ordered_headers() {
    let observed = normalize(
        7,
        CapturedRecord {
            topic: "records",
            partition: 2,
            offset: 11,
            key: Some(&[0, 255]),
            value: None,
            headers: vec![
                ("testlab-operation-id", Some(b"op-7")),
                ("testlab-sequence", Some(b"42")),
                ("nullable", None),
            ],
        },
    )
    .unwrap_or_else(|error| panic!("normalize observation: {error}"));

    assert_eq!(observed.operation_id.as_str(), "op-7");
    assert_eq!(observed.record.sequence, 42);
    assert_eq!(
        observed.record.key.as_ref().map(|key| key.encoding),
        Some(ByteEncoding::Hex)
    );
    assert_eq!(
        observed.record.key.as_ref().map(|key| key.data.as_str()),
        Some("00ff")
    );
    assert!(observed.record.value.is_none());
    assert!(observed.record.headers[2].value.is_none());
    assert_eq!(
        observed.digest,
        observed.record.digest().unwrap_or_default()
    );
}

#[test]
fn duplicate_operation_identity_is_invalid_observer_evidence() {
    let result = normalize(
        0,
        CapturedRecord {
            topic: "records",
            partition: 0,
            offset: 0,
            key: None,
            value: Some(b"value"),
            headers: vec![
                ("testlab-operation-id", Some(b"op-1")),
                ("testlab-operation-id", Some(b"op-2")),
                ("testlab-sequence", Some(b"1")),
            ],
        },
    );

    assert!(result.is_err());
}
