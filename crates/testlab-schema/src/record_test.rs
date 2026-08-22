//! Canonical record digest evidence.

use super::{ByteString, HeaderSpec, RecordSpec};

fn record() -> RecordSpec {
    RecordSpec {
        topic: "records".to_owned(),
        partition: 2,
        sequence: 9,
        key: None,
        value: Some(ByteString::utf8("value")),
        headers: vec![HeaderSpec {
            name: "trace".to_owned(),
            value: Some(ByteString::utf8("one")),
        }],
    }
}

#[test]
fn equal_records_have_equal_digests() {
    let first = record().digest();
    let second = record().digest();

    assert_eq!(first.ok(), second.ok());
}

#[test]
fn null_and_empty_keys_have_different_digests() {
    let null = record();
    let mut empty = record();
    empty.key = Some(ByteString::utf8(""));

    assert_ne!(null.digest().ok(), empty.digest().ok());
}

#[test]
fn header_order_changes_the_digest() {
    let mut first = record();
    first.headers.push(HeaderSpec {
        name: "second".to_owned(),
        value: None,
    });
    let mut second = first.clone();
    second.headers.reverse();

    assert_ne!(first.digest().ok(), second.digest().ok());
}
