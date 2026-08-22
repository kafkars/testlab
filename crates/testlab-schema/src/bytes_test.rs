//! Portable byte decoding evidence.

use super::{ByteEncoding, ByteString};

#[test]
fn hexadecimal_data_decodes_exactly() {
    let value = ByteString {
        encoding: ByteEncoding::Hex,
        data: "00ff10".to_owned(),
    };

    assert_eq!(value.decode().ok(), Some(vec![0, 255, 16]));
}

#[test]
fn empty_bytes_remain_distinct_from_record_null() {
    let value = ByteString::utf8("");

    assert_eq!(value.decode().ok(), Some(Vec::new()));
}
