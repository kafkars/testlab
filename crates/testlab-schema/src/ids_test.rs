//! Identifier validation evidence.

use super::OperationId;

#[test]
fn portable_identity_is_accepted() {
    let identity = OperationId::new("producer-1:send_2");

    assert_eq!(
        identity.ok().map(|value| value.to_string()),
        Some("producer-1:send_2".to_owned())
    );
}

#[test]
fn path_separators_are_rejected() {
    assert!(OperationId::new("../operation").is_err());
    assert!(OperationId::new("operation/one").is_err());
}
