//! Tests for the delegation token cli observation contract.

use super::*;

#[test]
fn accepts_one_secret_free_count_and_rejects_all_other_shapes() {
    let operation = OperationId::new("delegation-token-lifecycle")
        .unwrap_or_else(|error| panic!("operation id: {error}"));
    let owner = DelegationTokenPrincipalSpec {
        principal_type: "User".to_owned(),
        principal_name: "kafkars".to_owned(),
    };
    let state = normalize(4, &operation, &owner, b"token-count:0\n")
        .unwrap_or_else(|error| panic!("token state: {error}"));
    assert_eq!(state.token_count, 0);
    assert_eq!(state.owner, owner);
    assert!(normalize(4, &operation, &owner, b"TOKENID HMAC secret\n").is_err());
    assert!(normalize(4, &operation, &owner, b"token-count:1\ntoken-count:0\n").is_err());
}
