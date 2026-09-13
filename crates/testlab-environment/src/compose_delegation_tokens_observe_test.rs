//! Delegation-token observation tests pin the secret-free CLI projection.

use super::SCRIPT;

#[test]
fn cli_projection_retains_only_the_token_count() {
    assert!(SCRIPT.contains("$TESTLAB_KAFKA_SASL_PASSWORD"));
    assert!(!SCRIPT.contains("kafkars-testlab-password"));
    assert!(SCRIPT.contains("print \"token-count:\" $6"));
    assert!(SCRIPT.contains("2>&1 |"));
}
