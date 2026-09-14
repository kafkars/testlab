//! Delegation-token observation tests pin the secret-free live-token projection.

use super::SCRIPT;

#[test]
fn cli_projection_retains_only_the_token_count() {
    assert!(SCRIPT.contains("$TESTLAB_KAFKA_SASL_PASSWORD"));
    assert!(!SCRIPT.contains("kafkars-testlab-password"));
    assert!(SCRIPT.contains("NF >= 3 && minute($(NF - 2))"));
    assert!(SCRIPT.contains("$(NF - 1) > current_minute"));
    assert!(SCRIPT.contains("print \"token-count:\" (live + 0)"));
    assert!(SCRIPT.contains("2>&1 |"));
}
