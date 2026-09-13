//! Tests for the compose streams group observe contract.

use super::SCRIPT;

#[test]
fn cli_projection_retains_only_selected_presence() {
    assert!(SCRIPT.contains("kafka-streams-groups.sh"));
    assert!(SCRIPT.contains("streams-group-present:"));
    assert!(SCRIPT.contains("streams-group-absent:"));
}
