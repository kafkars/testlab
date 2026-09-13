//! Tests for the streams group cli observation contract.

use testlab_schema::OperationId;

#[test]
fn exact_caller_order_is_retained() {
    let operation =
        OperationId::new("streams-lifecycle").unwrap_or_else(|error| panic!("operation: {error}"));
    let groups = vec!["secondary".to_owned(), "primary".to_owned()];
    let state = super::normalize(
        3,
        &operation,
        &groups,
        b"streams-group-absent:secondary\nstreams-group-absent:primary\n",
    )
    .unwrap_or_else(|error| panic!("normalize: {error}"));
    assert!(state.all_absent);
    assert_eq!(state.group_ids, groups);
}
