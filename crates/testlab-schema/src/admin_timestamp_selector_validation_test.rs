//! Timestamp selector validation keeps the optional wire value unambiguous.

use std::collections::{BTreeMap, BTreeSet};

use super::{AdminOffsetSelector, ListOffsetsAction, ScenarioAction};

#[test]
fn timestamp_selector_requires_one_nonnegative_timestamp() {
    let clients = BTreeMap::from([(super::client("client-1"), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();
    let cases = [
        ("missing", AdminOffsetSelector::Timestamp, None),
        ("negative", AdminOffsetSelector::Timestamp, Some(-1)),
        ("extraneous", AdminOffsetSelector::Latest, Some(1)),
    ];
    for (name, position, timestamp_millis) in cases {
        super::validate(
            &action(name, position, timestamp_millis),
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }
    super::assert_problem(&problems, "timestamp selector requires timestamp_millis");
    super::assert_problem(&problems, "timestamp_millis must be nonnegative");
    super::assert_problem(
        &problems,
        "timestamp_millis requires the timestamp selector",
    );

    let mut valid_problems = Vec::new();
    super::validate(
        &action(
            "valid",
            AdminOffsetSelector::Timestamp,
            Some(1_700_000_000_123),
        ),
        &clients,
        &mut operation_ids,
        &mut valid_problems,
    );
    assert!(valid_problems.is_empty(), "{valid_problems:?}");
}

fn action(
    name: &str,
    position: AdminOffsetSelector,
    timestamp_millis: Option<i64>,
) -> ScenarioAction {
    ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: super::client("client-1"),
        operation_id: super::operation(&format!("admin-timestamp-{name}")),
        topic: "records".to_owned(),
        partition: 0,
        position,
        timestamp_millis,
        expected_offset: Some(1),
        expected_error_code: None,
        timeout_ms: 1_000,
    })
}
