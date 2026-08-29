//! Classic-group batch normalization selects complete authoritative requested snapshots.

use testlab_schema::{BrokerStateObservation, OperationId};

use crate::observer_admin_classic_group::normalize_fixture;
use crate::observer_admin_target::ClassicGroupsTarget;

#[test]
fn exact_batch_retains_caller_order_and_consecutive_observations() {
    let observed = normalize_fixture(
        11,
        &target(),
        vec![
            ("group-a".to_owned(), 0, "Empty", "consumer"),
            ("group-b".to_owned(), 2, "Stable", "consumer"),
        ],
    )
    .unwrap_or_else(|error| panic!("normalize classic groups: {error}"));

    assert_eq!(facts(&observed), [(11, "group-b", 2), (12, "group-a", 0)]);
}

#[test]
fn missing_duplicate_and_mismatched_groups_are_invalid() {
    let target = target();
    assert!(
        normalize_fixture(
            0,
            &target,
            vec![("group-b".to_owned(), 2, "Stable", "consumer")],
        )
        .is_err()
    );
    assert!(
        normalize_fixture(
            0,
            &target,
            vec![
                ("group-b".to_owned(), 2, "Stable", "consumer"),
                ("group-b".to_owned(), 2, "Stable", "consumer"),
            ],
        )
        .is_err()
    );
    assert!(
        normalize_fixture(
            0,
            &target,
            vec![
                ("group-b".to_owned(), 2, "Stable", "consumer"),
                ("other".to_owned(), 0, "Empty", "consumer"),
            ],
        )
        .is_err()
    );
}

#[test]
fn unrelated_groups_do_not_contaminate_the_selected_snapshot() {
    let observed = normalize_fixture(
        11,
        &target(),
        vec![
            ("group-a".to_owned(), 0, "Empty", "consumer"),
            ("unrelated".to_owned(), 1, "Unknown", "connect"),
            ("group-b".to_owned(), 2, "Stable", "consumer"),
        ],
    )
    .unwrap_or_else(|error| panic!("normalize selected classic groups: {error}"));

    assert_eq!(facts(&observed), [(11, "group-b", 2), (12, "group-a", 0)]);
}

#[test]
fn hidden_error_sentinels_are_invalid_for_the_whole_batch() {
    for (state, protocol_type) in [("Unknown", "consumer"), ("", "consumer"), ("Stable", "")] {
        assert!(
            normalize_fixture(
                0,
                &target(),
                vec![
                    ("group-b".to_owned(), 2, state, protocol_type),
                    ("group-a".to_owned(), 0, "Empty", "consumer"),
                ],
            )
            .is_err(),
            "state {state:?}, protocol type {protocol_type:?}"
        );
    }
}

fn facts(observations: &[BrokerStateObservation]) -> Vec<(u64, &str, u32)> {
    observations
        .iter()
        .map(|observation| {
            let BrokerStateObservation::ConsumerGroup(observation) = observation else {
                panic!("consumer group state kind");
            };
            (
                observation.observation,
                observation.group_id.as_str(),
                observation
                    .member_count
                    .unwrap_or_else(|| panic!("present group member count")),
            )
        })
        .collect()
}

fn target() -> ClassicGroupsTarget {
    ClassicGroupsTarget {
        operation_id: OperationId::new("describe-groups")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        group_ids: vec!["group-b".to_owned(), "group-a".to_owned()],
    }
}
