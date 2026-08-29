//! Consumer-group normalization tests preserve absence and reject mismatched identities.

use testlab_schema::{BrokerStateObservation, OperationId};

use crate::observer_admin_group::{normalize_fixture, normalize_fixture_with_state};
use crate::observer_admin_target::GroupTarget;

#[test]
fn exact_group_normalizes_only_name_and_member_count() {
    let observed = normalize_fixture(4, &target(), vec![("orders-group".to_owned(), 2)])
        .unwrap_or_else(|error| panic!("normalize group: {error}"));

    let BrokerStateObservation::ConsumerGroup(observed) = observed else {
        panic!("consumer group state kind");
    };
    assert_eq!(observed.observation, 4);
    assert_eq!(observed.operation_id.as_str(), "describe-group");
    assert_eq!(observed.group_id, "orders-group");
    assert!(observed.exists);
    assert_eq!(observed.member_count, Some(2));
}

#[test]
fn missing_group_is_valid_independent_absence() {
    let observed = normalize_fixture(0, &target(), Vec::new())
        .unwrap_or_else(|error| panic!("normalize absent group: {error}"));

    let BrokerStateObservation::ConsumerGroup(observed) = observed else {
        panic!("consumer group state kind");
    };
    assert!(!observed.exists);
    assert_eq!(observed.member_count, None);
}

#[test]
fn mismatched_or_duplicate_exact_group_is_invalid() {
    assert!(normalize_fixture(0, &target(), vec![("other-group".to_owned(), 1)]).is_err());
    assert!(
        normalize_fixture(
            0,
            &target(),
            vec![
                ("orders-group".to_owned(), 1),
                ("orders-group".to_owned(), 1),
            ],
        )
        .is_err()
    );
}

#[test]
fn stable_empty_and_dead_consumer_states_remain_authoritative() {
    for state in ["Stable", "Empty", "Dead"] {
        assert!(
            normalize_fixture_with_state(
                0,
                &target(),
                vec![("orders-group".to_owned(), 0, state, "consumer")],
            )
            .is_ok(),
            "state {state}"
        );
    }
}

#[test]
fn hidden_group_error_sentinels_are_rejected() {
    for (state, protocol_type) in [("", "consumer"), ("Unknown", "consumer"), ("Stable", "")] {
        assert!(
            normalize_fixture_with_state(
                0,
                &target(),
                vec![("orders-group".to_owned(), 0, state, protocol_type,)],
            )
            .is_err(),
            "state {state:?}, protocol {protocol_type:?}"
        );
    }
}

fn target() -> GroupTarget {
    GroupTarget {
        operation_id: OperationId::new("describe-group")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        group_id: "orders-group".to_owned(),
        expected_member_count: Some(2),
        expected_exists: true,
        poll_expected: false,
    }
}
