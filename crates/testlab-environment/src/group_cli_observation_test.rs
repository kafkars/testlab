//! Group CLI observations reject partial output and retain caller-ordered broker facts.

use testlab_schema::{BrokerStateObservation, OperationId};

use crate::group_cli_observation::{normalize, selection, supports};
use crate::observer_admin_target::{AdminTarget, ClassicGroupsTarget, GroupTarget, ListTarget};

const HEADER: &str = "GROUP COORDINATOR (ID) ASSIGNMENT-STRATEGY STATE #MEMBERS\n";

#[test]
fn modern_members_and_empty_groups_are_exact_not_legacy_zeroes() {
    for (state, count) in [("Stable", 2), ("Reconciling", 1), ("Empty", 0)] {
        let output = format!("{HEADER}workers broker-2:19092 (2) uniform {state} {count}\n");
        let states = normalize(7, &group(), output.as_bytes())
            .unwrap_or_else(|error| panic!("snapshot: {error}"));
        let BrokerStateObservation::ConsumerGroup(observed) = &states[0] else {
            panic!("group state");
        };
        assert_eq!(observed.group_id, "workers");
        assert_eq!(observed.member_count, Some(count));
        assert!(observed.exists);
        assert_eq!(observed.observation, 7);
        assert_eq!(observed.operation_id, operation());
    }
}

#[test]
fn batch_preserves_caller_order_and_rejects_missing_or_extra_groups() {
    let target = AdminTarget::ClassicGroups(ClassicGroupsTarget {
        operation_id: operation(),
        group_ids: vec!["zulu".to_owned(), "alpha".to_owned()],
    });
    let output = format!(
        "{HEADER}alpha broker-1:19092 (1) range Stable 2\n{HEADER}zulu broker-3:19092 (3) range Stable 1\n"
    );
    let states = normalize(3, &target, output.as_bytes())
        .unwrap_or_else(|error| panic!("snapshot: {error}"));
    let names = states
        .iter()
        .map(|state| {
            let BrokerStateObservation::ConsumerGroup(state) = state else {
                panic!("group state");
            };
            (state.group_id.as_str(), state.observation)
        })
        .collect::<Vec<_>>();
    assert_eq!(names, [("zulu", 3), ("alpha", 4)]);
    assert!(normalize(3, &group(), output.as_bytes()).is_err());
    assert!(normalize(3, &target, b"").is_err());
}

#[test]
fn all_group_snapshot_can_report_absence_but_not_invalid_output() {
    let target = AdminTarget::ConsumerGroups(ListTarget {
        operation_id: operation(),
        names: vec!["workers".to_owned()],
    });
    let states = normalize(0, &target, b"").unwrap_or_else(|error| panic!("snapshot: {error}"));
    let BrokerStateObservation::ConsumerGroup(state) = &states[0] else {
        panic!("group state");
    };
    assert!(!state.exists);
    assert_eq!(state.member_count, None);
    assert!(normalize(0, &target, b"Error: broker request failed").is_err());
    assert_eq!(selection(&target), ["--all-groups"]);
}

#[test]
fn partial_duplicate_malformed_and_failed_descriptions_are_invalid() {
    let valid = format!("{HEADER}workers broker-1:19092 (1) uniform Stable 2\n");
    for output in [
        String::new(),
        HEADER.to_owned(),
        "workers broker-1:19092 (1) uniform Stable 2".to_owned(),
        format!("{valid}{valid}"),
        valid.replace("Stable 2", "Unknown 2"),
        valid.replace("Stable 2", "Empty 2"),
        valid.replace("Stable 2", "Stable -1"),
        valid.replace("(1)", "(?)"),
        format!("{valid}Error: request failed\n"),
    ] {
        assert!(
            normalize(0, &group(), output.as_bytes()).is_err(),
            "{output:?}"
        );
    }
    assert!(normalize(0, &group(), &[255]).is_err());
    assert_eq!(selection(&group()), ["--group", "workers"]);
}

#[test]
fn polling_delete_targets_keep_the_existing_absence_observer() {
    let AdminTarget::ConsumerGroup(mut target) = group() else {
        panic!("group target");
    };
    target.poll_expected = true;
    assert!(!supports(&AdminTarget::ConsumerGroup(target)));
    assert!(supports(&group()));
}

#[test]
fn cli_query_retains_raw_output_and_checks_duplicate_correlation_before_effects() {
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec![
        "-c".to_owned(),
        format!("printf '%s\\n' '{HEADER}workers broker-1:19092 (1) uniform Stable 2'"),
    ];
    let observed = environment.observe_groups_with_cli(&group(), std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.phase.operations.len(), 1);
    assert_eq!(observed.phase.artifacts.len(), 2);
    assert_eq!(observed.state_observations.len(), 1);
    let duplicate =
        environment.observe_groups_with_cli(&group(), std::time::Duration::from_secs(2));
    assert!(!duplicate.phase.succeeded());
    assert!(duplicate.phase.operations.is_empty());
}

fn operation() -> OperationId {
    OperationId::new("describe-workers").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn group() -> AdminTarget {
    AdminTarget::ConsumerGroup(GroupTarget {
        operation_id: operation(),
        group_id: "workers".to_owned(),
        expected_member_count: Some(2),
        expected_exists: true,
        poll_expected: false,
    })
}
