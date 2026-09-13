//! Plural Share offset observation pins nested caller order and separate read-only queries.

use testlab_schema::{
    AdapterCommand, BrokerStateObservation, ClientId, ListShareGroupsOffsetsAction,
    ListShareGroupsOffsetsCommand, OperationId, ScenarioAction, ShareGroupOffsetExpectation,
    ShareGroupOffsetSelection, ShareGroupOffsetsExpectation, ShareGroupOffsetsSelection,
};

use crate::observer_admin_target::{
    AdminTarget, ShareGroupOffsetSelectionTarget, ShareGroupOffsetsSelectionTarget,
    ShareGroupsOffsetsTarget,
};

#[test]
fn exact_action_and_command_map_to_one_nested_caller_ordered_target() {
    let action = ScenarioAction::ListShareGroupsOffsets(action());
    let command = AdapterCommand::ListShareGroupsOffsets(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map plural Share offsets: {error}"))
        .unwrap_or_else(|| panic!("plural Share offsets target"));
    assert_eq!(target, expected_target());
    assert_eq!(target.observation_count(), 2);
}

#[test]
fn altered_wire_order_is_rejected_before_observation() {
    let action = ScenarioAction::ListShareGroupsOffsets(action());
    let mut command = command();
    command.groups.swap(0, 1);
    let error = AdminTarget::from_exact(&action, &AdapterCommand::ListShareGroupsOffsets(command))
        .err()
        .unwrap_or_else(|| panic!("mismatched plural Share offset order"));
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn cli_queries_are_separate_read_only_and_flattened_in_caller_order() {
    let target = expected_target();
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec![
        "-c".to_owned(),
        "for argument in \"$@\"; do group=$argument; done; case \"$group\" in share-z) topic=topic-z; start=3; lag=5;; share-a) topic=topic-a; start=2; lag=7;; esac; printf '%s\n' 'GROUP TOPIC PARTITION START-OFFSET LAG' \"$group $topic 0 $start $lag\""
            .to_owned(),
    ];

    let observed = environment
        .observe_share_groups_offsets_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.phase.operations.len(), 2);
    assert_eq!(observed.phase.artifacts.len(), 4);
    assert_eq!(observed.state_observations.len(), 2);
    for (index, (operation, expected_group)) in observed
        .phase
        .operations
        .iter()
        .zip(["share-z", "share-a"])
        .enumerate()
    {
        assert!(
            operation
                .args
                .iter()
                .any(|argument| argument == "--describe")
        );
        assert!(
            operation
                .args
                .iter()
                .any(|argument| argument == "--offsets")
        );
        assert!(!operation.args.iter().any(|argument| argument == "--delete"));
        assert_eq!(
            operation.args.last().map(String::as_str),
            Some(expected_group)
        );
        let BrokerStateObservation::ShareGroupOffset(state) = &observed.state_observations[index]
        else {
            panic!("Share-group offset observation");
        };
        assert_eq!(state.observation, index as u64);
        assert_eq!(state.group_id, expected_group);
    }
    let BrokerStateObservation::ShareGroupOffset(zulu) = &observed.state_observations[0] else {
        panic!("zulu Share-group offset");
    };
    assert_eq!(zulu.topic, "topic-z");
    assert_eq!(zulu.start_offset, Some(3));
    assert_eq!(zulu.lag, Some(5));
    let BrokerStateObservation::ShareGroupOffset(alpha) = &observed.state_observations[1] else {
        panic!("alpha Share-group offset");
    };
    assert_eq!(alpha.topic, "topic-a");
    assert_eq!(alpha.start_offset, Some(2));
    assert_eq!(alpha.lag, Some(7));

    let duplicate = environment
        .observe_share_groups_offsets_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(!duplicate.phase.succeeded());
    assert!(duplicate.phase.operations.is_empty());
}

fn action() -> ListShareGroupsOffsetsAction {
    ListShareGroupsOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        groups: vec![
            expectation("share-z", "topic-z", 3, 5),
            expectation("share-a", "topic-a", 2, 7),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> ListShareGroupsOffsetsCommand {
    ListShareGroupsOffsetsCommand {
        client_id: client(),
        operation_id: operation(),
        groups: vec![
            selection("share-z", "topic-z"),
            selection("share-a", "topic-a"),
        ],
        timeout_ms: 1_000,
    }
}

fn expected_target() -> AdminTarget {
    AdminTarget::ShareGroupsOffsets(ShareGroupsOffsetsTarget {
        operation_id: operation(),
        groups: vec![target("share-z", "topic-z"), target("share-a", "topic-a")],
    })
}

fn expectation(
    group_id: &str,
    topic: &str,
    expected_start_offset: i64,
    expected_lag: i64,
) -> ShareGroupOffsetsExpectation {
    ShareGroupOffsetsExpectation {
        group_id: group_id.to_owned(),
        partitions: vec![ShareGroupOffsetExpectation {
            topic: topic.to_owned(),
            partition: 0,
            expected_start_offset,
            expected_lag,
        }],
    }
}

fn selection(group_id: &str, topic: &str) -> ShareGroupOffsetsSelection {
    ShareGroupOffsetsSelection {
        group_id: group_id.to_owned(),
        partitions: vec![ShareGroupOffsetSelection {
            topic: topic.to_owned(),
            partition: 0,
        }],
    }
}

fn target(group_id: &str, topic: &str) -> ShareGroupOffsetsSelectionTarget {
    ShareGroupOffsetsSelectionTarget {
        group_id: group_id.to_owned(),
        offsets: vec![ShareGroupOffsetSelectionTarget {
            topic: topic.to_owned(),
            partition: 0,
        }],
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("list-share-groups-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
