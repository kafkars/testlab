//! Share-group observer tests pin exact command correlation and strict CLI parsing.

use testlab_schema::{
    AdapterCommand, BrokerStateObservation, ClientId, DescribeShareGroupAction,
    DescribeShareGroupCommand, ListShareGroupOffsetsAction, ListShareGroupOffsetsCommand,
    OperationId, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, ShareGroupOffsetTarget, ShareGroupTarget};

#[test]
fn action_and_wire_command_map_to_one_exact_share_group_target() {
    let action = ScenarioAction::DescribeShareGroup(action());
    let command = AdapterCommand::DescribeShareGroup(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("Share-group target: {error}"))
        .unwrap_or_else(|| panic!("missing Share-group target"));
    assert_eq!(
        target,
        AdminTarget::ShareGroup(ShareGroupTarget {
            operation_id: operation(),
            group_id: "share-group-1".to_owned(),
        })
    );

    let AdapterCommand::DescribeShareGroup(mut changed) = command else {
        panic!("Share-group command kind");
    };
    changed.group_id = "other-group".to_owned();
    assert!(
        AdminTarget::from_exact(&action, &AdapterCommand::DescribeShareGroup(changed)).is_err()
    );
}

#[test]
fn cli_normalization_retains_exact_stable_state_and_membership() {
    let observation = normalize(
        "GROUP COORDINATOR (ID) STATE #MEMBERS\nshare-group-1 localhost:9092 (1) Stable 1\n",
    );
    let BrokerStateObservation::ShareGroup(value) = observation else {
        panic!("Share-group observation kind");
    };
    assert_eq!(value.observation, 7);
    assert_eq!(value.operation_id, operation());
    assert_eq!(value.group_id, "share-group-1");
    assert!(value.exists);
    assert_eq!(value.state.as_deref(), Some("Stable"));
    assert_eq!(value.member_count, Some(1));
}

#[test]
fn cli_normalization_rejects_wrong_ambiguous_or_impossible_output() {
    for output in [
        "",
        "GROUP COORDINATOR STATE #MEMBERS\nshare-group-1 localhost:9092 Stable 1\n",
        "GROUP COORDINATOR (ID) STATE #MEMBERS\nother-group localhost:9092 (1) Stable 1\n",
        "GROUP COORDINATOR (ID) STATE #MEMBERS\nshare-group-1 localhost:9092 (1) Unknown 1\n",
        "GROUP COORDINATOR (ID) STATE #MEMBERS\nshare-group-1 localhost:9092 (1) Empty 1\n",
        "GROUP COORDINATOR (ID) STATE #MEMBERS\nshare-group-1 localhost:9092 (1) Stable 1\nshare-group-1 localhost:9093 (2) Stable 1\n",
    ] {
        let target = target();
        assert!(
            crate::share_group_cli_observation::normalize(7, &target, output.as_bytes()).is_err(),
            "{output:?}"
        );
    }
}

#[test]
fn offset_action_command_and_cli_row_preserve_exact_partition_state() {
    let action = ScenarioAction::ListShareGroupOffsets(offset_action());
    let command = AdapterCommand::ListShareGroupOffsets(offset_command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("Share-group offset target: {error}"))
        .unwrap_or_else(|| panic!("missing Share-group offset target"));
    assert_eq!(target, offset_target());

    let output = "GROUP TOPIC PARTITION START-OFFSET LAG\nshare-group-1 share-topic 0 1 1\n";
    let observation = crate::share_group_cli_observation::normalize(8, &target, output.as_bytes())
        .unwrap_or_else(|error| panic!("Share-group offset observation: {error}"));
    let BrokerStateObservation::ShareGroupOffset(value) = observation else {
        panic!("Share-group offset observation kind");
    };
    assert_eq!(value.observation, 8);
    assert_eq!(value.operation_id, offset_operation());
    assert_eq!(value.group_id, "share-group-1");
    assert_eq!(value.topic, "share-topic");
    assert_eq!(value.partition, 0);
    assert_eq!(value.start_offset, Some(1));
    assert_eq!(value.lag, Some(1));
}

#[test]
fn offset_cli_normalization_rejects_wrong_or_ambiguous_output() {
    for output in [
        "",
        "GROUP TOPIC PARTITION OFFSET LAG\nshare-group-1 share-topic 0 1 1\n",
        "GROUP TOPIC PARTITION START-OFFSET LAG\nshare-group-1 other-topic 0 1 1\n",
        "GROUP TOPIC PARTITION START-OFFSET LAG\nshare-group-1 share-topic 0 -1 1\n",
        "GROUP TOPIC PARTITION START-OFFSET LAG\nshare-group-1 share-topic 0 1 1\nshare-group-1 share-topic 1 1 1\n",
    ] {
        assert!(
            crate::share_group_cli_observation::normalize(8, &offset_target(), output.as_bytes())
                .is_err(),
            "{output:?}"
        );
    }
}

#[test]
fn cli_query_terminal_remains_exact_and_raw() {
    let target = target();
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec![
        "-c".to_owned(),
        "printf '%s\\n' 'GROUP COORDINATOR (ID) STATE #MEMBERS' 'share-group-1 localhost:9092 (1) Stable 1'"
            .to_owned(),
    ];

    let observed =
        environment.observe_share_group_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.phase.operations.len(), 1);
    assert_eq!(observed.phase.artifacts.len(), 2);
    assert_eq!(observed.state_observations.len(), 1);

    let duplicate =
        environment.observe_share_group_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(!duplicate.phase.succeeded());
    assert!(duplicate.phase.operations.is_empty());
}

#[test]
fn offset_cli_query_uses_pinned_offsets_mode_and_retains_raw_output() {
    let target = offset_target();
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec![
        "-c".to_owned(),
        "printf '%s\\n' 'GROUP TOPIC PARTITION START-OFFSET LAG' 'share-group-1 share-topic 0 1 1'"
            .to_owned(),
    ];

    let observed =
        environment.observe_share_group_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.phase.operations.len(), 1);
    assert!(
        observed.phase.operations[0]
            .args
            .iter()
            .any(|argument| argument == "--offsets")
    );
    assert_eq!(observed.phase.artifacts.len(), 2);
    assert_eq!(observed.state_observations.len(), 1);
}

fn normalize(output: &str) -> BrokerStateObservation {
    crate::share_group_cli_observation::normalize(7, &target(), output.as_bytes())
        .unwrap_or_else(|error| panic!("Share-group observation: {error}"))
}

fn target() -> AdminTarget {
    AdminTarget::ShareGroup(ShareGroupTarget {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
    })
}

fn offset_target() -> AdminTarget {
    AdminTarget::ShareGroupOffset(ShareGroupOffsetTarget {
        operation_id: offset_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
    })
}

fn action() -> DescribeShareGroupAction {
    DescribeShareGroupAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_topic: "share-topic".to_owned(),
        expected_partition: 0,
        timeout_ms: 1_000,
    }
}

fn command() -> DescribeShareGroupCommand {
    DescribeShareGroupCommand {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        timeout_ms: 1_000,
    }
}

fn offset_action() -> ListShareGroupOffsetsAction {
    ListShareGroupOffsetsAction {
        client_id: client(),
        operation_id: offset_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        expected_start_offset: 1,
        expected_lag: 1,
        timeout_ms: 1_000,
    }
}

fn offset_command() -> ListShareGroupOffsetsCommand {
    ListShareGroupOffsetsCommand {
        client_id: client(),
        operation_id: offset_operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-share-group").unwrap_or_else(|error| panic!("operation: {error}"))
}

fn offset_operation() -> OperationId {
    OperationId::new("list-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
