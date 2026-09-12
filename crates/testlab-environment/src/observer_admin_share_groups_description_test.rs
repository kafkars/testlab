//! Plural Share-group description observation pins caller order and separate read-only queries.

use testlab_schema::{
    AdapterCommand, BrokerStateObservation, ClientId, DescribeShareGroupsAction,
    DescribeShareGroupsCommand, OperationId, ScenarioAction, ShareGroupDescriptionExpectation,
};

use crate::observer_admin_target::{AdminTarget, GroupIdsTarget};

#[test]
fn exact_action_and_command_map_to_one_caller_ordered_target() {
    let action = ScenarioAction::DescribeShareGroups(action());
    let command = AdapterCommand::DescribeShareGroups(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map plural Share descriptions: {error}"))
        .unwrap_or_else(|| panic!("plural Share description target"));
    assert_eq!(target, expected_target());
    assert_eq!(target.observation_count(), 2);
}

#[test]
fn altered_wire_order_is_rejected_before_observation() {
    let action = ScenarioAction::DescribeShareGroups(action());
    let mut command = command();
    command.group_ids.swap(0, 1);
    let error = AdminTarget::from_exact(&action, &AdapterCommand::DescribeShareGroups(command))
        .expect_err("mismatched plural Share description order");
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn cli_queries_are_separate_read_only_and_caller_ordered() {
    let target = expected_target();
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec![
        "-c".to_owned(),
        "for argument in \"$@\"; do group=$argument; done; printf '%s\\n' 'GROUP COORDINATOR (ID) STATE #MEMBERS' \"$group localhost:9092 (1) Stable 1\""
            .to_owned(),
    ];

    let observed = environment
        .observe_share_group_descriptions_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.phase.operations.len(), 2);
    assert_eq!(observed.phase.artifacts.len(), 4);
    assert_eq!(observed.state_observations.len(), 2);
    for (index, (operation, expected_group)) in observed
        .phase
        .operations
        .iter()
        .zip(group_ids())
        .enumerate()
    {
        assert!(
            operation
                .args
                .iter()
                .any(|argument| argument == "--describe")
        );
        assert!(operation.args.iter().any(|argument| argument == "--state"));
        assert!(!operation.args.iter().any(|argument| argument == "--delete"));
        assert_eq!(operation.args.last(), Some(&expected_group));
        let BrokerStateObservation::ShareGroup(state) = &observed.state_observations[index] else {
            panic!("Share-group state observation");
        };
        assert_eq!(state.observation, index as u64);
        assert_eq!(state.group_id, expected_group);
        assert_eq!(state.state.as_deref(), Some("Stable"));
        assert_eq!(state.member_count, Some(1));
    }

    let duplicate = environment
        .observe_share_group_descriptions_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(!duplicate.phase.succeeded());
    assert!(duplicate.phase.operations.is_empty());
}

fn action() -> DescribeShareGroupsAction {
    DescribeShareGroupsAction {
        client_id: client(),
        operation_id: operation(),
        groups: [("share-group-z", "topic-z"), ("share-group-a", "topic-a")]
            .into_iter()
            .map(|(group_id, topic)| ShareGroupDescriptionExpectation {
                group_id: group_id.to_owned(),
                expected_state: "Stable".to_owned(),
                expected_member_count: 1,
                expected_rack_id: None,
                expected_topic: topic.to_owned(),
                expected_partition: 0,
            })
            .collect(),
        include_authorized_operations: true,
        timeout_ms: 1_000,
    }
}

fn command() -> DescribeShareGroupsCommand {
    DescribeShareGroupsCommand {
        client_id: client(),
        operation_id: operation(),
        group_ids: group_ids(),
        include_authorized_operations: true,
        timeout_ms: 1_000,
    }
}

fn expected_target() -> AdminTarget {
    AdminTarget::ShareGroupDescriptions(GroupIdsTarget {
        operation_id: operation(),
        group_ids: group_ids(),
    })
}

fn group_ids() -> Vec<String> {
    vec!["share-group-z".to_owned(), "share-group-a".to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("describe-share-groups").unwrap_or_else(|error| panic!("operation: {error}"))
}
