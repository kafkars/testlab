//! Share-group lifecycle observation keeps plural identity and read-only absence proof exact.

use testlab_schema::{
    AdapterCommand, BrokerStateObservation, ClientId, DeleteShareGroupsAction,
    DeleteShareGroupsCommand, OperationId, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, GroupIdsTarget};

#[test]
fn exact_plural_deletion_maps_to_one_ordered_presence_target() {
    let action = ScenarioAction::DeleteShareGroups(action());
    let command = AdapterCommand::DeleteShareGroups(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map Share-group deletion target: {error}"))
        .unwrap_or_else(|| panic!("Share-group deletion target"));
    assert_eq!(target, expected_target());
    assert_eq!(target.observation_count(), 2);
}

#[test]
fn altered_wire_order_is_rejected_before_observation() {
    let action = ScenarioAction::DeleteShareGroups(action());
    let mut command = command();
    command.group_ids.swap(0, 1);
    let error = AdminTarget::from_exact(&action, &AdapterCommand::DeleteShareGroups(command))
        .err()
        .unwrap_or_else(|| panic!("mismatched Share-group deletion order"));
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn list_snapshot_reports_each_selected_identity_in_caller_order() {
    let AdminTarget::ShareGroups(target) = expected_target() else {
        panic!("Share-groups target");
    };
    let observed = crate::share_group_cli_observation::normalize_list(
        7,
        &target,
        b"foreign-group\nshare-group-z\n",
    )
    .unwrap_or_else(|error| panic!("normalize Share-group list: {error}"));
    assert_eq!(observed.len(), 2);
    for (index, (observation, (group_id, exists))) in observed
        .iter()
        .zip([("share-group-z", true), ("share-group-a", false)])
        .enumerate()
    {
        let BrokerStateObservation::ShareGroup(actual) = observation else {
            panic!("Share-group presence observation");
        };
        assert_eq!(actual.observation, 7 + index as u64);
        assert_eq!(actual.operation_id, operation());
        assert_eq!(actual.group_id, group_id);
        assert_eq!(actual.exists, exists);
        assert_eq!(actual.state, None);
        assert_eq!(actual.member_count, None);
    }
}

#[test]
fn list_snapshot_rejects_ambiguous_or_duplicate_rows() {
    let AdminTarget::ShareGroups(target) = expected_target() else {
        panic!("Share-groups target");
    };
    for output in [
        b"GROUP TYPE\n".as_slice(),
        b"share-group-z\nshare-group-z\n".as_slice(),
    ] {
        let error = crate::share_group_cli_observation::normalize_list(7, &target, output)
            .err()
            .unwrap_or_else(|| panic!("ambiguous Share-group list must fail"));
        assert!(error.to_string().contains("list"));
    }
}

#[test]
fn cli_query_uses_only_the_read_only_list_mode() {
    let target = expected_target();
    let fixture = crate::compose_test_fixture::Fixture::new(false);
    let mut environment = fixture.environment();
    environment.program = "/bin/sh".into();
    environment.prefix = vec!["-c".to_owned(), "printf '%s\\n' foreign-group".to_owned()];

    let observed =
        environment.observe_share_group_with_cli(&target, std::time::Duration::from_secs(2));
    assert!(observed.phase.succeeded(), "{:?}", observed.phase.failure);
    assert_eq!(observed.state_observations.len(), 2);
    let args = &observed.phase.operations[0].args;
    assert!(args.iter().any(|argument| argument == "--list"));
    assert!(!args.iter().any(|argument| argument == "--delete"));
}

fn action() -> DeleteShareGroupsAction {
    DeleteShareGroupsAction {
        client_id: client(),
        operation_id: operation(),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn command() -> DeleteShareGroupsCommand {
    DeleteShareGroupsCommand {
        client_id: client(),
        operation_id: operation(),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn expected_target() -> AdminTarget {
    AdminTarget::ShareGroups(GroupIdsTarget {
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
    OperationId::new("delete-share-groups").unwrap_or_else(|error| panic!("operation: {error}"))
}
