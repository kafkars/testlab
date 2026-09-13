//! Consumer-group deletion observation pins exact targets and ordered absence.

use testlab_schema::{
    AdapterCommand, BrokerStateObservation, ClientId, DeleteConsumerGroupsAction,
    DeleteConsumerGroupsCommand, OperationId, ScenarioAction,
};

use crate::observer_admin_consumer_group_deletion_batch::normalize_fixture;
use crate::observer_admin_target::{AdminTarget, ListTarget};

#[test]
fn exact_action_and_wire_command_map_to_ordered_absence() {
    let action = ScenarioAction::DeleteConsumerGroups(action());
    let command = AdapterCommand::DeleteConsumerGroups(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map consumer-group deletion: {error}"))
        .unwrap_or_else(|| panic!("consumer-group deletion target"));
    assert_eq!(
        target,
        AdminTarget::ConsumerGroupDeletions(ListTarget {
            operation_id: operation(),
            names: group_ids(),
        })
    );
    assert_eq!(target.observation_count(), 2);
}

#[test]
fn altered_wire_order_is_rejected_before_observation() {
    let action = ScenarioAction::DeleteConsumerGroups(action());
    let mut command = command();
    command.group_ids.swap(0, 1);
    let error = AdminTarget::from_exact(&action, &AdapterCommand::DeleteConsumerGroups(command))
        .err()
        .unwrap_or_else(|| panic!("mismatched consumer-group deletion order"));
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn independent_snapshot_retains_order_and_absence() {
    let target = ListTarget {
        operation_id: operation(),
        names: group_ids(),
    };
    let observed = normalize_fixture(
        9,
        &target,
        vec![("unrelated".to_owned(), 1, "Unknown", "unrelated-protocol")],
    )
    .unwrap_or_else(|error| panic!("normalize consumer-group absence: {error}"));
    let facts = observed
        .iter()
        .map(|observation| {
            let BrokerStateObservation::ConsumerGroup(group) = observation else {
                panic!("consumer-group observation");
            };
            (group.observation, group.group_id.as_str(), group.exists)
        })
        .collect::<Vec<_>>();
    assert_eq!(facts, [(9, "group-z", false), (10, "group-a", false)]);
}

fn action() -> DeleteConsumerGroupsAction {
    DeleteConsumerGroupsAction {
        client_id: client(),
        operation_id: operation(),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn command() -> DeleteConsumerGroupsCommand {
    DeleteConsumerGroupsCommand {
        client_id: client(),
        operation_id: operation(),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn group_ids() -> Vec<String> {
    vec!["group-z".to_owned(), "group-a".to_owned()]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("delete-consumer-groups").unwrap_or_else(|error| panic!("operation: {error}"))
}
