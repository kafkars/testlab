//! Share-group lifecycle schema tests pin plural order, bounds, and empty-group preconditions.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminShareGroupDeletionOutcome, AdminShareGroupsDeletion,
    ClientId, DeleteShareGroupsAction, DeleteShareGroupsCommand, OperationId, Scenario,
    ScenarioAction,
};

#[test]
fn plural_action_command_and_completion_round_trip_in_caller_order() {
    let action = action();
    round_trip(&ScenarioAction::DeleteShareGroups(action.clone()));
    round_trip(&AdapterCommand::DeleteShareGroups(command()));
    round_trip(&AdapterEvent::ShareGroupsDeleted(completion()));
    assert_eq!(
        action.group_ids,
        ["share-group-z".to_owned(), "share-group-a".to_owned()]
    );
}

#[test]
fn checked_in_share_group_deletion_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate Share-group deletion: {error}"));
}

#[test]
fn validation_rejects_singleton_duplicate_and_invalid_group_sets() {
    let clients = BTreeMap::from([(client(), false)]);
    for (label, group_ids, expected) in [
        (
            "singleton",
            vec!["share-group-a".to_owned()],
            "between 2 and 32 groups",
        ),
        (
            "duplicate",
            vec!["share-group-a".to_owned(), "share-group-a".to_owned()],
            "duplicate group_id",
        ),
        (
            "invalid",
            vec![String::new(), "share-group-a".to_owned()],
            "invalid group_id",
        ),
    ] {
        let mut invalid = action();
        invalid.operation_id = operation(label);
        invalid.group_ids = group_ids;
        let mut problems = Vec::new();
        crate::admin_action_validation::validate(
            &ScenarioAction::DeleteShareGroups(invalid),
            &clients,
            &mut BTreeSet::new(),
            &mut problems,
        );
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "{label}: {problems:?}"
        );
    }
}

#[test]
fn transition_requires_every_modeled_share_group_member_to_be_closed() {
    let mut scenario = scenario();
    scenario.steps.retain(|step| {
        !matches!(
            &step.action,
            ScenarioAction::CloseShareConsumer { consumer_id, .. }
                if consumer_id.as_str() == "share-a"
        )
    });
    let mut problems = Vec::new();
    crate::admin_share_group_offset_transition_validation::validate(&scenario, &mut problems);
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("every modeled Share-group member to be closed")),
        "{problems:?}"
    );
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-share-groups.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Share-group deletion: {error}"))
}

fn action() -> DeleteShareGroupsAction {
    DeleteShareGroupsAction {
        client_id: client(),
        operation_id: operation("delete-share-groups"),
        group_ids: vec!["share-group-z".to_owned(), "share-group-a".to_owned()],
        timeout_ms: 1_000,
    }
}

fn command() -> DeleteShareGroupsCommand {
    DeleteShareGroupsCommand {
        client_id: client(),
        operation_id: operation("delete-share-groups"),
        group_ids: vec!["share-group-z".to_owned(), "share-group-a".to_owned()],
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminShareGroupsDeletion {
    AdminShareGroupsDeletion {
        operation_id: operation("delete-share-groups"),
        outcomes: vec![
            AdminShareGroupDeletionOutcome {
                group_id: "share-group-z".to_owned(),
                error_code: None,
            },
            AdminShareGroupDeletionOutcome {
                group_id: "share-group-a".to_owned(),
                error_code: None,
            },
        ],
    }
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode Share-group deletion: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode Share-group deletion: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
