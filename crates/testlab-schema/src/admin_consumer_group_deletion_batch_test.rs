//! Consumer-group batch deletion schemas pin order, bounds, and empty baselines.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupDeletionOutcome, AdminConsumerGroupsDeletion,
    ClientId, DeleteConsumerGroupsAction, DeleteConsumerGroupsCommand, EVIDENCE_SCHEMA_VERSION,
    OperationId, PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction,
};

#[test]
fn consumer_group_deletion_advances_all_versioned_boundaries() {
    assert_eq!(PROTOCOL_VERSION, 136);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 140);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 126);
}

#[test]
fn action_command_and_completion_round_trip_in_caller_order() {
    round_trip(&ScenarioAction::DeleteConsumerGroups(action()));
    round_trip(&AdapterCommand::DeleteConsumerGroups(command()));
    round_trip(&AdapterEvent::ConsumerGroupsDeleted(completion()));
    assert_eq!(command().group_ids, group_ids());
}

#[test]
fn checked_in_consumer_group_deletion_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate consumer-group deletion: {error}"));
}

#[test]
fn validation_rejects_nonplural_and_duplicate_groups() {
    for (label, group_ids, expected) in [
        ("singleton", vec!["group-a".to_owned()], "2 to 32 entries"),
        (
            "duplicate",
            vec!["group-a".to_owned(), "group-a".to_owned()],
            "unique valid group ids",
        ),
    ] {
        let mut action = action();
        action.operation_id = operation(label);
        action.group_ids = group_ids;
        let mut problems = Vec::new();
        crate::admin_action_validation::validate(
            &ScenarioAction::DeleteConsumerGroups(action),
            &BTreeMap::from([(client(), false)]),
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
fn transition_requires_matching_prior_empty_group_description() {
    let mut scenario = scenario();
    scenario
        .steps
        .retain(|step| !matches!(&step.action, ScenarioAction::DescribeClassicGroups(_)));
    let error = scenario
        .validate()
        .expect_err("batch deletion without prior empty-group description");
    assert!(
        error
            .to_string()
            .contains("requires a prior caller-ordered zero-member classic-group description"),
        "{error}"
    );
}

fn action() -> DeleteConsumerGroupsAction {
    DeleteConsumerGroupsAction {
        client_id: client(),
        operation_id: operation("delete-consumer-groups"),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn command() -> DeleteConsumerGroupsCommand {
    DeleteConsumerGroupsCommand {
        client_id: client(),
        operation_id: operation("delete-consumer-groups"),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminConsumerGroupsDeletion {
    AdminConsumerGroupsDeletion {
        operation_id: operation("delete-consumer-groups"),
        outcomes: group_ids()
            .into_iter()
            .map(|group_id| AdminConsumerGroupDeletionOutcome {
                group_id,
                error_code: None,
            })
            .collect(),
    }
}

fn group_ids() -> Vec<String> {
    vec!["group-z".to_owned(), "group-a".to_owned()]
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-consumer-groups.toml"
    ))
    .unwrap_or_else(|error| panic!("parse consumer-group deletion: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode consumer-group deletion: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode consumer-group deletion: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
