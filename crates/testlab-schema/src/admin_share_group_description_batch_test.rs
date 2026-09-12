//! Plural Share-group description schemas pin caller order and modeled active members.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminShareGroupDescription, AdminShareGroupDescriptionOutcome,
    AdminShareGroupsDescription, ClientId, DescribeShareGroupsAction, DescribeShareGroupsCommand,
    OperationId, Scenario, ScenarioAction, ShareGroupDescriptionExpectation,
};

#[test]
fn action_command_and_public_completion_round_trip_in_caller_order() {
    round_trip(&ScenarioAction::DescribeShareGroups(action()));
    round_trip(&AdapterCommand::DescribeShareGroups(command()));
    round_trip(&AdapterEvent::ShareGroupsDescribed(completion()));
    assert_eq!(command().group_ids, group_ids());
}

#[test]
fn checked_in_plural_description_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate plural Share description: {error}"));
}

#[test]
fn validation_rejects_nonplural_duplicate_and_invalid_expectations() {
    let clients = BTreeMap::from([(client(), false)]);
    for (label, groups, expected) in [
        (
            "singleton",
            vec![expectation("share-a", "topic-a")],
            "between 2 and 32 entries",
        ),
        (
            "duplicate",
            vec![
                expectation("share-a", "topic-a"),
                expectation("share-a", "topic-b"),
            ],
            "unique valid group ids",
        ),
        (
            "invalid",
            vec![
                expectation("share-a", "topic-a"),
                ShareGroupDescriptionExpectation {
                    group_id: "share-b".to_owned(),
                    expected_state: "Unknown".to_owned(),
                    expected_member_count: 0,
                    expected_rack_id: None,
                    expected_topic: String::new(),
                    expected_partition: -1,
                },
            ],
            "expected_state must be Stable",
        ),
    ] {
        let mut action = action();
        action.operation_id = operation(label);
        action.groups = groups;
        let mut problems = Vec::new();
        crate::admin_action_validation::validate(
            &ScenarioAction::DescribeShareGroups(action),
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
fn transition_requires_each_expected_member_to_retain_a_matching_topic_batch() {
    let mut scenario = scenario();
    scenario.steps.retain(|step| {
        !matches!(
            &step.action,
            ScenarioAction::ShareReceive { consumer_id, .. }
                if consumer_id.as_str() == "share-alpha"
        )
    });
    let mut problems = Vec::new();
    crate::admin_share_group_description_transition_validation::validate(&scenario, &mut problems);
    assert!(
        problems.iter().any(|problem| {
            problem.contains("testlab-admin-share-description-alpha")
                && problem.contains("retaining a batch")
        }),
        "{problems:?}"
    );
}

fn action() -> DescribeShareGroupsAction {
    DescribeShareGroupsAction {
        client_id: client(),
        operation_id: operation("describe-share-groups"),
        groups: vec![
            expectation("share-z", "topic-z"),
            expectation("share-a", "topic-a"),
        ],
        timeout_ms: 1_000,
    }
}

fn command() -> DescribeShareGroupsCommand {
    DescribeShareGroupsCommand {
        client_id: client(),
        operation_id: operation("describe-share-groups"),
        group_ids: group_ids(),
        timeout_ms: 1_000,
    }
}

fn completion() -> AdminShareGroupsDescription {
    AdminShareGroupsDescription {
        operation_id: operation("describe-share-groups"),
        outcomes: group_ids()
            .into_iter()
            .map(|group_id| AdminShareGroupDescriptionOutcome {
                description: Some(AdminShareGroupDescription {
                    operation_id: operation("describe-share-groups"),
                    group_id: group_id.clone(),
                    state: "Stable".to_owned(),
                    group_epoch: 1,
                    assignment_epoch: 1,
                    assignor_name: "range".to_owned(),
                    members: Vec::new(),
                }),
                group_id,
                error_code: None,
            })
            .collect(),
    }
}

fn expectation(group_id: &str, topic: &str) -> ShareGroupDescriptionExpectation {
    ShareGroupDescriptionExpectation {
        group_id: group_id.to_owned(),
        expected_state: "Stable".to_owned(),
        expected_member_count: 1,
        expected_rack_id: None,
        expected_topic: topic.to_owned(),
        expected_partition: 0,
    }
}

fn group_ids() -> Vec<String> {
    vec!["share-z".to_owned(), "share-a".to_owned()]
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-share-groups.toml"
    ))
    .unwrap_or_else(|error| panic!("parse plural Share description: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode plural Share description: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode plural Share description: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
