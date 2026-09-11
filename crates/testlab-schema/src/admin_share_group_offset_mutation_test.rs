//! Share-group offset mutation schema tests pin intent separation and preconditions.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetAlteration, AlterShareGroupOffsetsAction,
    AlterShareGroupOffsetsCommand, ClientId, OperationId, Scenario, ScenarioAction,
};

#[test]
fn action_command_and_public_result_round_trip_without_expected_lag_on_wire() {
    round_trip(&ScenarioAction::AlterShareGroupOffsets(action()));
    let command = AdapterCommand::AlterShareGroupOffsets(command());
    round_trip(&command);
    round_trip(&AdapterEvent::ShareGroupOffsetsAltered(alteration()));
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode Share-group offset command: {error}"));
    assert!(!encoded.contains("expected_lag"));
    assert!(encoded.contains("start_offset"));
}

#[test]
fn checked_in_share_group_offset_alteration_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate Share-group offset alteration: {error}"));
}

#[test]
fn validation_rejects_negative_mutation_state() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut invalid = action();
    invalid.start_offset = -1;
    invalid.expected_lag = -1;
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::AlterShareGroupOffsets(invalid),
        &clients,
        &mut BTreeSet::new(),
        &mut problems,
    );
    for expected in [
        "start_offset must be nonnegative",
        "expected_lag must be nonnegative",
    ] {
        assert!(
            problems.iter().any(|problem| problem.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}

#[test]
fn transition_requires_closed_members_and_a_different_listed_baseline() {
    let source = scenario();
    for (label, mut scenario) in [
        ("closed member", source.clone()),
        ("listed baseline", source.clone()),
        ("different baseline", source),
    ] {
        match label {
            "closed member" => scenario
                .steps
                .retain(|step| !matches!(&step.action, ScenarioAction::CloseShareConsumer { .. })),
            "listed baseline" => scenario.steps.retain(|step| {
                !matches!(
                    &step.action,
                    ScenarioAction::ListShareGroupOffsets(action)
                        if action.operation_id.to_string().contains("before-alter")
                )
            }),
            "different baseline" => {
                let Some(action) =
                    scenario
                        .steps
                        .iter_mut()
                        .find_map(|step| match &mut step.action {
                            ScenarioAction::ListShareGroupOffsets(action)
                                if action.operation_id.to_string().contains("before-alter") =>
                            {
                                Some(action)
                            }
                            _ => None,
                        })
                else {
                    panic!("baseline action");
                };
                action.expected_start_offset = 2;
            }
            _ => unreachable!(),
        }
        let mut problems = Vec::new();
        crate::admin_share_group_offset_transition_validation::validate(&scenario, &mut problems);
        assert!(!problems.is_empty(), "{label} mutation unexpectedly passed");
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-alter-share-group-offsets.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Share-group offset alteration: {error}"))
}

fn action() -> AlterShareGroupOffsetsAction {
    AlterShareGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        start_offset: 2,
        expected_lag: 0,
        timeout_ms: 1_000,
    }
}

fn command() -> AlterShareGroupOffsetsCommand {
    AlterShareGroupOffsetsCommand {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        start_offset: 2,
        timeout_ms: 1_000,
    }
}

fn alteration() -> AdminShareGroupOffsetAlteration {
    AdminShareGroupOffsetAlteration {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        topic_id: [1; 16],
        error_code: None,
    }
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode Share-group offset mutation: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode Share-group offset mutation: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("alter-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
