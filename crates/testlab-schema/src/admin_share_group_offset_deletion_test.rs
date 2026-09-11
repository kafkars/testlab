//! Share-group offset deletion schema tests pin topic-wide intent and preconditions.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    AdapterCommand, AdapterEvent, AdminShareGroupOffsetDeletion, ClientId,
    DeleteShareGroupOffsetsAction, DeleteShareGroupOffsetsCommand, OperationId, Scenario,
    ScenarioAction,
};

#[test]
fn action_command_and_public_result_round_trip_without_partition_on_wire() {
    round_trip(&ScenarioAction::DeleteShareGroupOffsets(action()));
    let command = AdapterCommand::DeleteShareGroupOffsets(command());
    round_trip(&command);
    round_trip(&AdapterEvent::ShareGroupOffsetsDeleted(deletion()));
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode Share-group offset deletion: {error}"));
    assert!(!encoded.contains("partition"));
    assert!(encoded.contains("share-topic"));
}

#[test]
fn checked_in_share_group_offset_deletion_scenario_is_valid() {
    scenario()
        .validate()
        .unwrap_or_else(|error| panic!("validate Share-group offset deletion: {error}"));
}

#[test]
fn validation_rejects_negative_absence_partition() {
    let clients = BTreeMap::from([(client(), false)]);
    let mut invalid = action();
    invalid.partition = -1;
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &ScenarioAction::DeleteShareGroupOffsets(invalid),
        &clients,
        &mut BTreeSet::new(),
        &mut problems,
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("partition must be nonnegative")),
        "{problems:?}"
    );
}

#[test]
fn transition_requires_closed_members_and_a_listed_baseline() {
    let source = scenario();
    for (label, mut scenario) in [
        ("closed member", source.clone()),
        ("listed baseline", source),
    ] {
        if label == "closed member" {
            scenario
                .steps
                .retain(|step| !matches!(&step.action, ScenarioAction::CloseShareConsumer { .. }));
        } else {
            scenario
                .steps
                .retain(|step| !matches!(&step.action, ScenarioAction::ListShareGroupOffsets(_)));
        }
        let mut problems = Vec::new();
        crate::admin_share_group_offset_transition_validation::validate(&scenario, &mut problems);
        assert!(!problems.is_empty(), "{label} deletion unexpectedly passed");
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-share-group-offsets.toml"
    ))
    .unwrap_or_else(|error| panic!("parse Share-group offset deletion: {error}"))
}

fn action() -> DeleteShareGroupOffsetsAction {
    DeleteShareGroupOffsetsAction {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
        timeout_ms: 1_000,
    }
}

fn command() -> DeleteShareGroupOffsetsCommand {
    DeleteShareGroupOffsetsCommand {
        client_id: client(),
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        timeout_ms: 1_000,
    }
}

fn deletion() -> AdminShareGroupOffsetDeletion {
    AdminShareGroupOffsetDeletion {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        topic_id: [1; 16],
        error_code: None,
    }
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
{
    let encoded = serde_json::to_string(value)
        .unwrap_or_else(|error| panic!("encode Share-group offset deletion: {error}"));
    let decoded = serde_json::from_str::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode Share-group offset deletion: {error}"));
    assert_eq!(&decoded, value);
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("delete-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
