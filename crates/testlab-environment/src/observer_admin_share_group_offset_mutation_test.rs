//! Share-group offset mutation observations require exact intent and CLI identity.

use testlab_schema::{
    AdapterCommand, AlterShareGroupOffsetsAction, AlterShareGroupOffsetsCommand,
    BrokerStateObservation, ClientId, OperationId, Scenario, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, ShareGroupOffsetTarget};

#[test]
fn exact_mutation_maps_to_one_offsets_cli_target() {
    let action = ScenarioAction::AlterShareGroupOffsets(action());
    let command = AdapterCommand::AlterShareGroupOffsets(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map mutation target: {error}"))
        .unwrap_or_else(|| panic!("mutation target"));
    assert_eq!(target, expected_target());
    let observation = crate::share_group_cli_observation::normalize(
        7,
        &target,
        b"GROUP TOPIC PARTITION START-OFFSET LAG\nshare-group-1 share-topic 0 2 0\n",
    )
    .unwrap_or_else(|error| panic!("normalize mutation post-state: {error}"));
    let BrokerStateObservation::ShareGroupOffset(actual) = observation else {
        panic!("Share-group offset observation");
    };
    assert_eq!(actual.start_offset, Some(2));
    assert_eq!(actual.lag, Some(0));
}

#[test]
fn altered_wire_intent_is_rejected_before_observation() {
    let action = ScenarioAction::AlterShareGroupOffsets(action());
    let mut command = command();
    command.start_offset = 1;
    let error = AdminTarget::from_exact(&action, &AdapterCommand::AlterShareGroupOffsets(command))
        .expect_err("mismatched mutation intent");
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn mutation_topic_is_part_of_environment_provisioning() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-alter-share-group-offsets.toml"
    ))
    .unwrap_or_else(|error| panic!("parse alteration scenario: {error}"));
    let topics = crate::compose_provision_targets::topics(&scenario);
    assert_eq!(
        topics.get("testlab-kafkars-admin-share-group-alter"),
        Some(&1)
    );
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

fn expected_target() -> AdminTarget {
    AdminTarget::ShareGroupOffset(ShareGroupOffsetTarget {
        operation_id: operation(),
        group_id: "share-group-1".to_owned(),
        topic: "share-topic".to_owned(),
        partition: 0,
    })
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("alter-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
