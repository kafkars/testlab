//! Share-group offset deletion observations retain exact topic intent and absence.

use testlab_schema::{
    AdapterCommand, BrokerStateObservation, ClientId, DeleteShareGroupOffsetsAction,
    DeleteShareGroupOffsetsCommand, OperationId, Scenario, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, ShareGroupOffsetTarget};

#[test]
fn exact_deletion_maps_to_one_offsets_cli_absence_target() {
    let action = ScenarioAction::DeleteShareGroupOffsets(action());
    let command = AdapterCommand::DeleteShareGroupOffsets(command());
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map deletion target: {error}"))
        .unwrap_or_else(|| panic!("deletion target"));
    assert_eq!(target, expected_target());
    for output in [
        "GROUP TOPIC PARTITION START-OFFSET LAG\n",
        "GROUP TOPIC PARTITION START-OFFSET LAG\nshare-group-1 sibling-topic 0 4 0\n",
    ] {
        let observation =
            crate::share_group_cli_observation::normalize(7, &target, output.as_bytes())
                .unwrap_or_else(|error| panic!("normalize deletion post-state: {error}"));
        let BrokerStateObservation::ShareGroupOffset(actual) = observation else {
            panic!("Share-group offset observation");
        };
        assert_eq!(actual.start_offset, None);
        assert_eq!(actual.lag, None);
    }
}

#[test]
fn altered_wire_topic_is_rejected_before_observation() {
    let action = ScenarioAction::DeleteShareGroupOffsets(action());
    let mut command = command();
    command.topic = "foreign-topic".to_owned();
    let error = AdminTarget::from_exact(&action, &AdapterCommand::DeleteShareGroupOffsets(command))
        .err()
        .unwrap_or_else(|| panic!("mismatched deletion intent"));
    assert!(error.to_string().contains("does not exactly match"));
}

#[test]
fn deletion_topic_is_part_of_environment_provisioning() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-share-group-offsets.toml"
    ))
    .unwrap_or_else(|error| panic!("parse deletion scenario: {error}"));
    let topics = crate::compose_provision_targets::topics(&scenario);
    assert_eq!(
        topics.get("testlab-kafkars-admin-share-group-delete"),
        Some(&1)
    );
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
    OperationId::new("delete-share-group-offsets")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
