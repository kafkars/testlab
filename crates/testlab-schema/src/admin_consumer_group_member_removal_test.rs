//! Static-member removal schema tests pin identities, wire boundaries, and retained baselines.

use crate::{
    AdapterCommand, AdapterEvent, AdminConsumerGroupMemberRemovalOutcome,
    AdminConsumerGroupMembersRemoval, EVIDENCE_SCHEMA_VERSION, PROTOCOL_VERSION,
    RemoveConsumerGroupMembersAction, RemoveConsumerGroupMembersCommand, SCENARIO_SCHEMA_VERSION,
    Scenario, ScenarioAction,
};

#[test]
fn version_cut_and_payloads_preserve_caller_order() {
    assert_eq!(PROTOCOL_VERSION, 133);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 137);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 123);
    round_trip(&ScenarioAction::RemoveConsumerGroupMembers(action()));
    round_trip(&AdapterCommand::RemoveConsumerGroupMembers(command()));
    round_trip(&AdapterEvent::ConsumerGroupMembersRemoved(completion()));
    assert_eq!(command().group_instance_ids, identities());
}

#[test]
fn scenario_requires_the_named_post_shutdown_static_baseline() {
    let scenario = scenario();
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate static-member scenario: {error}"));
    let mut missing = scenario;
    let action = missing
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::RemoveConsumerGroupMembers(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("static-member removal action"));
    action.baseline_operation_id = operation("missing-baseline");
    let error = missing
        .validate()
        .expect_err("missing static-member baseline must fail");
    assert!(error.to_string().contains("named prior group baseline"));
}

#[test]
fn scenario_rejects_a_member_that_was_not_abandoned_before_client_shutdown() {
    let mut invalid = scenario();
    let abandon = invalid
        .steps
        .iter()
        .position(|step| step.id.as_str() == "abandon-static-alpha")
        .unwrap_or_else(|| panic!("alpha abandonment"));
    let shutdown = invalid
        .steps
        .iter()
        .position(|step| step.id.as_str() == "shutdown-client-alpha")
        .unwrap_or_else(|| panic!("alpha shutdown"));
    invalid.steps.swap(abandon, shutdown);
    let error = invalid
        .validate()
        .expect_err("open static member at baseline must fail");
    assert!(
        error
            .to_string()
            .contains("stopped client before the baseline")
    );
}

#[test]
fn scenario_requires_static_sessions_to_cover_the_scenario_deadline() {
    let mut invalid = scenario();
    let scenario_timeout_ms = invalid.timeout_ms;
    let configuration = invalid
        .steps
        .iter_mut()
        .find_map(|step| match &mut step.action {
            ScenarioAction::CreateGroupConsumer {
                configuration: Some(configuration),
                ..
            } if configuration.group_instance_id.as_deref() == Some("static-alpha") => {
                Some(configuration)
            }
            _ => None,
        });
    let configuration = configuration.unwrap_or_else(|| panic!("alpha static configuration"));
    configuration.classic_session_timeout_ms = Some(scenario_timeout_ms - 1);
    let error = invalid
        .validate()
        .expect_err("short static session must not qualify member removal");
    assert!(
        error
            .to_string()
            .contains("session timeout covering the scenario")
    );
}

fn action() -> RemoveConsumerGroupMembersAction {
    RemoveConsumerGroupMembersAction {
        client_id: client(),
        operation_id: operation("remove-static-members"),
        baseline_operation_id: operation("describe-retained-static-members"),
        group_id: "workers".to_owned(),
        group_instance_ids: identities(),
        reason: "testlab stable-cut removal".to_owned(),
        timeout_ms: 30_000,
    }
}

fn command() -> RemoveConsumerGroupMembersCommand {
    let action = action();
    RemoveConsumerGroupMembersCommand {
        client_id: action.client_id,
        operation_id: action.operation_id,
        group_id: action.group_id,
        group_instance_ids: action.group_instance_ids,
        reason: action.reason,
        timeout_ms: action.timeout_ms,
    }
}

fn completion() -> AdminConsumerGroupMembersRemoval {
    AdminConsumerGroupMembersRemoval {
        operation_id: operation("remove-static-members"),
        group_id: "workers".to_owned(),
        throttle_time_ms: 7,
        outcomes: identities()
            .into_iter()
            .map(|group_instance_id| AdminConsumerGroupMemberRemovalOutcome {
                group_instance_id,
                error_code: None,
            })
            .collect(),
    }
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-remove-static-group-members.toml"
    ))
    .unwrap_or_else(|error| panic!("parse static-member scenario: {error}"))
}

fn identities() -> Vec<String> {
    vec!["static-zulu".to_owned(), "static-alpha".to_owned()]
}

fn client() -> crate::ClientId {
    crate::ClientId::new("client-zulu").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn operation(value: &str) -> crate::OperationId {
    crate::OperationId::new(value).unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Eq + std::fmt::Debug,
{
    let encoded = serde_json::to_vec(value)
        .unwrap_or_else(|error| panic!("encode static-member payload: {error}"));
    let decoded = serde_json::from_slice(&encoded)
        .unwrap_or_else(|error| panic!("decode static-member payload: {error}"));
    assert_eq!(&decoded, value);
}
