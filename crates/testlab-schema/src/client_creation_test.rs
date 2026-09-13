//! Expected cluster identity remains bounded, capability-gated scenario intent.

use std::collections::BTreeSet;

use crate::{
    AdapterCommand, Capability, ClientId, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction,
};

const CLUSTER_ID: &str = "4L6g3nShT-eMCtK--X86sw";

#[test]
fn cluster_identity_scenario_rejects_then_reuses_one_client_identity() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-describe-cluster.toml"
    ))
    .unwrap_or_else(|error| panic!("parse cluster identity scenario: {error}"));

    assert_eq!(scenario.schema_version, SCENARIO_SCHEMA_VERSION);
    scenario
        .validate()
        .unwrap_or_else(|error| panic!("validate cluster identity scenario: {error}"));
    assert!(
        scenario
            .requires
            .contains(&Capability::ExpectedClusterIdentity)
    );
    assert!(matches!(
        &scenario.steps[0].action,
        ScenarioAction::CreateClient(crate::CreateClientAction {
            client_id,
            expected_cluster_id: Some(cluster_id),
            expected_error_code: Some(code),
        }) if client_id.as_str() == "client-1" && cluster_id == "4L6g3nShT-eMCtK--X86sx" && code == "identity"
    ));
    assert!(matches!(
        &scenario.steps[1].action,
        ScenarioAction::CreateClient(crate::CreateClientAction {
            client_id,
            expected_cluster_id: Some(cluster_id),
            expected_error_code: None,
        }) if client_id.as_str() == "client-1" && cluster_id == CLUSTER_ID
    ));
}

#[test]
fn expected_failure_oracle_stays_off_the_adapter_command() {
    let client_id = client();
    let action = ScenarioAction::CreateClient(crate::CreateClientAction {
        client_id: client_id.clone(),
        expected_cluster_id: Some("wrong-cluster".to_owned()),
        expected_error_code: Some("identity".to_owned()),
    });
    let command = AdapterCommand::CreateClient(crate::CreateClientCommand {
        client_id,
        expected_cluster_id: Some("wrong-cluster".to_owned()),
    });

    assert_eq!(crate::expected_client_error(&action), Some("identity"));
    let action_json = serde_json::to_string(&action)
        .unwrap_or_else(|error| panic!("encode client action: {error}"));
    let command_json = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode client command: {error}"));
    assert!(action_json.contains("expected_error_code"));
    assert!(!command_json.contains("expected_error_code"));
}

#[test]
fn expected_cluster_identity_is_bounded_and_capability_gated() {
    for (cluster_id, error_code, expected_problem) in [
        (Some(String::new()), None, "must not be empty"),
        (Some("x".repeat(1_025)), None, "must not exceed 1024"),
        (
            None,
            Some("identity".to_owned()),
            "requires expected_cluster_id",
        ),
        (
            Some(CLUSTER_ID.to_owned()),
            Some("configuration".to_owned()),
            "must be identity",
        ),
    ] {
        let scenario = identity_scenario(cluster_id, error_code);
        let error = scenario.validation_error("invalid cluster identity intent must be rejected");
        assert!(
            error
                .problems
                .iter()
                .any(|problem| problem.contains(expected_problem)),
            "{error:?}"
        );
    }

    let mut scenario = identity_scenario(Some(CLUSTER_ID.to_owned()), None);
    scenario
        .requires
        .remove(&Capability::ExpectedClusterIdentity);
    let error = scenario.validation_error("cluster identity must require its exact capability");
    assert!(
        error
            .problems
            .iter()
            .any(|problem| problem.contains("expected_cluster_identity"))
    );
}

fn identity_scenario(
    expected_cluster_id: Option<String>,
    expected_error_code: Option<String>,
) -> Scenario {
    let client_id = client();
    let failed = expected_error_code.is_some();
    let mut steps = vec![crate::ScenarioStep {
        id: crate::StepId::new("create-client").unwrap_or_else(|error| panic!("step id: {error}")),
        action: ScenarioAction::CreateClient(crate::CreateClientAction {
            client_id: client_id.clone(),
            expected_cluster_id,
            expected_error_code,
        }),
    }];
    if !failed {
        steps.push(crate::ScenarioStep {
            id: crate::StepId::new("shutdown-client")
                .unwrap_or_else(|error| panic!("step id: {error}")),
            action: ScenarioAction::ShutdownClient { client_id },
        });
    }
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        id: crate::ScenarioId::new("client.identity-validation")
            .unwrap_or_else(|error| panic!("scenario id: {error}")),
        title: "identity".to_owned(),
        description: "cluster identity validation".to_owned(),
        timeout_ms: 1_000,
        requires: BTreeSet::from([Capability::Lifecycle, Capability::ExpectedClusterIdentity]),
        steps,
        assertions: Vec::new(),
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}
