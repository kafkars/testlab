//! Configuration-resource listing targets retain exact provisioning and observation scope.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterCommand, ClientId, ConfigResourceListingApi, ListConfigResourcesAction,
    ListConfigResourcesCommand, OperationId, Scenario, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, ListTarget};

#[test]
fn exact_command_maps_to_required_topic_metadata() {
    let action = ScenarioAction::ListConfigResources(ListConfigResourcesAction {
        client_id: client(),
        operation_id: operation(),
        api: ConfigResourceListingApi::Resource,
        required_resources: topics(),
        timeout_ms: 20_000,
    });
    let command = AdapterCommand::ListConfigResources(ListConfigResourcesCommand {
        client_id: client(),
        operation_id: operation(),
        api: ConfigResourceListingApi::Resource,
        timeout_ms: 20_000,
    });
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map configuration resources: {error}"));
    assert_eq!(
        target,
        Some(AdminTarget::Topics(ListTarget {
            operation_id: operation(),
            names: topics(),
        }))
    );
}

#[test]
fn dedicated_command_maps_to_one_client_metrics_cli_snapshot() {
    let action = ScenarioAction::ListConfigResources(ListConfigResourcesAction {
        client_id: client(),
        operation_id: operation(),
        api: ConfigResourceListingApi::ClientMetrics,
        required_resources: topics(),
        timeout_ms: 20_000,
    });
    let command = AdapterCommand::ListConfigResources(ListConfigResourcesCommand {
        client_id: client(),
        operation_id: operation(),
        api: ConfigResourceListingApi::ClientMetrics,
        timeout_ms: 20_000,
    });
    let target = AdminTarget::from_exact(&action, &command)
        .unwrap_or_else(|error| panic!("map client-metrics resources: {error}"));
    assert_eq!(
        target,
        Some(AdminTarget::ClientMetricsResources(ListTarget {
            operation_id: operation(),
            names: topics(),
        }))
    );
}

#[test]
fn checked_in_scenario_provisions_every_required_topic() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-config-resources.toml"
    ))
    .unwrap_or_else(|error| panic!("parse configuration-resource scenario: {error}"));
    assert_eq!(
        crate::compose_provision_targets::topics(&scenario),
        BTreeMap::from([
            ("testlab-kafkars-config-resource-alpha".to_owned(), 1),
            ("testlab-kafkars-config-resource-zulu".to_owned(), 1),
        ])
    );
}

fn topics() -> Vec<String> {
    vec![
        "testlab-kafkars-config-resource-zulu".to_owned(),
        "testlab-kafkars-config-resource-alpha".to_owned(),
    ]
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client: {error}"))
}

fn operation() -> OperationId {
    OperationId::new("admin-list-config-resources")
        .unwrap_or_else(|error| panic!("operation: {error}"))
}
