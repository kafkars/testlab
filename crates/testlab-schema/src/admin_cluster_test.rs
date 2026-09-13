//! Cluster and producer-state protocol payload tests.

use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::{
    AdapterCommand, AdapterEvent, BrokerStateObservation, EVIDENCE_SCHEMA_VERSION,
    PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, ScenarioAction,
};

#[test]
fn producer_state_cut_advances_every_versioned_boundary() {
    assert_eq!(PROTOCOL_VERSION, 117);
    assert_eq!(SCENARIO_SCHEMA_VERSION, 120);
    assert_eq!(EVIDENCE_SCHEMA_VERSION, 106);
}

#[test]
fn cluster_authorization_option_and_result_round_trip() {
    let operation_id =
        OperationId::new("admin-cluster").unwrap_or_else(|error| panic!("operation id: {error}"));
    let action = ScenarioAction::DescribeCluster(DescribeClusterAction {
        client_id: client(),
        operation_id: operation_id.clone(),
        include_authorized_operations: true,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeCluster(DescribeClusterCommand {
        client_id: client(),
        operation_id: operation_id.clone(),
        include_authorized_operations: true,
        timeout_ms: 1_000,
    });
    round_trip(&action);
    round_trip(&command);
    round_trip(&AdapterEvent::ClusterDescribed(AdminClusterDescription {
        operation_id,
        cluster_id: Some("cluster-a".to_owned()),
        broker_ids: vec![1],
        authorized_operations: Some(1),
    }));
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode cluster command: {error}"));
    assert!(encoded.contains("\"include_authorized_operations\":true"));
}

#[test]
fn feature_expectation_stays_off_wire_and_results_round_trip() {
    let action = ScenarioAction::DescribeFeatures(DescribeFeaturesAction {
        client_id: client(),
        operation_id: feature_operation(),
        expected_zk_migration_ready: false,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeFeatures(DescribeFeaturesCommand {
        client_id: client(),
        operation_id: feature_operation(),
        timeout_ms: 1_000,
    });
    round_trip(&action);
    round_trip(&command);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode feature command: {error}"));
    assert!(!encoded.contains("expected_zk_migration_ready"));

    let ranges = vec![range("metadata.version", 4, 30)];
    round_trip(&AdapterEvent::FeaturesDescribed(AdminFeaturesDescription {
        operation_id: feature_operation(),
        supported_features: ranges.clone(),
        supported_features_complete: true,
        finalized_features_epoch: Some(7),
        finalized_features: vec![range("metadata.version", 30, 30)],
        zk_migration_ready: false,
    }));
    round_trip(&BrokerStateObservation::Features(BrokerFeaturesState {
        observation: 3,
        operation_id: feature_operation(),
        features: vec![BrokerFeatureState {
            name: "metadata.version".to_owned(),
            supported_min_version_level: 4,
            supported_max_version_level: 30,
            finalized_version_level: 30,
        }],
        finalized_features_epoch: Some(7),
    }));
}

#[test]
fn feature_update_baseline_stays_off_wire_and_results_round_trip() {
    let update = FeatureUpdateSpec {
        name: "metadata.version".to_owned(),
        max_version_level: 30,
        kind: FeatureUpdateKind::Upgrade,
    };
    let action = ScenarioAction::ValidateFeatureUpdates(ValidateFeatureUpdatesAction {
        client_id: client(),
        operation_id: feature_update_operation(),
        baseline_operation_id: feature_operation(),
        updates: vec![update.clone()],
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::ValidateFeatureUpdates(ValidateFeatureUpdatesCommand {
        client_id: client(),
        operation_id: feature_update_operation(),
        updates: vec![update],
        timeout_ms: 1_000,
    });
    round_trip(&action);
    round_trip(&command);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode update command: {error}"));
    assert!(!encoded.contains("baseline_operation_id"));
    round_trip(&AdapterEvent::FeatureUpdatesValidated(
        AdminFeatureUpdatesValidation {
            operation_id: feature_update_operation(),
            throttle_time_ms: 0,
            outcomes: vec![AdminFeatureUpdateOutcome {
                name: "metadata.version".to_owned(),
                error_code: None,
            }],
        },
    ));
}

#[test]
fn producer_count_expectation_stays_off_wire_and_results_round_trip() {
    let action = ScenarioAction::DescribeProducers(DescribeProducersAction {
        client_id: client(),
        operation_id: producer_operation(),
        topic: "orders".to_owned(),
        partition: 2,
        expected_producer_count: 1,
        timeout_ms: 1_000,
    });
    let command = AdapterCommand::DescribeProducers(DescribeProducersCommand {
        client_id: client(),
        operation_id: producer_operation(),
        topic: "orders".to_owned(),
        partition: 2,
        timeout_ms: 1_000,
    });
    round_trip(&action);
    round_trip(&command);
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode producer command: {error}"));
    assert!(!encoded.contains("expected_producer_count"));

    let state = producer_state();
    round_trip(&AdapterEvent::ProducersDescribed(
        AdminProducersDescription {
            operation_id: producer_operation(),
            topic: "orders".to_owned(),
            partition: 2,
            producers: vec![state.clone()],
        },
    ));
    round_trip(&BrokerStateObservation::Producers(BrokerProducersState {
        observation: 4,
        operation_id: producer_operation(),
        topic: "orders".to_owned(),
        partition: 2,
        producers: vec![state],
    }));
}

#[test]
fn admin_discovery_actions_require_live_clients_and_bounded_unique_operations() {
    for action in [
        ScenarioAction::DescribeFeatures(DescribeFeaturesAction {
            client_id: client(),
            operation_id: feature_operation(),
            expected_zk_migration_ready: false,
            timeout_ms: 1_000,
        }),
        ScenarioAction::DescribeProducers(DescribeProducersAction {
            client_id: client(),
            operation_id: producer_operation(),
            topic: "orders".to_owned(),
            partition: 0,
            expected_producer_count: 1,
            timeout_ms: 1_000,
        }),
    ] {
        let mut problems = Vec::new();
        let mut operation_ids = BTreeSet::new();
        crate::admin_action_validation::validate(
            &action,
            &BTreeMap::from([(client(), false)]),
            &mut operation_ids,
            &mut problems,
        );
        assert!(problems.is_empty(), "{problems:?}");
    }
}

#[test]
fn producer_state_intent_rejects_negative_partitions_and_empty_expectations() {
    let action = ScenarioAction::DescribeProducers(DescribeProducersAction {
        client_id: client(),
        operation_id: producer_operation(),
        topic: "orders".to_owned(),
        partition: -1,
        expected_producer_count: 0,
        timeout_ms: 1_000,
    });
    let mut problems = Vec::new();
    crate::admin_action_validation::validate(
        &action,
        &BTreeMap::from([(client(), false)]),
        &mut BTreeSet::new(),
        &mut problems,
    );
    assert!(
        problems.iter().any(|problem| problem.contains("partition")),
        "{problems:?}"
    );
    assert!(
        problems
            .iter()
            .any(|problem| problem.contains("expected_producer_count")),
        "{problems:?}"
    );
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded =
        serde_json::to_vec(value).unwrap_or_else(|error| panic!("encode admin value: {error}"));
    let decoded = serde_json::from_slice::<T>(&encoded)
        .unwrap_or_else(|error| panic!("decode admin value: {error}"));
    assert_eq!(&decoded, value);
}

fn range(name: &str, min_version_level: i16, max_version_level: i16) -> FeatureVersionRange {
    FeatureVersionRange {
        name: name.to_owned(),
        min_version_level,
        max_version_level,
    }
}

fn producer_state() -> ProducerStateSnapshot {
    ProducerStateSnapshot {
        producer_id: 71,
        producer_epoch: 2,
        last_sequence: 0,
        last_timestamp: 1_700_000_000_000,
        coordinator_epoch: 4,
        current_transaction_start_offset: None,
    }
}

fn client() -> ClientId {
    ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
}

fn feature_operation() -> OperationId {
    OperationId::new("admin-features").unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn producer_operation() -> OperationId {
    OperationId::new("admin-producers").unwrap_or_else(|error| panic!("operation id: {error}"))
}

fn feature_update_operation() -> OperationId {
    OperationId::new("admin-validate-feature-updates")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
