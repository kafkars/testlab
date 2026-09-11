//! Cluster-admin payloads keep discovery expectations outside adapter commands.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// Scenario intent for one bounded cluster description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeClusterAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded cluster description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeClusterCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public cluster identity and broker set exposed by the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminClusterDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Cluster identity reported by the adapter, when available.
    pub cluster_id: Option<String>,
    /// Sorted broker identifiers reported by the adapter.
    pub broker_ids: Vec<i32>,
}

/// Scenario intent for one bounded cluster feature description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeFeaturesAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Expected public ZK-migration readiness for the controlled environment.
    pub expected_zk_migration_ready: bool,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded cluster feature description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeFeaturesCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// One exact named Kafka feature range.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureVersionRange {
    /// Exact Kafka feature name.
    pub name: String,
    /// Lowest represented feature level.
    pub min_version_level: i16,
    /// Highest represented feature level.
    pub max_version_level: i16,
}

/// Public feature metadata exposed by the packaged client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminFeaturesDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-visible broker-supported feature ranges in canonical name order.
    pub supported_features: Vec<FeatureVersionRange>,
    /// Whether the negotiated response includes minimum-level-zero features.
    pub supported_features_complete: bool,
    /// Cluster finalized-feature epoch, when exposed by Kafka.
    pub finalized_features_epoch: Option<i64>,
    /// Caller-visible finalized feature ranges in canonical name order.
    pub finalized_features: Vec<FeatureVersionRange>,
    /// Exact public ZK-migration readiness flag.
    pub zk_migration_ready: bool,
}

/// One feature row independently normalized from Kafka's pinned administration CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerFeatureState {
    /// Exact Kafka feature name.
    pub name: String,
    /// Lowest broker-supported feature level.
    pub supported_min_version_level: i16,
    /// Highest broker-supported feature level.
    pub supported_max_version_level: i16,
    /// Cluster finalized level printed for this supported feature.
    pub finalized_version_level: i16,
}

/// Independent feature metadata snapshot from Kafka's pinned CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerFeaturesState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete CLI feature rows in canonical name order.
    pub features: Vec<BrokerFeatureState>,
    /// One epoch shared by every CLI feature row, when available.
    pub finalized_features_epoch: Option<i64>,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::{
        AdapterCommand, AdapterEvent, BrokerStateObservation, EVIDENCE_SCHEMA_VERSION,
        PROTOCOL_VERSION, SCENARIO_SCHEMA_VERSION, ScenarioAction,
    };

    #[test]
    fn feature_cut_advances_every_versioned_boundary() {
        assert_eq!(PROTOCOL_VERSION, 54);
        assert_eq!(SCENARIO_SCHEMA_VERSION, 57);
        assert_eq!(EVIDENCE_SCHEMA_VERSION, 43);
    }

    #[test]
    fn expectation_stays_off_wire_and_all_results_round_trip() {
        let action = ScenarioAction::DescribeFeatures(DescribeFeaturesAction {
            client_id: client(),
            operation_id: operation(),
            expected_zk_migration_ready: false,
            timeout_ms: 1_000,
        });
        let command = AdapterCommand::DescribeFeatures(DescribeFeaturesCommand {
            client_id: client(),
            operation_id: operation(),
            timeout_ms: 1_000,
        });
        round_trip(&action);
        round_trip(&command);
        let encoded = serde_json::to_string(&command)
            .unwrap_or_else(|error| panic!("encode feature command: {error}"));
        assert!(!encoded.contains("expected_zk_migration_ready"));

        let ranges = vec![range("metadata.version", 4, 30)];
        round_trip(&AdapterEvent::FeaturesDescribed(AdminFeaturesDescription {
            operation_id: operation(),
            supported_features: ranges.clone(),
            supported_features_complete: true,
            finalized_features_epoch: Some(7),
            finalized_features: vec![range("metadata.version", 30, 30)],
            zk_migration_ready: false,
        }));
        round_trip(&BrokerStateObservation::Features(BrokerFeaturesState {
            observation: 3,
            operation_id: operation(),
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
    fn feature_action_requires_a_live_client_and_bounded_unique_operation() {
        let mut problems = Vec::new();
        let mut operation_ids = BTreeSet::new();
        crate::admin_action_validation::validate(
            &ScenarioAction::DescribeFeatures(DescribeFeaturesAction {
                client_id: client(),
                operation_id: operation(),
                expected_zk_migration_ready: false,
                timeout_ms: 1_000,
            }),
            &BTreeMap::from([(client(), false)]),
            &mut operation_ids,
            &mut problems,
        );
        assert!(problems.is_empty(), "{problems:?}");
        assert!(operation_ids.contains(&operation()));
    }

    fn round_trip<T>(value: &T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_vec(value)
            .unwrap_or_else(|error| panic!("encode feature value: {error}"));
        let decoded = serde_json::from_slice::<T>(&encoded)
            .unwrap_or_else(|error| panic!("decode feature value: {error}"));
        assert_eq!(&decoded, value);
    }

    fn range(name: &str, min_version_level: i16, max_version_level: i16) -> FeatureVersionRange {
        FeatureVersionRange {
            name: name.to_owned(),
            min_version_level,
            max_version_level,
        }
    }

    fn client() -> ClientId {
        ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"))
    }

    fn operation() -> OperationId {
        OperationId::new("admin-features").unwrap_or_else(|error| panic!("operation id: {error}"))
    }
}
