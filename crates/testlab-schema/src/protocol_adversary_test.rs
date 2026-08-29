//! Adversary schema tests cover exact JSON identities and deterministic bounds.

use crate::{
    ADVERSARY_PROTOCOL_VERSION, AdversaryControlEnvelope, EnvironmentDriver, EnvironmentManifest,
    EnvironmentOperationId, KafkaApi, ProtocolFault, ProtocolFaultAction,
};

#[test]
fn control_round_trip_preserves_every_selected_wire_fact() {
    let envelope = AdversaryControlEnvelope {
        protocol_version: ADVERSARY_PROTOCOL_VERSION,
        control: control(ProtocolFault::WrongCorrelationId { delta: -7 }),
    };

    let encoded = serde_json::to_string(&envelope)
        .unwrap_or_else(|error| panic!("encode adversary control: {error}"));
    let decoded = serde_json::from_str::<AdversaryControlEnvelope>(&encoded)
        .unwrap_or_else(|error| panic!("decode adversary control: {error}"));

    assert_eq!(decoded, envelope);
    assert!(encoded.contains("\"api\":\"metadata\""));
    assert!(encoded.contains("\"kind\":\"wrong_correlation_id\""));
}

#[test]
fn control_bounds_reject_ambiguous_or_unbounded_faults() {
    for fault in [
        ProtocolFault::WrongCorrelationId { delta: 0 },
        ProtocolFault::PartialFrame { bytes: 0 },
        ProtocolFault::Stall {
            duration_ms: 30_001,
        },
    ] {
        assert!(control(fault).validate().is_err());
    }
    let mut applications = control(ProtocolFault::StaleResponse);
    applications.applications = 0;
    assert!(applications.validate().is_err());
}

#[test]
fn supported_api_keys_and_adversary_topic_are_exact() {
    for api in [
        KafkaApi::Produce,
        KafkaApi::Metadata,
        KafkaApi::ApiVersions,
        KafkaApi::InitProducerId,
        KafkaApi::DescribeCluster,
    ] {
        assert_eq!(KafkaApi::from_key(api.key()), Some(api));
    }
    assert_eq!(KafkaApi::from_key(61), None);

    let manifest = EnvironmentManifest {
        schema_version: crate::ENVIRONMENT_SCHEMA_VERSION,
        id: crate::EnvironmentId::new("adversary")
            .unwrap_or_else(|error| panic!("environment id: {error}")),
        title: "protocol adversary".to_owned(),
        driver: EnvironmentDriver::KafkaProtocolAdversary {
            topic: "orders.v1".to_owned(),
        },
    };
    assert!(manifest.validate().is_ok());
    let mut invalid = manifest;
    invalid.driver = EnvironmentDriver::KafkaProtocolAdversary {
        topic: "invalid topic".to_owned(),
    };
    assert!(invalid.validate().is_err());
}

fn control(fault: ProtocolFault) -> ProtocolFaultAction {
    ProtocolFaultAction {
        operation_id: EnvironmentOperationId::new("fault-1")
            .unwrap_or_else(|error| panic!("operation id: {error}")),
        api: KafkaApi::Metadata,
        applications: 1,
        fault,
    }
}
