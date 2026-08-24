//! Environment manifest validation evidence.

use std::collections::BTreeMap;

use super::{
    Authentication, BrokerIdentity, EnvironmentDriver, EnvironmentError, EnvironmentId,
    EnvironmentManifest, SecurityProfile, TransportSecurity,
};

#[test]
fn immutable_compose_environment_is_accepted() {
    assert!(environment().validate().is_ok());
}

#[test]
fn compose_environment_round_trips_toml() {
    let expected = environment();
    let encoded = toml::to_string(&expected)
        .unwrap_or_else(|error| panic!("serialize environment fixture: {error}"));
    let actual = toml::from_str::<EnvironmentManifest>(&encoded)
        .unwrap_or_else(|error| panic!("parse environment fixture: {error}"));

    assert_eq!(actual, expected);
}

#[test]
fn floating_image_is_rejected() {
    let mut manifest = environment();
    let EnvironmentDriver::DockerCompose { image, .. } = &mut manifest.driver else {
        panic!("fixture must use Docker Compose");
    };
    *image = "apache/kafka:4.3.1".to_owned();

    assert!(matches!(
        manifest.validate(),
        Err(EnvironmentError::ImageNotImmutable(_))
    ));
}

#[test]
fn topology_requires_one_service_per_broker() {
    let mut manifest = environment();
    let EnvironmentDriver::DockerCompose {
        broker_services, ..
    } = &mut manifest.driver
    else {
        panic!("fixture must use Docker Compose");
    };
    broker_services.pop();

    assert!(matches!(
        manifest.validate(),
        Err(EnvironmentError::BrokerServiceCount {
            cluster_size: 3,
            services: 2
        })
    ));
}

#[test]
fn compose_paths_cannot_escape_the_catalog() {
    let mut manifest = environment();
    let EnvironmentDriver::DockerCompose { compose_files, .. } = &mut manifest.driver else {
        panic!("fixture must use Docker Compose");
    };
    compose_files.push("../unreviewed.yml".to_owned());

    assert!(matches!(
        manifest.validate(),
        Err(EnvironmentError::ComposePathInvalid(_))
    ));
}

#[test]
fn feature_levels_require_portable_names() {
    let mut manifest = environment();
    let EnvironmentDriver::DockerCompose { feature_levels, .. } = &mut manifest.driver else {
        panic!("fixture must use Docker Compose");
    };
    feature_levels.insert("share version".to_owned(), 1);

    assert_eq!(
        manifest.validate(),
        Err(EnvironmentError::FeatureNameInvalid(
            "share version".to_owned()
        ))
    );
}

fn environment() -> EnvironmentManifest {
    EnvironmentManifest {
        schema_version: 2,
        id: EnvironmentId::new("apache-kafka-4.3.1-plaintext")
            .unwrap_or_else(|error| panic!("fixture id: {error}")),
        title: "Apache Kafka 4.3.1 three-broker plaintext".to_owned(),
        driver: EnvironmentDriver::DockerCompose {
            broker: BrokerIdentity {
                implementation: "apache-kafka".to_owned(),
                version: "4.3.1".to_owned(),
            },
            image: format!("apache/kafka@sha256:{}", "a".repeat(64)),
            cluster_size: 3,
            security: SecurityProfile {
                transport: TransportSecurity::Plaintext,
                authentication: Authentication::None,
            },
            compose_files: vec!["clusters/apache-kafka/cluster.yml".to_owned()],
            broker_services: vec![
                "kafka-1".to_owned(),
                "kafka-2".to_owned(),
                "kafka-3".to_owned(),
            ],
            client_port: 19_092,
            feature_levels: BTreeMap::new(),
        },
    }
}
