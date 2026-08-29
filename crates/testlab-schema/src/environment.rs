//! Environment manifests identify independently controlled broker topologies.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::EnvironmentId;

/// Current environment manifest version.
pub const ENVIRONMENT_SCHEMA_VERSION: u16 = 4;

/// One independently controlled scenario environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentManifest {
    /// Exact environment schema version.
    pub schema_version: u16,
    /// Stable environment identity.
    pub id: EnvironmentId,
    /// Human-readable topology title.
    pub title: String,
    /// Effectful driver configuration.
    pub driver: EnvironmentDriver,
}

impl EnvironmentManifest {
    /// Validates environment identity without acquiring external effects.
    pub fn validate(&self) -> Result<(), EnvironmentError> {
        crate::environment_validation::validate(self)
    }
}

/// Runtime responsible for environment effects and observations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EnvironmentDriver {
    /// Testlab's in-process harness self-test environment.
    ModelBroker,
    /// External minimal Kafka peer with scenario-controlled hostile responses.
    KafkaProtocolAdversary {
        /// Sole baseline topic exposed by the peer.
        topic: String,
    },
    /// An immutable broker image controlled through Docker Compose.
    DockerCompose {
        /// Broker implementation and public version.
        broker: BrokerIdentity,
        /// Image reference containing an exact SHA-256 repository digest.
        image: String,
        /// Number of broker services required in the topology.
        cluster_size: u16,
        /// Client-facing transport and authentication contract.
        security: SecurityProfile,
        /// Repository-relative Compose files in application order.
        compose_files: Vec<String>,
        /// Compose service names that must expose brokers.
        broker_services: Vec<String>,
        /// Container port used for client bootstrap discovery.
        client_port: u16,
        /// Explicit broker feature levels established after readiness.
        #[serde(default)]
        feature_levels: BTreeMap<String, u16>,
        /// Routes packaged-client traffic through an external fault proxy.
        #[serde(default)]
        network_proxy: bool,
    },
}

/// Public broker product identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerIdentity {
    /// Broker implementation, such as Apache Kafka.
    pub implementation: String,
    /// Exact public broker version.
    pub version: String,
}

/// Client-facing security contract for one environment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityProfile {
    /// Transport trust mode.
    pub transport: TransportSecurity,
    /// Authentication mechanism.
    pub authentication: Authentication,
}

/// Client-facing transport security.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportSecurity {
    /// No TLS on the client-facing listener.
    Plaintext,
    /// TLS using an explicit qualification certificate authority.
    TlsCustom,
}

/// Client-facing authentication mechanism.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Authentication {
    /// No SASL authentication.
    None,
    /// SASL/PLAIN authentication.
    Plain,
    /// SASL/SCRAM-SHA-256 authentication.
    ScramSha256,
    /// SASL/SCRAM-SHA-512 authentication.
    ScramSha512,
}

/// Invalid environment manifest.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EnvironmentError {
    /// The environment schema is unknown.
    #[error("unsupported environment schema version {0}")]
    UnsupportedVersion(u16),
    /// The environment title was empty.
    #[error("environment title must not be empty")]
    EmptyTitle,
    /// The adversary topic was not a portable Kafka topic name.
    #[error("invalid protocol-adversary topic {0}")]
    AdversaryTopicInvalid(String),
    /// The broker implementation or version was empty.
    #[error("broker implementation and version must not be empty")]
    BrokerIdentityEmpty,
    /// The broker image was not pinned by repository digest.
    #[error("broker image must contain one exact SHA-256 repository digest: {0}")]
    ImageNotImmutable(String),
    /// The topology declared no brokers.
    #[error("environment cluster_size must be greater than zero")]
    ClusterEmpty,
    /// The topology declared port zero.
    #[error("environment client_port must be greater than zero")]
    ClientPortZero,
    /// One broker feature name was not portable.
    #[error("invalid broker feature name {0}")]
    FeatureNameInvalid(String),
    /// The Compose file list was empty.
    #[error("Docker Compose environment must declare at least one Compose file")]
    ComposeFilesEmpty,
    /// One Compose path was not a portable repository-relative YAML path.
    #[error("invalid repository-relative Compose path {0}")]
    ComposePathInvalid(String),
    /// One Compose path appeared twice.
    #[error("duplicate Compose path {0}")]
    ComposePathDuplicate(String),
    /// The service count disagreed with the topology.
    #[error(
        "cluster_size {cluster_size} requires the same number of broker services, found {services}"
    )]
    BrokerServiceCount {
        /// Declared broker count.
        cluster_size: u16,
        /// Declared broker service count.
        services: usize,
    },
    /// One Compose service name was not portable.
    #[error("invalid broker service name {0}")]
    BrokerServiceInvalid(String),
    /// One broker service appeared twice.
    #[error("duplicate broker service name {0}")]
    BrokerServiceDuplicate(String),
    /// The current proxy listener supports plaintext unauthenticated Kafka only.
    #[error("network proxy currently requires plaintext Kafka without SASL")]
    NetworkProxySecurityUnsupported,
}
