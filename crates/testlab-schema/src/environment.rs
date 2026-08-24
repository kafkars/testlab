//! Environment manifests identify independently controlled broker topologies.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::EnvironmentId;

/// Current environment manifest version.
pub const ENVIRONMENT_SCHEMA_VERSION: u16 = 2;

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
        if self.schema_version != ENVIRONMENT_SCHEMA_VERSION {
            return Err(EnvironmentError::UnsupportedVersion(self.schema_version));
        }
        if self.title.trim().is_empty() {
            return Err(EnvironmentError::EmptyTitle);
        }
        match &self.driver {
            EnvironmentDriver::ModelBroker => Ok(()),
            EnvironmentDriver::DockerCompose {
                broker,
                image,
                cluster_size,
                security: _,
                compose_files,
                broker_services,
                client_port,
                feature_levels,
            } => validate_compose(
                broker,
                image,
                *cluster_size,
                compose_files,
                broker_services,
                *client_port,
                feature_levels,
            ),
        }
    }
}

/// Runtime responsible for environment effects and observations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EnvironmentDriver {
    /// Testlab's in-process harness self-test environment.
    ModelBroker,
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

fn validate_compose(
    broker: &BrokerIdentity,
    image: &str,
    cluster_size: u16,
    compose_files: &[String],
    broker_services: &[String],
    client_port: u16,
    feature_levels: &BTreeMap<String, u16>,
) -> Result<(), EnvironmentError> {
    if broker.implementation.trim().is_empty() || broker.version.trim().is_empty() {
        return Err(EnvironmentError::BrokerIdentityEmpty);
    }
    validate_image(image)?;
    if cluster_size == 0 {
        return Err(EnvironmentError::ClusterEmpty);
    }
    if client_port == 0 {
        return Err(EnvironmentError::ClientPortZero);
    }
    validate_feature_names(feature_levels)?;
    validate_paths(compose_files)?;
    validate_services(cluster_size, broker_services)
}

fn validate_feature_names(feature_levels: &BTreeMap<String, u16>) -> Result<(), EnvironmentError> {
    for name in feature_levels.keys() {
        let valid = name.chars().enumerate().all(|(index, character)| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || (index > 0 && matches!(character, '.' | '-'))
        });
        if name.is_empty() || !valid {
            return Err(EnvironmentError::FeatureNameInvalid(name.clone()));
        }
    }
    Ok(())
}

fn validate_image(image: &str) -> Result<(), EnvironmentError> {
    let Some((repository, digest)) = image.split_once("@sha256:") else {
        return Err(EnvironmentError::ImageNotImmutable(image.to_owned()));
    };
    let valid_repository = !repository.is_empty()
        && !repository.contains('@')
        && !repository.chars().any(char::is_whitespace);
    let valid_digest = digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if valid_repository && valid_digest {
        Ok(())
    } else {
        Err(EnvironmentError::ImageNotImmutable(image.to_owned()))
    }
}

fn validate_paths(paths: &[String]) -> Result<(), EnvironmentError> {
    if paths.is_empty() {
        return Err(EnvironmentError::ComposeFilesEmpty);
    }
    let mut unique = BTreeSet::new();
    for value in paths {
        let path = Path::new(value);
        let escapes = path.is_absolute()
            || value.is_empty()
            || path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            });
        let extension = path.extension().and_then(std::ffi::OsStr::to_str);
        if escapes || !matches!(extension, Some("yml" | "yaml")) {
            return Err(EnvironmentError::ComposePathInvalid(value.clone()));
        }
        if !unique.insert(value) {
            return Err(EnvironmentError::ComposePathDuplicate(value.clone()));
        }
    }
    Ok(())
}

fn validate_services(cluster_size: u16, services: &[String]) -> Result<(), EnvironmentError> {
    if usize::from(cluster_size) != services.len() {
        return Err(EnvironmentError::BrokerServiceCount {
            cluster_size,
            services: services.len(),
        });
    }
    let mut unique = BTreeSet::new();
    for service in services {
        let valid = service.chars().enumerate().all(|(index, character)| {
            character.is_ascii_alphanumeric() || (index > 0 && matches!(character, '-' | '_' | '.'))
        });
        if service.is_empty() || !valid {
            return Err(EnvironmentError::BrokerServiceInvalid(service.clone()));
        }
        if !unique.insert(service) {
            return Err(EnvironmentError::BrokerServiceDuplicate(service.clone()));
        }
    }
    Ok(())
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
}
