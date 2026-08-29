//! Environment validation rejects ambiguous topology and trust-boundary inputs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};

use crate::{
    Authentication, BrokerIdentity, ENVIRONMENT_SCHEMA_VERSION, EnvironmentDriver,
    EnvironmentError, EnvironmentManifest, SecurityProfile, TransportSecurity,
};

pub(crate) fn validate(environment: &EnvironmentManifest) -> Result<(), EnvironmentError> {
    if environment.schema_version != ENVIRONMENT_SCHEMA_VERSION {
        return Err(EnvironmentError::UnsupportedVersion(
            environment.schema_version,
        ));
    }
    if environment.title.trim().is_empty() {
        return Err(EnvironmentError::EmptyTitle);
    }
    match &environment.driver {
        EnvironmentDriver::ModelBroker => Ok(()),
        EnvironmentDriver::KafkaProtocolAdversary { topic } => {
            crate::environment_adversary_validation::validate_topic(topic)
        }
        EnvironmentDriver::DockerCompose {
            broker,
            image,
            cluster_size,
            security,
            compose_files,
            broker_services,
            client_port,
            feature_levels,
            network_proxy,
        } => validate_compose(
            broker,
            image,
            *cluster_size,
            compose_files,
            broker_services,
            *client_port,
            feature_levels,
            *network_proxy,
            *security,
        ),
    }
}

#[allow(clippy::too_many_arguments, reason = "validated manifest fields")]
fn validate_compose(
    broker: &BrokerIdentity,
    image: &str,
    cluster_size: u16,
    compose_files: &[String],
    broker_services: &[String],
    client_port: u16,
    feature_levels: &BTreeMap<String, u16>,
    network_proxy: bool,
    security: SecurityProfile,
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
    validate_services(cluster_size, broker_services)?;
    validate_network_proxy(network_proxy, security)
}

fn validate_network_proxy(
    enabled: bool,
    security: SecurityProfile,
) -> Result<(), EnvironmentError> {
    if enabled
        && (security.transport != TransportSecurity::Plaintext
            || security.authentication != Authentication::None)
    {
        return Err(EnvironmentError::NetworkProxySecurityUnsupported);
    }
    Ok(())
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
