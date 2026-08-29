//! Compose construction reserves isolated public, upstream, and observer routes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Instant;

use testlab_schema::{EnvironmentDriver, TransportSecurity};

use crate::compose::DockerComposeEnvironment;
use crate::compose_ports::HostPorts;
use crate::compose_support::{compose_prefix, project_name};
use crate::compose_types::{ComposeFailure, ComposeRequest};
use crate::security::ClientSecurity;

impl DockerComposeEnvironment {
    /// Resolves an immutable manifest into a side-effect-free lifecycle owner.
    pub fn new(request: ComposeRequest<'_>) -> Result<Self, ComposeFailure> {
        request.environment.validate().map_err(|error| {
            ComposeFailure::new("environment_manifest_invalid", error.to_string())
        })?;
        let EnvironmentDriver::DockerCompose {
            broker_services, ..
        } = &request.environment.driver
        else {
            return Err(invalid_driver());
        };
        let host_ports = HostPorts::reserve(broker_services.len())?;
        Self::with_program(request, PathBuf::from("docker"), host_ports, None)
    }

    #[cfg(test)]
    pub(crate) fn new_with_program(
        request: ComposeRequest<'_>,
        program: PathBuf,
        host_port: u16,
    ) -> Result<Self, ComposeFailure> {
        let (count, network_proxy) = match &request.environment.driver {
            EnvironmentDriver::DockerCompose {
                broker_services,
                network_proxy,
                ..
            } => (broker_services.len(), *network_proxy),
            EnvironmentDriver::ModelBroker | EnvironmentDriver::KafkaProtocolAdversary { .. } => {
                (0, false)
            }
        };
        let proxy_ports = test_proxy_ports(network_proxy, host_port, count)?;
        Self::with_program(
            request,
            program,
            HostPorts::fixed(host_port, count)?,
            proxy_ports,
        )
    }

    fn with_program(
        request: ComposeRequest<'_>,
        program: PathBuf,
        host_ports: HostPorts,
        proxy_ports: Option<(HostPorts, HostPorts)>,
    ) -> Result<Self, ComposeFailure> {
        request.environment.validate().map_err(|error| {
            ComposeFailure::new("environment_manifest_invalid", error.to_string())
        })?;
        let EnvironmentDriver::DockerCompose {
            image,
            cluster_size,
            security,
            compose_files,
            broker_services,
            client_port,
            feature_levels,
            network_proxy,
            ..
        } = &request.environment.driver
        else {
            return Err(invalid_driver());
        };
        let security_directory = (security.transport == TransportSecurity::TlsCustom).then(|| {
            request
                .repository_root
                .join("target/testlab-security")
                .join(request.run_id.as_str())
        });
        let ca_pem = security_directory.as_ref().map(|path| path.join("ca.pem"));
        let client_security = ClientSecurity::new(*security, ca_pem.as_deref())?;
        let mut environment = client_security.compose_environment(
            image,
            host_ports.as_slice(),
            security_directory.as_deref(),
        );
        let (backend_ports, observer_ports, proxy_program) = proxy_routes(
            *network_proxy,
            broker_services.len(),
            proxy_ports,
            &mut environment,
        )?;
        Ok(Self {
            repository_root: request.repository_root.to_path_buf(),
            run_id: request.run_id.clone(),
            program,
            prefix: compose_prefix(&project_name(request.run_id), compose_files),
            environment,
            client_security,
            broker_services: broker_services.clone(),
            client_port: *client_port,
            cluster_size: *cluster_size,
            feature_levels: feature_levels.clone(),
            host_ports,
            backend_ports,
            observer_ports,
            proxy_program,
            network_proxy: None,
            started_unix_ms: request.started_unix_ms,
            started: Instant::now(),
            next_operation: 0,
            next_state_observation: 0,
            observed_admin_operations: BTreeSet::new(),
            stopped_roles: BTreeMap::new(),
            stopped_brokers: Vec::new(),
            active_broker_policies: BTreeSet::new(),
            up_attempted: false,
        })
    }
}

type ProxyRoutes = (Option<HostPorts>, Option<HostPorts>, Option<PathBuf>);

fn proxy_routes(
    enabled: bool,
    count: usize,
    supplied: Option<(HostPorts, HostPorts)>,
    environment: &mut Vec<(String, String)>,
) -> Result<ProxyRoutes, ComposeFailure> {
    if !enabled {
        return Ok((None, None, None));
    }
    let (backend, observer) = match supplied {
        Some(ports) => ports,
        None => (HostPorts::reserve(count)?, HostPorts::reserve(count)?),
    };
    backend.append_named(environment, "KAFKA_BACKEND_HOST_PORT");
    observer.append_named(environment, "KAFKA_OBSERVER_HOST_PORT");
    let program = std::env::current_exe().map_err(|error| {
        ComposeFailure::new(
            "environment_program_missing",
            format!("failed to locate network proxy worker: {error}"),
        )
    })?;
    Ok((Some(backend), Some(observer), Some(program)))
}

#[cfg(test)]
fn test_proxy_ports(
    enabled: bool,
    first: u16,
    count: usize,
) -> Result<Option<(HostPorts, HostPorts)>, ComposeFailure> {
    if !enabled {
        return Ok(None);
    }
    let backend = first.checked_add(1_000).ok_or_else(test_port_overflow)?;
    let observer = first.checked_add(2_000).ok_or_else(test_port_overflow)?;
    Ok(Some((
        HostPorts::fixed(backend, count)?,
        HostPorts::fixed(observer, count)?,
    )))
}

#[cfg(test)]
fn test_port_overflow() -> ComposeFailure {
    ComposeFailure::new(
        "environment_host_port_invalid",
        "test network proxy port overflowed",
    )
}

fn invalid_driver() -> ComposeFailure {
    ComposeFailure::new(
        "environment_driver_invalid",
        "environment does not use the Docker Compose driver",
    )
}
