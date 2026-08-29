//! Compose proxy integration keeps adapter traffic separate from observer traffic.

use std::time::{Duration, Instant};

use testlab_schema::{NetworkProxyControl, NetworkProxyRoute};

use crate::compose::DockerComposeEnvironment;
use crate::compose_ports::HostPorts;
use crate::compose_support::{elapsed_unix_ms, remaining};
use crate::compose_types::{ComposeFailure, ComposePhase};
use crate::network_proxy_process_types::{
    NetworkProxyFinish, NetworkProxyProcessRequest, RunningNetworkProxy,
};

impl DockerComposeEnvironment {
    pub(super) fn reassign_compose_ports(&mut self) -> Result<(), ComposeFailure> {
        let count = self.broker_services.len();
        if self.backend_ports.is_some() {
            let backend = HostPorts::reserve(count)?;
            let observer = HostPorts::reserve(count)?;
            backend.apply_named(&mut self.environment, "KAFKA_BACKEND_HOST_PORT")?;
            observer.apply_named(&mut self.environment, "KAFKA_OBSERVER_HOST_PORT")?;
            self.backend_ports = Some(backend);
            self.observer_ports = Some(observer);
        } else {
            let host = HostPorts::reserve(count)?;
            host.apply_to(&mut self.environment)?;
            self.host_ports = host;
        }
        Ok(())
    }

    pub(super) fn release_compose_ports(&mut self) {
        if let Some(backend) = self.backend_ports.as_mut() {
            backend.release();
            if let Some(observer) = self.observer_ports.as_mut() {
                observer.release();
            }
        } else {
            self.host_ports.release();
        }
    }

    pub(super) fn start_network_proxy(
        &mut self,
        phase: &mut ComposePhase,
        deadline: Instant,
    ) -> bool {
        let Some(program) = self.proxy_program.clone() else {
            return true;
        };
        let Some(backend) = self.backend_ports.as_ref() else {
            phase.fail(
                "network_proxy_routes_missing",
                "network proxy backend routes were not reserved",
            );
            return false;
        };
        let front = self.host_ports.endpoints();
        let back = backend.endpoints();
        if front.len() != back.len() {
            phase.fail(
                "network_proxy_routes_invalid",
                "network proxy route counts differ",
            );
            return false;
        }
        let routes = front
            .into_iter()
            .zip(back)
            .enumerate()
            .map(
                |(index, (listen_endpoint, upstream_endpoint))| NetworkProxyRoute {
                    broker_ordinal: u16::try_from(index + 1).unwrap_or(u16::MAX),
                    listen_endpoint,
                    upstream_endpoint,
                },
            )
            .collect::<Vec<_>>();
        let operation_id = match self.operation_id() {
            Ok(id) => id,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return false;
            }
        };
        self.host_ports.release();
        let start = RunningNetworkProxy::start(
            &NetworkProxyProcessRequest {
                program: &program,
                repository_root: &self.repository_root,
                routes: &routes,
                operation_id,
                started_unix_ms: elapsed_unix_ms(self.started_unix_ms, self.started.elapsed()),
            },
            remaining(deadline),
        );
        match start {
            Ok(proxy) => {
                self.network_proxy = Some(proxy);
                true
            }
            Err(failed) => {
                append_phase(phase, failed);
                false
            }
        }
    }

    /// Applies one validated proxy control and returns newly completed effects.
    pub fn control_network_proxy(
        &mut self,
        control: &NetworkProxyControl,
        timeout: Duration,
    ) -> Result<Vec<testlab_schema::NetworkProxyObservation>, ComposeFailure> {
        let proxy = self.network_proxy.as_mut().ok_or_else(|| {
            ComposeFailure::new(
                "network_proxy_control_unsupported",
                "environment does not own a running network proxy",
            )
        })?;
        proxy.control(control, timeout)?;
        Ok(proxy.take_observations())
    }

    /// Stops the external proxy before Compose removes its upstream brokers.
    pub fn finish_network_proxy(&mut self, timeout: Duration) -> NetworkProxyFinish {
        self.network_proxy
            .take()
            .map_or_else(NetworkProxyFinish::default, |proxy| proxy.finish(timeout))
    }
}

fn append_phase(target: &mut ComposePhase, source: ComposePhase) {
    target.operations.extend(source.operations);
    target.artifacts.extend(source.artifacts);
    if let Some(failure) = source.failure {
        target.fail(failure.code, failure.diagnostic);
    }
}
