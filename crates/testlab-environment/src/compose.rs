//! Docker Compose lifecycle owns immutable setup, readiness, snapshots, and cleanup.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use testlab_schema::{AdapterSecurity, BrokerRoleTarget, OperationId, RunId};

use crate::compose_command::{self, CommandSpec};
use crate::compose_ports::HostPorts;
use crate::compose_support::{failure_code, remaining};
use crate::compose_types::ComposePhase;
use crate::network_proxy_process_types::RunningNetworkProxy;
use crate::security::ClientSecurity;

/// One isolated Compose project that must be explicitly finished for evidence.
#[must_use = "call finish so owned containers, networks, and volumes are removed"]
pub struct DockerComposeEnvironment {
    pub(super) repository_root: PathBuf,
    pub(super) run_id: RunId,
    pub(super) program: PathBuf,
    pub(super) prefix: Vec<String>,
    pub(super) environment: Vec<(String, String)>,
    pub(super) client_security: ClientSecurity,
    pub(super) broker_services: Vec<String>,
    pub(super) client_port: u16,
    pub(super) cluster_size: u16,
    pub(super) feature_levels: BTreeMap<String, u16>,
    pub(super) host_ports: HostPorts,
    pub(super) backend_ports: Option<HostPorts>,
    pub(super) observer_ports: Option<HostPorts>,
    pub(super) proxy_program: Option<PathBuf>,
    pub(super) network_proxy: Option<RunningNetworkProxy>,
    pub(super) started_unix_ms: u64,
    pub(super) started: Instant,
    pub(super) next_operation: u32,
    pub(super) next_state_observation: u64,
    pub(super) observed_admin_operations: BTreeSet<OperationId>,
    pub(super) stopped_roles: BTreeMap<BrokerRoleTarget, u16>,
    pub(super) stopped_brokers: Vec<u16>,
    pub(super) active_broker_policies: BTreeSet<testlab_schema::BrokerPolicy>,
    pub(super) up_attempted: bool,
}

impl Debug for DockerComposeEnvironment {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DockerComposeEnvironment")
            .field("run_id", &self.run_id)
            .field("project", &self.prefix.get(2))
            .field("host_ports", &self.host_ports.as_slice())
            .field("up_attempted", &self.up_attempted)
            .finish_non_exhaustive()
    }
}

impl DockerComposeEnvironment {
    /// Returns the independent observer bootstrap endpoint.
    pub fn endpoint(&self) -> String {
        self.observer_ports
            .as_ref()
            .unwrap_or(&self.host_ports)
            .endpoint()
    }

    /// Returns every independent observer endpoint in broker order.
    pub fn endpoints(&self) -> Vec<String> {
        self.observer_ports
            .as_ref()
            .unwrap_or(&self.host_ports)
            .endpoints()
    }

    /// Returns adapter-facing endpoints, including every proxy route when enabled.
    pub fn adapter_endpoints(&self) -> Vec<String> {
        self.host_ports.endpoints()
    }

    /// Returns the non-secret connection policy sent in the adapter handshake.
    pub fn adapter_security(&self) -> AdapterSecurity {
        self.client_security.adapter_security()
    }

    /// Returns ephemeral secret values passed only in the adapter process environment.
    pub fn adapter_environment(&self) -> Vec<(String, String)> {
        self.client_security.adapter_environment()
    }

    /// Captures final state and logs before removing all project resources.
    pub fn finish(mut self, timeout: Duration) -> ComposePhase {
        let mut phase = ComposePhase::default();
        if !self.up_attempted {
            return phase;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_cleanup_deadline_invalid",
                "cleanup deadline overflowed",
            );
            return phase;
        };
        if self.network_proxy.is_some() {
            let proxy = self.finish_network_proxy(remaining(deadline) / 4);
            phase.operations.extend(proxy.phase.operations);
            phase.artifacts.extend(proxy.phase.artifacts);
            if let Some(failure) = proxy.phase.failure {
                phase.fail(failure.code, failure.diagnostic);
            }
            if !proxy.observations.is_empty() {
                phase.fail(
                    "network_proxy_observations_uncollected",
                    "network proxy observations were not recorded before cleanup",
                );
            }
        }
        let commands = [
            (
                compose_command::ps(&self.prefix),
                "environment_snapshot_failed",
            ),
            (
                compose_command::logs(&self.prefix, &self.broker_services),
                "environment_logs_failed",
            ),
            (
                compose_command::down(&self.prefix),
                "environment_cleanup_failed",
            ),
        ];
        for (index, (spec, code)) in commands.into_iter().enumerate() {
            let remaining_commands = u32::try_from(3 - index).unwrap_or(1);
            let timeout = remaining(deadline) / remaining_commands;
            match self.execute(spec, timeout) {
                Ok(output) => {
                    if !phase.retain(output) {
                        phase.fail(code, format!("{code} terminal operation failed"));
                    }
                }
                Err(error) => phase.fail(error.code, error.diagnostic),
            }
        }
        phase
    }

    pub(super) fn required(
        &mut self,
        phase: &mut ComposePhase,
        spec: CommandSpec,
        deadline: Instant,
    ) -> bool {
        let code = failure_code(spec.kind);
        match self.execute(spec, remaining(deadline)) {
            Ok(output) => {
                let diagnostic = output.operation.diagnostic.clone();
                if phase.retain(output) {
                    true
                } else {
                    phase.fail(code, diagnostic.unwrap_or_else(|| format!("{code} failed")));
                    false
                }
            }
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                false
            }
        }
    }
}
