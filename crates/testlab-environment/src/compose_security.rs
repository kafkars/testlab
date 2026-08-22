//! Post-readiness setup materializes environment-owned client security state.

use std::fs;
use std::time::Instant;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command;
use crate::compose_types::ComposePhase;

impl DockerComposeEnvironment {
    pub(super) fn prepare_client_security(
        &mut self,
        phase: &mut ComposePhase,
        deadline: Instant,
    ) -> bool {
        let Some(service) = self.broker_services.first().cloned() else {
            phase.fail(
                "environment_security_setup_failed",
                "broker service missing",
            );
            return false;
        };
        if let Some(ca_pem) = self.client_security.ca_pem_path() {
            let Some(parent) = ca_pem.parent() else {
                phase.fail(
                    "environment_security_path_invalid",
                    format!("TLS CA path has no parent: {}", ca_pem.display()),
                );
                return false;
            };
            if let Err(error) = fs::create_dir_all(parent) {
                phase.fail(
                    "environment_security_path_failed",
                    format!("failed to create {}: {error}", parent.display()),
                );
                return false;
            }
            let spec = compose_command::copy_tls_ca(&self.prefix, &service, &ca_pem);
            if !self.required(phase, spec, deadline) {
                return false;
            }
        }
        if let Some(mechanism) = self.client_security.scram_mechanism() {
            let spec =
                compose_command::scram_setup(&self.prefix, &service, self.client_port, mechanism);
            if !self.required(phase, spec, deadline) {
                return false;
            }
        }
        true
    }
}
