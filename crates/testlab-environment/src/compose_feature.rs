//! Broker feature setup applies explicit manifest authority after readiness.

use std::time::Instant;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command;
use crate::compose_types::ComposePhase;

impl DockerComposeEnvironment {
    pub(super) fn prepare_broker_features(
        &mut self,
        phase: &mut ComposePhase,
        deadline: Instant,
    ) -> bool {
        let Some(service) = self.broker_services.first().cloned() else {
            phase.fail(
                "environment_feature_setup_failed",
                "broker feature setup requires one broker service",
            );
            return false;
        };
        for (name, level) in self.feature_levels.clone() {
            let spec = compose_command::feature_setup(
                &self.prefix,
                &service,
                self.client_port,
                &name,
                level,
            );
            if !self.required(phase, spec, deadline) {
                return false;
            }
        }
        true
    }
}
