//! Compose provisions declared client-metrics resources through Kafka's pinned CLI.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use testlab_schema::{
    ConfigResourceListingApi, EnvironmentOperationKind, Scenario, ScenarioAction,
};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_types::ComposePhase;

impl DockerComposeEnvironment {
    pub(super) fn provision_client_metrics_resources(
        &mut self,
        scenario: &Scenario,
        timeout: Duration,
    ) -> ComposePhase {
        let names = resources(scenario);
        let mut phase = ComposePhase::default();
        if names.is_empty() {
            return phase;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_provision_failed",
                "client-metrics resource provisioning deadline overflow",
            );
            return phase;
        };
        let Some(service) = self.broker_services.first().cloned() else {
            phase.fail(
                "environment_provision_failed",
                "no broker service for client-metrics resource provisioning",
            );
            return phase;
        };
        for name in names {
            let operation = self.next_operation;
            let spec = compose_owned(
                EnvironmentOperationKind::BrokerProvision,
                &self.prefix,
                command(&service, self.client_port, &name),
                format!("client-metrics-provision-{operation:05}.txt"),
                format!("client-metrics-provision-{operation:05}.stderr.txt"),
            );
            if !self.required(&mut phase, spec, deadline) {
                break;
            }
        }
        phase
    }
}

pub(super) fn resources(scenario: &Scenario) -> BTreeSet<String> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::ListConfigResources(action)
                if action.api == ConfigResourceListingApi::ClientMetrics =>
            {
                Some(action.required_resources.iter().cloned())
            }
            _ => None,
        })
        .flatten()
        .collect()
}

fn command(service: &str, client_port: u16, name: &str) -> Vec<String> {
    vec![
        "exec".to_owned(),
        "--no-TTY".to_owned(),
        service.to_owned(),
        "/opt/kafka/bin/kafka-client-metrics.sh".to_owned(),
        "--bootstrap-server".to_owned(),
        format!("localhost:{client_port}"),
        "--alter".to_owned(),
        "--name".to_owned(),
        name.to_owned(),
        "--metrics".to_owned(),
        "org.apache.kafka.producer.".to_owned(),
        "--interval".to_owned(),
        "30000".to_owned(),
    ]
}

#[cfg(test)]
#[path = "compose_client_metrics_resources_provision_test.rs"]
mod test;
