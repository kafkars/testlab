//! Independent Kafka observations select and track exact broker-role owners.

use std::thread;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use testlab_schema::{
    BrokerRoleTarget, EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus,
};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command;
use crate::compose_support::{elapsed_unix_ms, remaining};
use crate::compose_types::{ComposeFailure, ComposePhase};

const OBSERVE_ATTEMPT_MAX: Duration = Duration::from_secs(2);
const OBSERVE_RETRY_DELAY: Duration = Duration::from_millis(100);

impl DockerComposeEnvironment {
    /// Stops one independently observed role owner and proves a distinct replacement.
    pub fn stop_broker_role(
        &mut self,
        target: &BrokerRoleTarget,
        timeout: Duration,
    ) -> ComposePhase {
        let mut phase = ComposePhase::default();
        if self.cluster_size < 3 || self.stopped_roles.contains_key(target) {
            phase.fail(
                "environment_role_disruption_invalid",
                format!("cannot stop broker role {target:?}"),
            );
            return phase;
        }
        if !matches!(target, BrokerRoleTarget::PartitionLeader { .. })
            && !self.client_security.supports_plaintext_wire()
        {
            phase.fail(
                "environment_role_observation_unsupported",
                "controller and coordinator targeting currently require plaintext Kafka",
            );
            return phase;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_disruption_deadline_invalid",
                "broker role stop deadline overflowed",
            );
            return phase;
        };
        let original = match self.wait_role(target, None, deadline) {
            Ok(node) => node,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return phase;
            }
        };
        let Some((ordinal, service)) = self.service_for_node(original) else {
            phase.fail(
                "environment_broker_target_invalid",
                format!("role owner node {original} is not a declared broker"),
            );
            return phase;
        };
        self.record_role(&mut phase, target, "before_stop", original, &service);
        let operation = self.next_operation;
        if !self.required(
            &mut phase,
            compose_command::stop(&self.prefix, &service, operation),
            deadline,
        ) {
            return phase;
        }
        let replacement = match self.wait_role(target, Some(original), deadline) {
            Ok(node) => node,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return phase;
            }
        };
        let Some((_, replacement_service)) = self.service_for_node(replacement) else {
            phase.fail(
                "environment_broker_target_invalid",
                format!("replacement role owner node {replacement} is not a declared broker"),
            );
            return phase;
        };
        self.record_role(
            &mut phase,
            target,
            "after_election",
            replacement,
            &replacement_service,
        );
        if phase.succeeded() {
            self.stopped_roles.insert(target.clone(), ordinal);
        }
        phase
    }

    /// Restarts the exact broker retained by a prior role stop.
    pub fn restore_broker_role(
        &mut self,
        target: &BrokerRoleTarget,
        timeout: Duration,
    ) -> ComposePhase {
        let mut phase = ComposePhase::default();
        let Some(ordinal) = self.stopped_roles.get(target).copied() else {
            phase.fail(
                "environment_role_disruption_invalid",
                format!("no stopped owner is retained for {target:?}"),
            );
            return phase;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_disruption_deadline_invalid",
                "broker role restore deadline overflowed",
            );
            return phase;
        };
        let Some(service) = self
            .broker_services
            .get(usize::from(ordinal.saturating_sub(1)))
            .cloned()
        else {
            phase.fail(
                "environment_broker_target_invalid",
                "stopped broker role owner was lost",
            );
            return phase;
        };
        let operation = self.next_operation;
        if self.required(
            &mut phase,
            compose_command::start(&self.prefix, &service, operation),
            deadline,
        ) {
            self.wait_restart_ready(&mut phase, &service, operation, deadline);
        }
        if phase.succeeded() {
            self.stopped_roles.remove(target);
        }
        phase
    }

    fn wait_role(
        &self,
        target: &BrokerRoleTarget,
        excluded: Option<i32>,
        deadline: Instant,
    ) -> Result<i32, ComposeFailure> {
        loop {
            let timeout = remaining(deadline).min(OBSERVE_ATTEMPT_MAX);
            let result = match target {
                BrokerRoleTarget::PartitionLeader { topic, partition } => {
                    self.partition_leader(topic, *partition, timeout)
                }
                BrokerRoleTarget::Controller => self.wire_role(timeout, |endpoint, timeout| {
                    crate::kafka_role_wire::controller(endpoint, timeout)
                }),
                BrokerRoleTarget::GroupCoordinator { group_id } => {
                    self.wire_role(timeout, |endpoint, timeout| {
                        crate::kafka_role_wire::coordinator(endpoint, group_id, 0, timeout)
                    })
                }
                BrokerRoleTarget::TransactionCoordinator { transactional_id } => {
                    self.wire_role(timeout, |endpoint, timeout| {
                        crate::kafka_role_wire::coordinator(endpoint, transactional_id, 1, timeout)
                    })
                }
            };
            if let Ok(node) = result
                && Some(node) != excluded
                && self.service_for_node(node).is_some()
            {
                return Ok(node);
            }
            if remaining(deadline).is_zero() {
                return Err(ComposeFailure::new(
                    "environment_role_observation_failed",
                    format!("no eligible owner was observed for {target:?}"),
                ));
            }
            thread::sleep(OBSERVE_RETRY_DELAY.min(remaining(deadline)));
        }
    }

    fn wire_role(
        &self,
        timeout: Duration,
        query: impl Fn(&str, Duration) -> Result<i32, String>,
    ) -> Result<i32, String> {
        let per_endpoint = timeout.min(Duration::from_millis(500));
        let mut diagnostic = "no broker endpoints were available".to_owned();
        for endpoint in self.endpoints() {
            match query(&endpoint, per_endpoint) {
                Ok(node) => return Ok(node),
                Err(error) => diagnostic = error,
            }
        }
        Err(diagnostic)
    }

    fn partition_leader(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> Result<i32, String> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", self.endpoints().join(","))
            .set("group.id", format!("testlab-role-{}", self.run_id));
        self.client_security.configure(&mut config);
        let consumer: BaseConsumer = config.create().map_err(|error| error.to_string())?;
        let metadata = consumer
            .fetch_metadata(Some(topic), timeout)
            .map_err(|error| error.to_string())?;
        let topic = metadata
            .topics()
            .iter()
            .find(|candidate| candidate.name() == topic)
            .ok_or_else(|| "metadata omitted the topic".to_owned())?;
        let partition = topic
            .partitions()
            .iter()
            .find(|candidate| candidate.id() == partition)
            .ok_or_else(|| "metadata omitted the partition".to_owned())?;
        (partition.leader() >= 0)
            .then_some(partition.leader())
            .ok_or_else(|| "metadata did not expose a partition leader".to_owned())
    }

    fn service_for_node(&self, node: i32) -> Option<(u16, String)> {
        let ordinal = u16::try_from(node).ok()?;
        let service = self
            .broker_services
            .get(usize::from(ordinal.checked_sub(1)?))?
            .clone();
        Some((ordinal, service))
    }

    fn record_role(
        &mut self,
        phase: &mut ComposePhase,
        target: &BrokerRoleTarget,
        stage: &str,
        node: i32,
        service: &str,
    ) {
        let Ok(id) = self.operation_id() else {
            phase.fail(
                "environment_operation_overflow",
                "broker role observation id overflowed",
            );
            return;
        };
        let now = elapsed_unix_ms(self.started_unix_ms, self.started.elapsed());
        let mut args = vec![target.role_name().to_owned()];
        args.extend(target.evidence_target());
        args.extend([stage.to_owned(), node.to_string(), service.to_owned()]);
        phase.operations.push(EnvironmentOperation {
            id,
            kind: EnvironmentOperationKind::BrokerRoleObserve,
            program: "testlab-kafka-role-observer/1".to_owned(),
            args,
            started_unix_ms: now,
            completed_unix_ms: now,
            status: EnvironmentOperationStatus::Succeeded,
            exit_code: None,
            stdout_artifact: None,
            stderr_artifact: None,
            diagnostic: None,
        });
    }
}
