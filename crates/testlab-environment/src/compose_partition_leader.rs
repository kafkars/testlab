//! Independent metadata selects the exact leader kept offline during client work.

use std::thread;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use testlab_schema::{EnvironmentOperation, EnvironmentOperationKind, EnvironmentOperationStatus};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command;
use crate::compose_support::{elapsed_unix_ms, remaining};
use crate::compose_types::{ComposeFailure, ComposePhase};

const METADATA_ATTEMPT_MAX: Duration = Duration::from_secs(3);
const METADATA_RETRY_DELAY: Duration = Duration::from_millis(100);

impl DockerComposeEnvironment {
    /// Stops the exact independently observed leader and proves a distinct replacement.
    pub fn stop_partition_leader(
        &mut self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> ComposePhase {
        let mut phase = ComposePhase::default();
        let key = (topic.to_owned(), partition);
        if self.cluster_size < 2 || self.stopped_partition_leaders.contains_key(&key) {
            phase.fail(
                "environment_partition_disruption_invalid",
                format!("cannot stop partition leader for {topic}:{partition}"),
            );
            return phase;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_disruption_deadline_invalid",
                "leader stop overflowed",
            );
            return phase;
        };
        let original = match self.wait_leader(topic, partition, None, deadline) {
            Ok(leader) => leader,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return phase;
            }
        };
        let Some((ordinal, service)) = self.service_for_node(original) else {
            phase.fail(
                "environment_broker_target_invalid",
                format!("leader node {original} is not a declared broker"),
            );
            return phase;
        };
        let operation = self.next_operation;
        if !self.required(
            &mut phase,
            compose_command::stop(&self.prefix, &service, operation),
            deadline,
        ) {
            return phase;
        }
        let replacement = match self.wait_leader(topic, partition, Some(original), deadline) {
            Ok(leader) => leader,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return phase;
            }
        };
        self.record_leaders(&mut phase, topic, partition, original, replacement);
        self.stopped_partition_leaders.insert(key, ordinal);
        phase
    }

    /// Restarts the exact broker retained by a prior leader-stop action.
    pub fn restore_partition_leader(
        &mut self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> ComposePhase {
        let mut phase = ComposePhase::default();
        let key = (topic.to_owned(), partition);
        let Some(ordinal) = self.stopped_partition_leaders.get(&key).copied() else {
            phase.fail(
                "environment_partition_disruption_invalid",
                format!("no stopped leader is retained for {topic}:{partition}"),
            );
            return phase;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_disruption_deadline_invalid",
                "leader start overflowed",
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
                "stopped broker was lost",
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
            self.stopped_partition_leaders.remove(&key);
        }
        phase
    }

    fn wait_leader(
        &self,
        topic: &str,
        partition: i32,
        excluded: Option<i32>,
        deadline: Instant,
    ) -> Result<i32, ComposeFailure> {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", self.endpoints().join(","))
            .set("group.id", format!("testlab-leader-{}", self.run_id));
        self.client_security.configure(&mut config);
        let consumer: BaseConsumer = config.create().map_err(|error| {
            ComposeFailure::new("environment_leader_observation_failed", error.to_string())
        })?;
        loop {
            let timeout = remaining(deadline).min(METADATA_ATTEMPT_MAX);
            let diagnostic = match probe(&consumer, topic, partition, timeout) {
                Ok(Some(leader)) if Some(leader) != excluded => return Ok(leader),
                Ok(_) => "leader election retained the stopped node".to_owned(),
                Err(error) => error,
            };
            if remaining(deadline).is_zero() {
                return Err(ComposeFailure::new(
                    "environment_leader_observation_failed",
                    format!("{topic}:{partition}: {diagnostic}"),
                ));
            }
            thread::sleep(METADATA_RETRY_DELAY.min(remaining(deadline)));
        }
    }

    fn service_for_node(&self, node: i32) -> Option<(u16, String)> {
        let ordinal = u16::try_from(node).ok()?;
        let service = self
            .broker_services
            .get(usize::from(ordinal.checked_sub(1)?))?
            .clone();
        Some((ordinal, service))
    }

    fn record_leaders(
        &mut self,
        phase: &mut ComposePhase,
        topic: &str,
        partition: i32,
        original: i32,
        replacement: i32,
    ) {
        let Ok(id) = self.operation_id() else {
            phase.fail(
                "environment_operation_overflow",
                "leader observation id overflowed",
            );
            return;
        };
        let now = elapsed_unix_ms(self.started_unix_ms, self.started.elapsed());
        let (_, version) = rdkafka::util::get_rdkafka_version();
        phase.operations.push(EnvironmentOperation {
            id,
            kind: EnvironmentOperationKind::BrokerLeaderObserve,
            program: format!("librdkafka/{version}"),
            args: vec![
                topic.to_owned(),
                partition.to_string(),
                original.to_string(),
                replacement.to_string(),
            ],
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

fn probe(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> Result<Option<i32>, String> {
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
    Ok((partition.leader() >= 0).then_some(partition.leader()))
}
