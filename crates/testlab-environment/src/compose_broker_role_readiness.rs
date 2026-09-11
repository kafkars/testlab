//! Restored partition owners rejoin the full ISR before a scenario continues.

use std::collections::BTreeSet;
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use testlab_schema::BrokerRoleTarget;

use crate::compose::DockerComposeEnvironment;
use crate::compose_support::remaining;
use crate::compose_types::ComposeFailure;

const OBSERVE_ATTEMPT_MAX: Duration = Duration::from_secs(2);
const OBSERVE_RETRY_DELAY: Duration = Duration::from_millis(100);

impl DockerComposeEnvironment {
    pub(super) fn wait_partition_replica_restored(
        &self,
        target: &BrokerRoleTarget,
        restored_node: i32,
        deadline: Instant,
    ) -> Result<i32, ComposeFailure> {
        let BrokerRoleTarget::PartitionLeader { topic, partition } = target else {
            return Err(invalid(
                "restored replica readiness requires a partition target",
            ));
        };
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", self.endpoints().join(","))
            .set("group.id", format!("testlab-restore-{}", self.run_id));
        self.client_security.configure(&mut config);
        let consumer: BaseConsumer = config
            .create()
            .map_err(|error| invalid(format!("create metadata observer: {error}")))?;
        loop {
            let timeout = remaining(deadline).min(OBSERVE_ATTEMPT_MAX);
            if let Ok(metadata) = consumer.fetch_metadata(Some(topic), timeout)
                && let Some(metadata_topic) = metadata
                    .topics()
                    .iter()
                    .find(|candidate| candidate.name() == topic)
                && let Some(metadata_partition) = metadata_topic
                    .partitions()
                    .iter()
                    .find(|candidate| candidate.id() == *partition)
                && partition_replica_ready(
                    metadata_partition.leader(),
                    metadata_partition.replicas(),
                    metadata_partition.isr(),
                    restored_node,
                )
            {
                return Ok(metadata_partition.leader());
            }
            if remaining(deadline).is_zero() {
                return Err(invalid(format!(
                    "restored broker {restored_node} did not rejoin the full ISR for {topic}-{partition}"
                )));
            }
            thread::sleep(OBSERVE_RETRY_DELAY.min(remaining(deadline)));
        }
    }
}

pub(super) fn partition_replica_ready(
    leader: i32,
    replicas: &[i32],
    in_sync_replicas: &[i32],
    restored_node: i32,
) -> bool {
    let replicas = replicas.iter().copied().collect::<BTreeSet<_>>();
    let in_sync_replicas = in_sync_replicas.iter().copied().collect::<BTreeSet<_>>();
    leader >= 0
        && restored_node >= 0
        && !replicas.is_empty()
        && replicas.len() == in_sync_replicas.len()
        && replicas.contains(&leader)
        && replicas.contains(&restored_node)
        && replicas == in_sync_replicas
}

fn invalid(detail: impl Into<String>) -> ComposeFailure {
    ComposeFailure::new("environment_role_restoration_failed", detail)
}
