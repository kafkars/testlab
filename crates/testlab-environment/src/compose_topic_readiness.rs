//! Topic readiness requires leaders and full in-sync replicas before client execution.

use std::collections::BTreeMap;
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::metadata::Metadata;

const ATTEMPT_MAX: Duration = Duration::from_secs(5);
const RETRY_DELAY: Duration = Duration::from_millis(100);

pub(super) fn wait(
    admin: &AdminClient<DefaultClientContext>,
    expected: &BTreeMap<String, i32>,
    replication_factor: i32,
    deadline: Instant,
) -> Result<(), String> {
    let mut diagnostic = "metadata was not fetched".to_owned();
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(format!("topics did not become ready: {diagnostic}"));
        };
        if remaining.is_zero() {
            return Err(format!("topics did not become ready: {diagnostic}"));
        }
        let timeout = remaining.min(ATTEMPT_MAX);
        match admin.inner().fetch_metadata(None, timeout) {
            Ok(metadata) => match readiness_diagnostic(&metadata, expected, replication_factor) {
                None => return Ok(()),
                Some(problem) => diagnostic = problem,
            },
            Err(error) => diagnostic = format!("metadata fetch failed: {error}"),
        }
        if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            thread::sleep(remaining.min(RETRY_DELAY));
        }
    }
}

fn readiness_diagnostic(
    metadata: &Metadata,
    expected: &BTreeMap<String, i32>,
    replication_factor: i32,
) -> Option<String> {
    let Ok(replicas) = usize::try_from(replication_factor) else {
        return Some(format!(
            "replication factor {replication_factor} cannot be represented"
        ));
    };
    for (name, partitions) in expected {
        let Some(topic) = metadata.topics().iter().find(|topic| topic.name() == name) else {
            return Some(format!("topic {name} is missing from metadata"));
        };
        if let Some(error) = topic.error() {
            return Some(format!("topic {name} metadata failed: {error:?}"));
        }
        let Ok(expected_partitions) = usize::try_from(*partitions) else {
            return Some(format!(
                "topic {name} partition count {partitions} cannot be represented"
            ));
        };
        if topic.partitions().len() != expected_partitions {
            return Some(format!(
                "topic {name} has {} of {expected_partitions} partitions",
                topic.partitions().len()
            ));
        }
        for partition in topic.partitions() {
            if let Some(error) = partition.error() {
                return Some(format!(
                    "topic {name} partition {} metadata failed: {error:?}",
                    partition.id()
                ));
            }
            if partition.leader() < 0
                || partition.replicas().len() != replicas
                || partition.isr().len() != replicas
            {
                return Some(format!(
                    "topic {name} partition {} has leader {}, {} replicas, and {} in-sync replicas; expected {replicas}",
                    partition.id(),
                    partition.leader(),
                    partition.replicas().len(),
                    partition.isr().len()
                ));
            }
        }
    }
    None
}
