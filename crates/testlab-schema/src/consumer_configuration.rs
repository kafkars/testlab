//! Portable Fetch and retained-delivery policy is shared by assigned and group consumers.

use serde::{Deserialize, Serialize};

use crate::ClientId;

const DEFAULT_PARTITION_MAX_BYTES: u64 = 1_048_576;
const DEFAULT_BUFFERED_BYTES: u64 = 8_388_608;
const DEFAULT_MAX_BATCH_BYTES: u64 = 1_048_576;

/// Public transactional visibility selected for a directly assigned consumer.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedConsumerReadIsolation {
    /// Deliver committed, aborted, and unresolved transaction records.
    #[default]
    ReadUncommitted,
    /// Deliver only nontransactional and committed transaction records.
    ReadCommitted,
}

/// Public batch observation selected for one directly assigned receive.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignedConsumerReceiveMethod {
    /// Wait on the named retained-delivery observer.
    #[default]
    Recv,
    /// Repeatedly attempt the immediate retained-batch take operation.
    TryTakeBatch,
}

/// Broker long-poll and byte policy fixed before a consumer starts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerFetchConfiguration {
    /// Broker maximum long-poll interval.
    pub max_wait_ms: u64,
    /// Preferred minimum response bytes.
    pub min_bytes: u64,
    /// Soft whole-response byte ceiling.
    pub max_bytes: u64,
    /// Soft per-partition byte ceiling.
    pub partition_max_bytes: u64,
    /// End-to-end deadline for each background Fetch attempt.
    pub attempt_timeout_ms: u64,
}

/// Bounded Fetch-call and retained-delivery capacities.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerLimitsConfiguration {
    /// Maximum concurrently retained Fetch calls.
    pub in_flight_fetches: u32,
    /// Maximum retained delivery batches.
    pub buffered_batches: u32,
    /// Cumulative retained application-byte capacity.
    pub buffered_bytes: u64,
    /// Hard decoded byte ceiling for one Fetch result.
    pub max_batch_bytes: u64,
}

/// Immutable client-wide policy for one directly assigned consumer.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AssignedConsumerConfiguration {
    /// Transactional record visibility.
    pub read_isolation: AssignedConsumerReadIsolation,
    /// Optional broker Fetch policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch: Option<ConsumerFetchConfiguration>,
    /// Optional Fetch-call and retained-delivery capacities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<ConsumerLimitsConfiguration>,
}

/// Creates a client with explicit directly assigned consumer policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAssignedConsumerClientAction {
    /// New client identity.
    pub client_id: ClientId,
    /// Assigned-consumer policy fixed before client startup.
    pub configuration: AssignedConsumerConfiguration,
}

pub(crate) fn validate(
    owner: &str,
    fetch: Option<ConsumerFetchConfiguration>,
    limits: Option<ConsumerLimitsConfiguration>,
    problems: &mut Vec<String>,
) {
    if let Some(fetch) = fetch {
        positive_i32(owner, "fetch.max_wait_ms", fetch.max_wait_ms, problems);
        nonnegative_i32(owner, "fetch.min_bytes", fetch.min_bytes, problems);
        positive_i32(owner, "fetch.max_bytes", fetch.max_bytes, problems);
        positive_i32(
            owner,
            "fetch.partition_max_bytes",
            fetch.partition_max_bytes,
            problems,
        );
        positive_i32(
            owner,
            "fetch.attempt_timeout_ms",
            fetch.attempt_timeout_ms,
            problems,
        );
        if fetch.min_bytes > fetch.max_bytes {
            problems.push(format!(
                "{owner} fetch.min_bytes must not exceed fetch.max_bytes"
            ));
        }
    }
    if let Some(limits) = limits {
        positive_i32(
            owner,
            "limits.in_flight_fetches",
            u64::from(limits.in_flight_fetches),
            problems,
        );
        positive_i32(
            owner,
            "limits.buffered_batches",
            u64::from(limits.buffered_batches),
            problems,
        );
        positive_i32(
            owner,
            "limits.buffered_bytes",
            limits.buffered_bytes,
            problems,
        );
        positive_i32(
            owner,
            "limits.max_batch_bytes",
            limits.max_batch_bytes,
            problems,
        );
    }
    let partition_max_bytes = fetch
        .map(|configuration| configuration.partition_max_bytes)
        .unwrap_or(DEFAULT_PARTITION_MAX_BYTES);
    let (buffered_bytes, max_batch_bytes) = limits
        .map(|configuration| (configuration.buffered_bytes, configuration.max_batch_bytes))
        .unwrap_or((DEFAULT_BUFFERED_BYTES, DEFAULT_MAX_BATCH_BYTES));
    if max_batch_bytes > buffered_bytes {
        problems.push(format!(
            "{owner} limits.max_batch_bytes must not exceed limits.buffered_bytes"
        ));
    }
    if max_batch_bytes < partition_max_bytes {
        problems.push(format!(
            "{owner} limits.max_batch_bytes must cover fetch.partition_max_bytes"
        ));
    }
}

fn positive_i32(owner: &str, field: &str, value: u64, problems: &mut Vec<String>) {
    if !(1..=i32::MAX as u64).contains(&value) {
        problems.push(format!(
            "{owner} {field} must be between 1 and {}",
            i32::MAX
        ));
    }
}

fn nonnegative_i32(owner: &str, field: &str, value: u64, problems: &mut Vec<String>) {
    if value > i32::MAX as u64 {
        problems.push(format!("{owner} {field} must be at most {}", i32::MAX));
    }
}

#[cfg(test)]
#[path = "assigned_consumer_configuration_test.rs"]
mod tests;
