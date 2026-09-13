//! Delegation-token lifecycle payloads retain public facts without secret bytes.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

#[path = "admin_delegation_token_validation.rs"]
pub(crate) mod validation;

#[cfg(test)]
#[path = "admin_delegation_token_test.rs"]
mod tests;

/// One exact Kafka principal used by delegation-token administration.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationTokenPrincipalSpec {
    /// Exact Kafka principal type.
    pub principal_type: String,
    /// Exact Kafka principal name.
    pub principal_name: String,
}

impl DelegationTokenPrincipalSpec {
    /// Returns Kafka's conventional type-and-name representation.
    pub fn kafka_name(&self) -> String {
        format!("{}:{}", self.principal_type, self.principal_name)
    }
}

/// Scenario intent for one bounded create, describe, renew, and expire sequence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExerciseDelegationTokenLifecycleAction {
    /// Existing authenticated client whose Admin handle is used.
    pub client_id: ClientId,
    /// Stable identity for the composite public operation.
    pub operation_id: OperationId,
    /// Explicit token owner.
    pub owner: DelegationTokenPrincipalSpec,
    /// Caller-ordered token renewers.
    pub renewers: Vec<DelegationTokenPrincipalSpec>,
    /// Requested maximum token lifetime.
    pub max_lifetime_ms: u64,
    /// Requested renewal period.
    pub renew_period_ms: u64,
    /// Explicit public expiration delay, or none for Kafka's immediate sentinel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_ms: Option<u64>,
    /// Complete four-operation public bound.
    pub timeout_ms: u64,
}

/// Wire payload for one bounded delegation-token lifecycle.
pub type ExerciseDelegationTokenLifecycleCommand = ExerciseDelegationTokenLifecycleAction;

/// Non-secret public facts from one complete delegation-token lifecycle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminDelegationTokenLifecycle {
    /// Stable identity for the composite public operation.
    pub operation_id: OperationId,
    /// Owner returned by token creation.
    pub owner: DelegationTokenPrincipalSpec,
    /// Requester returned by the selected Kafka response version.
    pub requester: Option<DelegationTokenPrincipalSpec>,
    /// Renewers returned by token creation in public order.
    pub renewers: Vec<DelegationTokenPrincipalSpec>,
    /// Exact non-secret token identity.
    pub token_id: String,
    /// Token issue epoch timestamp.
    pub issue_timestamp_ms: i64,
    /// Initial token expiry epoch timestamp.
    pub initial_expiry_timestamp_ms: i64,
    /// Maximum token epoch timestamp.
    pub max_timestamp_ms: i64,
    /// Expiry epoch timestamp returned by renewal.
    pub renewed_expiry_timestamp_ms: i64,
    /// Expiry epoch timestamp returned by immediate expiration.
    pub expired_at_timestamp_ms: i64,
    /// Number of secret bytes retained only inside the adapter.
    pub hmac_size: usize,
    /// Whether the owner-filtered description exactly matched the created token.
    pub description_matched: bool,
    /// Nonnegative create-operation throttle.
    pub create_throttle_time_ms: u64,
    /// Nonnegative describe-operation throttle.
    pub describe_throttle_time_ms: u64,
    /// Nonnegative renew-operation throttle.
    pub renew_throttle_time_ms: u64,
    /// Nonnegative expire-operation throttle.
    pub expire_throttle_time_ms: u64,
}

/// Independent owner-filtered token count from Kafka's pinned CLI.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerDelegationTokensState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable identity for the composite public operation.
    pub operation_id: OperationId,
    /// Exact owner selection used by the independent CLI.
    pub owner: DelegationTokenPrincipalSpec,
    /// Number of live tokens returned for that owner.
    pub token_count: u32,
}
