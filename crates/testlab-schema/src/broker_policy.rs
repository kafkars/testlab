//! Broker-policy actions declare external ACL and quota state without embedding credentials.

use serde::{Deserialize, Serialize};

/// Exact producer authorization error exposed by the packaged Kafkars adapter.
pub const PRODUCER_TOPIC_AUTHORIZATION_ERROR_CODE: &str = "access:broker_29";
/// Exact admin topic authorization error exposed by the packaged Kafkars adapter.
pub const ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE: &str = "broker:broker_29";
/// Exact consumer-group authorization error exposed by the packaged Kafkars adapter.
pub const GROUP_AUTHORIZATION_ERROR_CODE: &str = "broker:broker_30";
/// Exact transactional-ID authorization error exposed by the packaged Kafkars adapter.
pub const TRANSACTIONAL_ID_AUTHORIZATION_ERROR_CODE: &str = "broker:broker_53";

/// One bounded request to establish or remove an exact broker policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerPolicyAction {
    /// Exact policy controlled outside the packaged adapter.
    pub policy: BrokerPolicy,
    /// Required policy state after the action.
    pub state: BrokerPolicyState,
    /// Complete alter-and-observe bound.
    pub timeout_ms: u64,
}

/// Broker policy exercised against the fixed environment-owned client principal.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "policy_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerPolicy {
    /// One literal deny ACL.
    Acl {
        /// Literal Kafka resource.
        resource: BrokerAclResource,
        /// Denied operation.
        operation: BrokerAclOperation,
    },
    /// One user byte-rate quota.
    Quota {
        /// Produce or consume direction.
        direction: BrokerQuotaDirection,
        /// Exact configured byte rate.
        bytes_per_second: u64,
        /// Smallest wall-clock window that must retain the quota.
        minimum_active_ms: u64,
    },
}

/// Literal ACL resources supported by black-box policy scenarios.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "resource_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerAclResource {
    /// One exact topic.
    Topic {
        /// Literal topic name.
        name: String,
    },
    /// One exact consumer group.
    Group {
        /// Literal group ID.
        name: String,
    },
    /// One exact transactional ID.
    TransactionalId {
        /// Literal transactional ID.
        name: String,
    },
}

/// ACL operations exercised by policy scenarios.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerAclOperation {
    /// Read one topic or group.
    Read,
    /// Write one topic or transactional ID.
    Write,
    /// Create one topic.
    Create,
}

/// User quota direction.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerQuotaDirection {
    /// Producer byte rate.
    Producer,
    /// Consumer byte rate.
    Consumer,
}

/// Desired exact state for one broker policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerPolicyState {
    /// The exact policy must exist.
    Present,
    /// The exact policy must not exist.
    Absent,
}

impl BrokerAclResource {
    /// Returns the literal resource name.
    pub fn name(&self) -> &str {
        match self {
            Self::Topic { name } | Self::Group { name } | Self::TransactionalId { name } => name,
        }
    }

    /// Returns the stable lowercase resource kind used in evidence.
    pub const fn kind_name(&self) -> &'static str {
        match self {
            Self::Topic { .. } => "topic",
            Self::Group { .. } => "group",
            Self::TransactionalId { .. } => "transactional_id",
        }
    }
}

impl BrokerAclOperation {
    /// Returns Kafka CLI spelling.
    pub const fn cli_name(self) -> &'static str {
        match self {
            Self::Read => "Read",
            Self::Write => "Write",
            Self::Create => "Create",
        }
    }

    /// Returns stable lowercase evidence spelling.
    pub const fn evidence_name(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Create => "create",
        }
    }
}

impl BrokerQuotaDirection {
    /// Returns the Kafka user quota configuration key.
    pub const fn config_name(self) -> &'static str {
        match self {
            Self::Producer => "producer_byte_rate",
            Self::Consumer => "consumer_byte_rate",
        }
    }

    /// Returns stable evidence spelling.
    pub const fn evidence_name(self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }
}

impl BrokerPolicyState {
    /// Returns stable evidence spelling.
    pub const fn evidence_name(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Absent => "absent",
        }
    }
}

impl BrokerPolicy {
    /// Returns the normalized non-secret fact retained after a successful query.
    pub fn evidence_args(&self, state: BrokerPolicyState) -> Vec<String> {
        match self {
            Self::Acl {
                resource,
                operation,
            } => vec![
                "acl".to_owned(),
                "User:kafkars".to_owned(),
                resource.kind_name().to_owned(),
                resource.name().to_owned(),
                operation.evidence_name().to_owned(),
                state.evidence_name().to_owned(),
            ],
            Self::Quota {
                direction,
                bytes_per_second,
                minimum_active_ms,
            } => vec![
                "quota".to_owned(),
                "kafkars".to_owned(),
                direction.evidence_name().to_owned(),
                bytes_per_second.to_string(),
                minimum_active_ms.to_string(),
                state.evidence_name().to_owned(),
            ],
        }
    }
}
