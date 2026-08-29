//! Broker-role targets identify independently discoverable Kafka ownership.

use serde::{Deserialize, Serialize};

/// One exact broker-owned Kafka role selected for a bounded disruption.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerRoleTarget {
    /// Leader of one exact record partition.
    PartitionLeader {
        /// Exact topic.
        topic: String,
        /// Exact nonnegative partition.
        partition: i32,
    },
    /// Active `KRaft` controller exposed by broker metadata.
    Controller,
    /// Coordinator for one exact consumer-group identity.
    GroupCoordinator {
        /// Exact consumer-group identity.
        group_id: String,
    },
    /// Coordinator for one exact transactional identity.
    TransactionCoordinator {
        /// Exact transactional identity.
        transactional_id: String,
    },
}

impl BrokerRoleTarget {
    /// Returns the stable role name used in evidence arguments.
    pub const fn role_name(&self) -> &'static str {
        match self {
            Self::PartitionLeader { .. } => "partition_leader",
            Self::Controller => "controller",
            Self::GroupCoordinator { .. } => "group_coordinator",
            Self::TransactionCoordinator { .. } => "transaction_coordinator",
        }
    }

    /// Returns the stable target arguments that follow the role name in evidence.
    pub fn evidence_target(&self) -> Vec<String> {
        match self {
            Self::PartitionLeader { topic, partition } => {
                vec![topic.clone(), partition.to_string()]
            }
            Self::Controller => Vec::new(),
            Self::GroupCoordinator { group_id } => vec![group_id.clone()],
            Self::TransactionCoordinator { transactional_id } => {
                vec![transactional_id.clone()]
            }
        }
    }
}
