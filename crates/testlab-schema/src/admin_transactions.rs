//! Transaction Admin payloads keep fixture expectations outside wire commands.

use serde::{Deserialize, Serialize};

use crate::{ClientId, OperationId};

/// One exact expected row in a canonical cluster transaction listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionListingExpectation {
    /// Exact transactional identifier.
    pub transactional_id: String,
    /// Exact Kafka-owned transaction-state spelling.
    pub expected_state: String,
}

/// Scenario intent for one unfiltered cluster-wide transaction listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListTransactionsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact canonical transaction identities and states expected in the fixture.
    pub expected_transactions: Vec<TransactionListingExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one unfiltered cluster-wide transaction listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListTransactionsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Exact public transaction listing fields shared with independent evidence.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionListingSnapshot {
    /// Exact transactional identifier.
    pub transactional_id: String,
    /// Exact nonnegative producer identity.
    pub producer_id: i64,
    /// Exact Kafka-owned transaction-state spelling.
    pub transaction_state: String,
}

/// Public canonical cluster-wide transaction listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTransactionsListing {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Globally transactional-ID-sorted public rows.
    pub transactions: Vec<TransactionListingSnapshot>,
}

/// One independently observed Kafka CLI transaction-list row.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerTransactionListing {
    /// Exact nonnegative transaction coordinator broker identity.
    pub coordinator_id: i32,
    /// Publicly comparable transaction fields.
    pub transaction: TransactionListingSnapshot,
}

/// Independent canonical cluster-wide transaction listing.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerTransactionsState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Globally transactional-ID-sorted Kafka CLI rows.
    pub transactions: Vec<BrokerTransactionListing>,
}

/// One topic and its canonical transaction partition set.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionTopicSnapshot {
    /// Exact Kafka topic spelling.
    pub topic: String,
    /// Strictly ascending nonnegative partitions.
    pub partitions: Vec<i32>,
}

/// Scenario-side expectations for one initialized transactional identifier.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionDescriptionExpectation {
    /// Exact transactional identifier.
    pub transactional_id: String,
    /// Exact Kafka-owned transaction-state spelling.
    pub expected_state: String,
    /// Exact configured transaction timeout.
    pub expected_transaction_timeout_ms: i32,
    /// Whether Kafka must expose a current transaction start time.
    pub expected_start_time_present: bool,
    /// Exact canonical topic-partition participation.
    pub expected_topics: Vec<TransactionTopicSnapshot>,
}

/// Scenario intent for one caller-ordered transaction-description batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTransactionsAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered exact expectations.
    pub transactions: Vec<TransactionDescriptionExpectation>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one caller-ordered transaction-description batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeTransactionsCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Caller-ordered exact transactional identifiers.
    pub transactional_ids: Vec<String>,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Exact public transaction-description fields shared with independent evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionDescriptionSnapshot {
    /// Exact transactional identifier.
    pub transactional_id: String,
    /// Exact Kafka-owned transaction-state spelling.
    pub transaction_state: String,
    /// Exact configured signed transaction timeout.
    pub transaction_timeout_ms: i32,
    /// Current transaction start time, when active.
    pub transaction_start_time_ms: Option<i64>,
    /// Exact signed producer identity.
    pub producer_id: i64,
    /// Exact signed producer epoch.
    pub producer_epoch: i16,
    /// Canonical participating topics and partitions.
    pub topics: Vec<TransactionTopicSnapshot>,
}

/// Public caller-ordered transaction descriptions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminTransactionsDescription {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// One successful public description per caller-selected ID.
    pub transactions: Vec<TransactionDescriptionSnapshot>,
}

/// One independently observed Kafka CLI transaction description.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerTransactionState {
    /// Monotonic observation ordinal from the environment.
    pub observation: u64,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact nonnegative transaction coordinator broker identity.
    pub coordinator_id: i32,
    /// Publicly comparable transaction fields.
    pub transaction: TransactionDescriptionSnapshot,
}

#[cfg(test)]
#[path = "admin_transactions_test.rs"]
mod tests;
