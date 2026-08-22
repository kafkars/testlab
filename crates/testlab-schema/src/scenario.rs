//! Declarative public scenarios are validated before any subject process starts.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    Capability, ClientId, ConsumerId, OperationId, ProducerId, RecordSpec, ScenarioId, StepId,
    TerminalStatus,
};

/// Current scenario manifest version.
pub const SCENARIO_SCHEMA_VERSION: u16 = 5;

/// One complete black-box scenario.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    /// Exact scenario schema version.
    pub schema_version: u16,
    /// Stable scenario identity.
    pub id: ScenarioId,
    /// Human-readable title.
    pub title: String,
    /// Reviewable statement of intent.
    pub description: String,
    /// Absolute run timeout measured from scenario execution start.
    pub timeout_ms: u64,
    /// Capabilities required from the adapter.
    #[serde(default)]
    pub requires: BTreeSet<Capability>,
    /// Ordered public and environment actions.
    pub steps: Vec<ScenarioStep>,
    /// Deterministic operation assertions.
    pub assertions: Vec<OperationAssertion>,
}

impl Scenario {
    /// Validates structure, identity ownership, and complete lifecycle.
    pub fn validate(&self) -> Result<(), ScenarioError> {
        crate::scenario_validation::validate(self)
    }
}

/// One named scenario action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ScenarioStep {
    /// Stable step identity.
    pub id: StepId,
    /// Action payload.
    #[serde(flatten)]
    pub action: ScenarioAction,
}

/// Scenario action vocabulary for scenario schema v5.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScenarioAction {
    /// Creates one public client.
    CreateClient {
        /// New client identity.
        client_id: ClientId,
    },
    /// Waits for one explicit public client readiness probe.
    AwaitClientReady {
        /// Existing live client.
        client_id: ClientId,
    },
    /// Creates one public producer.
    CreateProducer {
        /// Existing owning client.
        client_id: ClientId,
        /// New producer identity.
        producer_id: ProducerId,
    },
    /// Selects the next self-test broker outcome.
    SetBrokerBehavior {
        /// One-shot broker behavior.
        behavior: BrokerBehavior,
    },
    /// Offers one record.
    Send {
        /// Existing open producer.
        producer_id: ProducerId,
        /// New operation identity.
        operation_id: OperationId,
        /// Exact logical record.
        record: RecordSpec,
    },
    /// Offers an ordered record batch through one public call.
    SendBatch {
        /// Existing open producer.
        producer_id: ProducerId,
        /// Ordered records with stable operation identities.
        operations: Vec<BatchRecord>,
    },
    /// Claims one directly assigned public consumer.
    CreateAssignedConsumer {
        /// Existing owning client.
        client_id: ClientId,
        /// New consumer identity.
        consumer_id: ConsumerId,
    },
    /// Replaces one consumer's assignment at the beginning of one partition.
    AssignBeginning {
        /// Existing open consumer.
        consumer_id: ConsumerId,
        /// Exact topic.
        topic: String,
        /// Exact partition.
        partition: i32,
    },
    /// Bounded receive that must expose one previously sent exact record.
    Receive {
        /// Existing assigned consumer.
        consumer_id: ConsumerId,
        /// Stable identity for the receive operation.
        receive_id: OperationId,
        /// Producer operation whose record must appear exactly once.
        expected_operation_id: OperationId,
        /// Adapter-side public observation bound.
        timeout_ms: u64,
    },
    /// Closes one directly assigned consumer.
    CloseAssignedConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Registers one classic consumer-group member.
    CreateGroupConsumer {
        /// Existing owning client.
        client_id: ClientId,
        /// New consumer identity.
        consumer_id: ConsumerId,
        /// Exact Kafka group identity.
        group_id: String,
        /// Subscribed topic.
        topic: String,
    },
    /// Receives one group batch and commits its assignment-fenced checkpoint.
    GroupReceive {
        /// Existing group consumer.
        consumer_id: ConsumerId,
        /// Stable receive operation identity.
        receive_id: OperationId,
        /// Producer operation whose record must appear exactly once.
        expected_operation_id: OperationId,
        /// Adapter-side public observation bound.
        timeout_ms: u64,
    },
    /// Closes one classic group consumer.
    CloseGroupConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Flushes one open producer.
    Flush {
        /// Producer to flush.
        producer_id: ProducerId,
    },
    /// Closes one open producer.
    CloseProducer {
        /// Producer to close.
        producer_id: ProducerId,
    },
    /// Shuts down one client after its producers close.
    ShutdownClient {
        /// Client to shut down.
        client_id: ClientId,
    },
}

/// One identified record within a public batch send.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BatchRecord {
    /// Stable operation identity.
    pub operation_id: OperationId,
    /// Exact logical record.
    pub record: RecordSpec,
}

/// One-shot behavior supported by the self-test model broker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrokerBehavior {
    /// Persist once and acknowledge.
    Acknowledge,
    /// Persist once and close without a response.
    AcceptAndDropResponse,
    /// Reject before persistence.
    Reject,
    /// Persist twice and acknowledge, used by verifier tests.
    DuplicateAndAcknowledge,
    /// Persist corrupted bytes and acknowledge, used by verifier tests.
    CorruptAndAcknowledge,
}

/// Expected public and broker-visible result for one send.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationAssertion {
    /// Operation under test.
    pub operation_id: OperationId,
    /// Whether the public producer should accept ownership.
    pub accepted: bool,
    /// Expected terminal status for an accepted operation.
    pub terminal: Option<TerminalStatus>,
    /// Expected independent visibility.
    pub visibility: VisibilityExpectation,
}

/// Expected number of broker-visible records.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VisibilityExpectation {
    /// No matching record may exist.
    Absent,
    /// Exactly one matching record must exist.
    ExactlyOnce,
    /// Zero or one matching record may exist.
    ZeroOrOne,
}

/// Invalid scenario with all reviewable problems retained.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid scenario: {problems:?}")]
pub struct ScenarioError {
    /// Every discovered validation problem.
    pub problems: Vec<String>,
}
