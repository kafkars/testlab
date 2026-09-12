//! Required-capability checks keep declared support aligned with scenario use.

use std::collections::BTreeSet;

use crate::scenario_action_validation::ActionStates;
use crate::{Capability, Scenario};

pub(crate) fn validate_required(
    scenario: &Scenario,
    usage: &BTreeSet<Capability>,
    state: &ActionStates,
    problems: &mut Vec<String>,
) {
    require(
        !state.sends.is_empty(),
        Capability::Producer,
        "send steps require the producer capability",
        scenario,
        problems,
    );
    for &(capability, message) in REQUIRED_USAGE {
        require(
            usage.contains(&capability),
            capability,
            message,
            scenario,
            problems,
        );
    }
    let handles = !state.clients.is_empty()
        || !state.producers.is_empty()
        || !state.consumers.is_empty()
        || !state.transactions.is_empty();
    require(
        handles,
        Capability::Lifecycle,
        "handle steps require the lifecycle capability",
        scenario,
        problems,
    );
}

const REQUIRED_USAGE: &[(Capability, &str)] = &[
    (
        Capability::IndependentHandles,
        "independent child handles require the independent_handles capability",
    ),
    (
        Capability::ConcurrentActors,
        "concurrent actor steps require the concurrent_actors capability",
    ),
    (
        Capability::ProducerConfiguration,
        "configured-client steps require the producer_configuration capability",
    ),
    (
        Capability::ProducerWaitingSend,
        "waiting sends require the producer_waiting_send capability",
    ),
    (
        Capability::ProducerCancellation,
        "cancel-send steps require the producer_cancellation capability",
    ),
    (
        Capability::ProducerBatch,
        "batch-send steps require the producer_batch capability",
    ),
    (
        Capability::ProducerReceiptMetadata,
        "UUID-bound sends require the producer_receipt_metadata capability",
    ),
    (
        Capability::AssignedConsumer,
        "assigned-consumer steps require the assigned_consumer capability",
    ),
    (
        Capability::AssignedConsumerFetchEvidence,
        "Fetch evidence receives require the assigned_consumer_fetch_evidence capability",
    ),
    (
        Capability::AssignedConsumerRecordTransfer,
        "owned record transfers require the assigned_consumer_record_transfer capability",
    ),
    (
        Capability::AssignedConsumerImmediateBatch,
        "immediate assigned-consumer receives require the assigned_consumer_immediate_batch capability",
    ),
    (
        Capability::AssignedConsumerEvents,
        "assigned-consumer event observations require the assigned_consumer_events capability",
    ),
    (
        Capability::AssignedConsumerConfiguration,
        "configured assigned-consumer clients require the assigned_consumer_configuration capability",
    ),
    (
        Capability::AssignedConsumerControls,
        "assigned-consumer controls require the assigned_consumer_controls capability",
    ),
    (
        Capability::ConsumerGroups,
        "classic group-consumer steps require the consumer_groups capability",
    ),
    (
        Capability::ConsumerProtocolGroups,
        "KIP-848 group-consumer steps require the consumer_protocol_groups capability",
    ),
    (
        Capability::GroupConsumerImmediateBatch,
        "immediate group receives require the group_consumer_immediate_batch capability",
    ),
    (
        Capability::GroupConsumerAcknowledge,
        "processing acknowledgements require the group_consumer_acknowledge capability",
    ),
    (
        Capability::GroupConsumerPartialCheckpoint,
        "partial group checkpoints require the group_consumer_partial_checkpoint capability",
    ),
    (
        Capability::GroupConsumerControls,
        "group-consumer controls require the group_consumer_controls capability",
    ),
    (
        Capability::GroupConsumerConfiguration,
        "configured group consumers require the group_consumer_configuration capability",
    ),
    (
        Capability::GroupConsumerShutdown,
        "group shutdown steps require the group_consumer_shutdown capability",
    ),
    (
        Capability::ShareConsumer,
        "share-consumer steps require the share_consumer capability",
    ),
    (
        Capability::ShareConsumerConfiguration,
        "configured share consumers require the share_consumer_configuration capability",
    ),
    (
        Capability::Admin,
        "admin steps require the admin capability",
    ),
    (
        Capability::Transactions,
        "transaction steps require the transactions capability",
    ),
    (
        Capability::TransactionBatchSend,
        "transactional batch sends require the transaction_batch_send capability",
    ),
    (
        Capability::TransactionTopicUuidValidation,
        "UUID-bound transactions require the transaction_topic_uuid_validation capability",
    ),
    (
        Capability::ModelBroker,
        "broker-control steps require the model_broker capability",
    ),
    (
        Capability::ExpectedClusterIdentity,
        "expected cluster IDs require the expected_cluster_identity capability",
    ),
    (
        Capability::ClientReadiness,
        "client readiness steps require the client_readiness capability",
    ),
    (
        Capability::ClientMetrics,
        "client metrics steps require the client_metrics capability",
    ),
];

fn require(
    used: bool,
    capability: Capability,
    message: &str,
    scenario: &Scenario,
    problems: &mut Vec<String>,
) {
    if used && !scenario.requires.contains(&capability) {
        problems.push(message.to_owned());
    }
}
