//! Scenario actions declare bounded public calls and external broker controls.
#![allow(missing_docs, reason = "admin variants use public payload types")]
use crate::{ClientId, ConsumerId, OperationId, ProducerId};
/// Scenario action vocabulary for scenario schema v37.
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScenarioAction {
    CreateClient {
        client_id: ClientId,
    },
    CreateConfiguredClient(crate::CreateConfiguredClientAction),
    /// Waits for one explicit public client readiness probe.
    AwaitClientReady {
        /// Existing client identity.
        client_id: ClientId,
    },
    ObserveClientMetrics(crate::ObserveClientMetricsAction),
    /// Creates one public producer.
    CreateProducer {
        /// Owning client.
        client_id: ClientId,
        /// New producer identity.
        producer_id: ProducerId,
    },
    SetBrokerBehavior {
        /// Next model-broker behavior.
        behavior: crate::BrokerBehavior,
    },
    ArmProtocolFault(crate::ProtocolFaultAction),
    AlterNetworkFault(crate::NetworkFaultAction),
    CutNetworkConnections(crate::NetworkConnectionCutAction),
    /// Restarts one environment-owned broker and waits for Kafka readiness.
    RestartBroker {
        /// One-based declared broker ordinal.
        broker_ordinal: u16,
        /// Complete disruption bound.
        timeout_ms: u64,
    },
    /// Stops one declared broker without restoring it in the same action.
    StopBroker {
        /// One-based declared broker ordinal.
        broker_ordinal: u16,
        /// Complete stop bound.
        timeout_ms: u64,
    },
    /// Starts one broker retained by a prior stop action.
    StartBroker {
        /// One-based declared broker ordinal.
        broker_ordinal: u16,
        /// Complete start and readiness bound.
        timeout_ms: u64,
    },
    /// Stops the independently observed owner of one exact Kafka role.
    StopBrokerRole {
        /// Exact role target discovered outside the packaged adapter.
        target: crate::BrokerRoleTarget,
        /// Complete election bound.
        timeout_ms: u64,
    },
    /// Restarts the broker retained by one prior role stop.
    RestoreBrokerRole {
        /// Exact role target used by the paired stop.
        target: crate::BrokerRoleTarget,
        /// Complete restoration bound.
        timeout_ms: u64,
    },
    /// Establishes or removes one independently observed broker policy.
    AlterBrokerPolicy(crate::BrokerPolicyAction),
    /// Offers one record.
    Send {
        /// Existing producer.
        producer_id: ProducerId,
        /// Stable operation identity.
        operation_id: OperationId,
        /// Exact record.
        record: crate::RecordSpec,
    },
    CancelProducerSend(crate::CancelProducerSendCommand),
    SendBatch {
        /// Existing producer.
        producer_id: ProducerId,
        /// Ordered operations.
        operations: Vec<crate::BatchRecord>,
    },
    StartConcurrentActors(crate::StartConcurrentActorsAction),
    JoinConcurrentActors(crate::JoinConcurrentActorsAction),
    /// Claims one directly assigned public consumer.
    CreateAssignedConsumer {
        /// Owning client.
        client_id: ClientId,
        /// New consumer identity.
        consumer_id: ConsumerId,
    },
    /// Replaces one consumer's assignment at the beginning of one partition.
    AssignBeginning {
        /// Existing assigned consumer.
        consumer_id: ConsumerId,
        /// Exact topic.
        topic: String,
        /// Exact partition.
        partition: i32,
    },
    /// Replaces one direct assignment with multiple partitions at their beginnings.
    AssignBeginningBatch(crate::AssignBeginningBatchAction),
    ControlAssignedConsumer(crate::AssignedConsumerControlAction),
    /// Bounded receive that must expose one previously sent exact record.
    Receive {
        /// Existing assigned consumer.
        consumer_id: ConsumerId,
        /// Stable receive identity.
        receive_id: OperationId,
        /// Expected producer operation.
        expected_operation_id: OperationId,
        /// Complete receive bound.
        timeout_ms: u64,
    },
    /// Closes one directly assigned consumer.
    CloseAssignedConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Registers one consumer-group member with an explicit protocol.
    CreateGroupConsumer {
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        /// Subscribed topic.
        topic: String,
        /// Group protocol.
        protocol: crate::GroupProtocol,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        configuration: Option<crate::GroupConsumerConfiguration>,
    },
    /// Receives one group batch and commits its assignment-fenced checkpoint.
    GroupReceive {
        /// Existing group consumer.
        consumer_id: ConsumerId,
        /// Stable receive identity.
        receive_id: OperationId,
        /// Expected producer operation.
        expected_operation_id: OperationId,
        /// Exact normalized public failure expected instead of a completion.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_error_code: Option<String>,
        /// Complete receive and commit bound.
        timeout_ms: u64,
    },
    /// Observes one stable complete assignment across declared live group members.
    ObserveGroupAssignments(crate::ObserveGroupAssignmentsAction),
    /// Receives and commits an exact record set across declared live group members.
    GroupReceiveSet(crate::GroupReceiveSetAction),
    ControlGroupConsumer(crate::GroupConsumerControlAction),
    ShutdownGroupConsumer(crate::GroupConsumerShutdownAction),
    CloseGroupConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    /// Registers one unique KIP-932 share-group member.
    CreateShareConsumer {
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        topic: String,
        /// Complete membership-start bound.
        membership_timeout_ms: u64,
        /// Complete close bound.
        close_timeout_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        configuration: Option<crate::ShareConsumerFetchConfiguration>,
    },
    /// Retains one exact share batch for a later acknowledgement or drop.
    ShareReceive {
        /// Existing share consumer.
        consumer_id: ConsumerId,
        /// Stable retained-batch identity.
        receive_id: OperationId,
        /// Ordered producer operations expected in this public batch.
        expected_operation_ids: Vec<OperationId>,
        /// Smallest accepted delivery count.
        minimum_delivery_count: i16,
        /// Exact public acquisition count expected for the retained batch.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_acquisition_count: Option<usize>,
        /// Complete receive bound.
        timeout_ms: u64,
    },
    /// Acknowledges every record in one retained share batch by record order.
    ShareAcknowledge {
        /// Existing share consumer.
        consumer_id: ConsumerId,
        /// Retained batch identity.
        receive_id: OperationId,
        /// Stable acknowledgement identity.
        acknowledgement_id: OperationId,
        /// One disposition per record in the retained public batch.
        dispositions: Vec<crate::ShareDisposition>,
        /// Complete acknowledgement bound.
        timeout_ms: u64,
    },
    /// Drops one retained share batch without a broker acknowledgement.
    DropShareBatch {
        /// Existing share consumer.
        consumer_id: ConsumerId,
        /// Retained batch identity.
        receive_id: OperationId,
    },
    /// Closes one unique share member and declares whether success is required.
    CloseShareConsumer {
        /// Share consumer to close.
        consumer_id: ConsumerId,
        /// Required public result.
        expect_success: bool,
    },
    CreateTopic(crate::CreateTopicAction),
    CreateTopicsBatch(crate::CreateTopicsBatchAction),
    CreatePartitions(crate::CreatePartitionsAction),
    DeleteTopic(crate::DeleteTopicAction),
    DescribeTopic(crate::DescribeTopicAction),
    ListTopics(crate::ListTopicsAction),
    ListOffsets(crate::ListOffsetsAction),
    DeleteRecords(crate::DeleteRecordsAction),
    DescribeTopicConfig(crate::DescribeTopicConfigAction),
    AlterTopicConfig(crate::AlterTopicConfigAction),
    DescribeCluster(crate::DescribeClusterAction),
    ListConsumerGroups(crate::ListConsumerGroupsAction),
    DescribeConsumerGroup(crate::DescribeConsumerGroupAction),
    ListConsumerGroupOffsets(crate::ListConsumerGroupOffsetsAction),
    ListConsumerGroupOffsetsBatch(crate::ListConsumerGroupOffsetsBatchAction),
    ListConsumerGroupsOffsets(crate::ListConsumerGroupsOffsetsAction),
    AlterConsumerGroupOffset(crate::AlterConsumerGroupOffsetAction),
    AlterConsumerGroupOffsets(crate::AlterConsumerGroupOffsetsAction),
    DeleteConsumerGroupOffset(crate::DeleteConsumerGroupOffsetAction),
    DeleteConsumerGroupOffsets(crate::DeleteConsumerGroupOffsetsAction),
    DeleteConsumerGroup(crate::DeleteConsumerGroupAction),
    DescribeClassicGroups(crate::DescribeClassicGroupsAction),
    /// Initializes one uniquely controlled public transactional producer.
    CreateTransactionalProducer {
        /// Owning client.
        client_id: ClientId,
        /// New producer identity.
        producer_id: ProducerId,
        /// Exact transactional identity.
        transactional_id: String,
        /// Broker transaction timeout.
        transaction_timeout_ms: u64,
        /// Complete initialization bound.
        initialization_timeout_ms: u64,
        /// Exact normalized public initialization failure.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_error_code: Option<String>,
    },
    /// Runs one linear transaction through send and commit or abort.
    ExecuteTransaction {
        /// Existing transactional producer.
        producer_id: ProducerId,
        /// Stable transaction identity.
        transaction_id: OperationId,
        /// Ordered staged operations.
        operations: Vec<crate::BatchRecord>,
        /// Requested transaction outcome.
        disposition: crate::TransactionDisposition,
        /// Complete transaction bound.
        timeout_ms: u64,
    },
    /// Atomically transforms one group-consumer batch and checkpoint.
    ExecuteTransactionalTransform(crate::TransactionalTransformAction),
    /// Stages one record, initializes a replacement owner, and observes the old commit result.
    FenceTransaction {
        /// Existing transactional producer.
        producer_id: ProducerId,
        /// Stable transaction identity.
        transaction_id: OperationId,
        /// Operation staged before fencing.
        operation: crate::BatchRecord,
        /// Owning client for the replacement.
        replacement_client_id: ClientId,
        /// Replacement producer identity.
        replacement_producer_id: ProducerId,
        /// Shared transactional identity.
        transactional_id: String,
        /// Broker transaction timeout.
        transaction_timeout_ms: u64,
        /// Complete replacement initialization bound.
        initialization_timeout_ms: u64,
        /// Complete fencing bound.
        timeout_ms: u64,
    },
    CloseTransactionalProducer(crate::CloseTransactionalProducerAction),
    Flush {
        /// Producer to flush.
        producer_id: ProducerId,
    },
    CloseProducer {
        /// Producer to close.
        producer_id: ProducerId,
    },
    ShutdownClient {
        client_id: ClientId,
    },
}
