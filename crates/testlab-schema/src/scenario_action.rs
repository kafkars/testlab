#![allow(missing_docs, reason = "admin variants use public payload types")]
use crate::{ClientId, ConsumerId, OperationId, ProducerId};
#[derive(Clone, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScenarioAction {
    CreateClient(crate::CreateClientAction),
    CreateConfiguredClient(crate::CreateConfiguredClientAction),
    CreateAssignedConsumerClient(crate::CreateAssignedConsumerClientAction),
    AwaitClientReady {
        client_id: ClientId,
    },
    ObserveClientMetrics(crate::ObserveClientMetricsAction),
    CreateProducer {
        client_id: ClientId,
        producer_id: ProducerId,
        #[serde(default)]
        ownership: crate::ChildHandleOwnership,
    },
    SetBrokerBehavior {
        behavior: crate::BrokerBehavior,
    },
    ArmProtocolFault(crate::ProtocolFaultAction),
    AlterNetworkFault(crate::NetworkFaultAction),
    CutNetworkConnections(crate::NetworkConnectionCutAction),
    RestartBroker {
        broker_ordinal: u16,
        timeout_ms: u64,
    },
    StopBroker {
        broker_ordinal: u16,
        timeout_ms: u64,
    },
    StartBroker {
        broker_ordinal: u16,
        timeout_ms: u64,
    },
    StopBrokerRole {
        /// Exact role target discovered outside the packaged adapter.
        target: crate::BrokerRoleTarget,
        /// Complete election bound.
        timeout_ms: u64,
    },
    RestoreBrokerRole {
        /// Exact role target used by the paired stop.
        target: crate::BrokerRoleTarget,
        /// Complete restoration bound.
        timeout_ms: u64,
    },
    AlterBrokerPolicy(crate::BrokerPolicyAction),
    Send {
        producer_id: ProducerId,
        operation_id: OperationId,
        /// Exact public single-record producer method.
        #[serde(default)]
        method: crate::ProducerSendMethod,
        #[serde(default)]
        partitioning: crate::ProducerPartitioning,
        record: crate::RecordSpec,
    },
    CancelProducerSend(crate::CancelProducerSendCommand),
    SendBatch {
        producer_id: ProducerId,
        operations: Vec<crate::BatchRecord>,
    },
    StartConcurrentActors(crate::StartConcurrentActorsAction),
    JoinConcurrentActors(crate::JoinConcurrentActorsAction),
    CreateAssignedConsumer {
        client_id: ClientId,
        consumer_id: ConsumerId,
        #[serde(default)]
        ownership: crate::ChildHandleOwnership,
    },
    AssignBeginning {
        consumer_id: ConsumerId,
        topic: String,
        partition: i32,
    },
    AssignBeginningBatch(crate::AssignBeginningBatchAction),
    ControlAssignedConsumer(crate::AssignedConsumerControlAction),
    ObserveAssignedConsumerEvent(crate::ObserveAssignedConsumerEventAction),
    Receive {
        consumer_id: ConsumerId,
        /// Exact public retained-batch observation method.
        #[serde(default)]
        method: crate::AssignedConsumerReceiveMethod,
        receive_id: OperationId,
        expected_operation_id: OperationId,
        /// Complete receive bound.
        timeout_ms: u64,
    },
    /// Closes one directly assigned consumer.
    CloseAssignedConsumer {
        /// Consumer to close.
        consumer_id: ConsumerId,
    },
    CreateGroupConsumer {
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        topics: Vec<String>,
        /// Group protocol.
        protocol: crate::GroupProtocol,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        configuration: Option<crate::GroupConsumerConfiguration>,
    },
    /// Receives one group batch and commits its assignment-fenced checkpoint.
    GroupReceive {
        consumer_id: ConsumerId,
        /// Exact public retained-batch observation method.
        #[serde(default)]
        method: crate::GroupConsumerReceiveMethod,
        receive_id: OperationId,
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
        consumer_id: ConsumerId,
    },
    AbandonGroupConsumer(crate::GroupConsumerAbandonment),
    /// Registers one unique KIP-932 share-group member.
    CreateShareConsumer {
        client_id: ClientId,
        consumer_id: ConsumerId,
        group_id: String,
        topics: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rack: Option<String>,
        membership_timeout_ms: u64,
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
    DeleteTopics(crate::DeleteTopicsAction),
    DescribeTopic(crate::DescribeTopicAction),
    DescribeTopics(crate::DescribeTopicsAction),
    ListTopics(crate::ListTopicsAction),
    ListConfigResources(crate::ListConfigResourcesAction),
    ListOffsets(crate::ListOffsetsAction),
    ListOffsetsBatch(crate::ListOffsetsBatchAction),
    DeleteRecords(crate::DeleteRecordsAction),
    DeleteRecordsBatch(crate::DeleteRecordsBatchAction),
    DescribeTopicConfig(crate::DescribeTopicConfigAction),
    DescribeTopicConfigs(crate::DescribeTopicConfigsAction),
    AlterTopicConfigs(crate::AlterTopicConfigsAction),
    AlterTopicConfig(crate::AlterTopicConfigAction),
    DescribeCluster(crate::DescribeClusterAction),
    UnregisterBroker(crate::UnregisterBrokerAction),
    DescribeFeatures(crate::DescribeFeaturesAction),
    ValidateFeatureUpdates(crate::ValidateFeatureUpdatesAction),
    ExerciseDelegationTokenLifecycle(crate::ExerciseDelegationTokenLifecycleAction),
    ExerciseStreamsGroupAdminLifecycle(crate::ExerciseStreamsGroupAdminLifecycleAction),
    DescribeProducers(crate::DescribeProducersAction),
    DescribeLogDirs(crate::DescribeLogDirsAction),
    DescribeReplicaLogDirs(crate::DescribeReplicaLogDirsAction),
    AlterReplicaLogDirs(crate::AlterReplicaLogDirsAction),
    DescribeMetadataQuorum(crate::DescribeMetadataQuorumAction),
    ListTransactions(crate::ListTransactionsAction),
    DescribeTransactions(crate::DescribeTransactionsAction),
    FenceProducers(crate::FenceProducersAction),
    AlterPartitionReassignments(crate::AlterPartitionReassignmentsAction),
    ListPartitionReassignments(crate::ListPartitionReassignmentsAction),
    ElectLeaders(crate::ElectLeadersAction),
    ListConsumerGroups(crate::ListConsumerGroupsAction),
    DescribeConsumerGroup(crate::DescribeConsumerGroupAction),
    DescribeConsumerGroups(crate::DescribeConsumerGroupsAction),
    DescribeShareGroup(crate::DescribeShareGroupAction),
    DescribeShareGroups(crate::DescribeShareGroupsAction),
    ListShareGroupOffsets(crate::ListShareGroupOffsetsAction),
    ListShareGroupsOffsets(crate::ListShareGroupsOffsetsAction),
    AlterShareGroupOffsets(crate::AlterShareGroupOffsetsAction),
    DeleteShareGroupOffsets(crate::DeleteShareGroupOffsetsAction),
    DeleteShareGroups(crate::DeleteShareGroupsAction),
    ListConsumerGroupOffsets(crate::ListConsumerGroupOffsetsAction),
    ListConsumerGroupOffsetsBatch(crate::ListConsumerGroupOffsetsBatchAction),
    ListConsumerGroupsOffsets(crate::ListConsumerGroupsOffsetsAction),
    AlterConsumerGroupOffset(crate::AlterConsumerGroupOffsetAction),
    AlterConsumerGroupOffsets(crate::AlterConsumerGroupOffsetsAction),
    DeleteConsumerGroupOffset(crate::DeleteConsumerGroupOffsetAction),
    DeleteConsumerGroupOffsets(crate::DeleteConsumerGroupOffsetsAction),
    DeleteConsumerGroup(crate::DeleteConsumerGroupAction),
    DeleteConsumerGroups(crate::DeleteConsumerGroupsAction),
    RemoveConsumerGroupMembers(crate::RemoveConsumerGroupMembersAction),
    DescribeClassicGroups(crate::DescribeClassicGroupsAction),
    CreateAcls(crate::CreateAclsAction),
    DescribeAcls(crate::DescribeAclsAction),
    DeleteAcls(crate::DeleteAclsAction),
    AlterClientQuota(crate::AlterClientQuotaAction),
    DescribeClientQuota(crate::DescribeClientQuotaAction),
    AlterUserScramCredential(crate::AlterUserScramCredentialAction),
    DescribeUserScramCredential(crate::DescribeUserScramCredentialAction),
    CreateTransactionalProducer {
        client_id: ClientId,
        producer_id: ProducerId,
        transactional_id: String,
        transaction_timeout_ms: u64,
        initialization_timeout_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_error_code: Option<String>,
    },
    ExecuteTransaction {
        /// Existing transactional producer.
        producer_id: ProducerId,
        /// Stable transaction identity.
        transaction_id: OperationId,
        /// Ordered staged operations.
        operations: Vec<crate::BatchRecord>,
        /// Requested transaction terminal operation.
        disposition: crate::TransactionDisposition,
        /// Complete transaction bound.
        timeout_ms: u64,
    },
    ExecuteTransactionalTransform(crate::TransactionalTransformAction),
    /// Stages one record, fences its owner, and proves replacement use.
    FenceTransaction {
        /// Public operation that fences the active transaction.
        #[serde(default)]
        fence_method: crate::TransactionFenceMethod,
        /// Existing transactional producer.
        producer_id: ProducerId,
        /// Stable transaction identity.
        transaction_id: OperationId,
        /// Operation staged before fencing.
        operation: crate::BatchRecord,
        replacement_client_id: ClientId,
        /// Replacement producer identity.
        replacement_producer_id: ProducerId,
        /// Shared transactional identity.
        transactional_id: String,
        transaction_timeout_ms: u64,
        /// Complete replacement initialization bound.
        initialization_timeout_ms: u64,
        timeout_ms: u64,
    },
    CloseTransactionalProducer(crate::CloseTransactionalProducerAction),
    Flush {
        producer_id: ProducerId,
    },
    CloseProducer {
        producer_id: ProducerId,
    },
    ShutdownClient {
        client_id: ClientId,
    },
}
