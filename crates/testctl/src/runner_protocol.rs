use std::collections::BTreeSet;
use testlab_schema::{AdapterEvent, ClientId, ConsumerId, OperationId, ProducerId};

use crate::run_error::RunFailure;
use crate::runner_protocol_admin::classify_admin;
use crate::runner_protocol_admin_config::classify as classify_admin_config;
use crate::runner_protocol_admin_group_batch::classify as classify_admin_group_batch;
pub(crate) use crate::runner_protocol_event::EventDisposition;
use crate::runner_protocol_event::classify_core;
use crate::runner_protocol_family::{classify_group, classify_transaction};

#[derive(Clone, Debug)]
pub(crate) enum ExpectedEvent {
    Ready,
    ClientCreated(ClientId),
    ClientReady(ClientId),
    ClientMetricsObserved(ClientId, OperationId),
    ProducerCreated(ProducerId),
    SendSettled(OperationId),
    ProducerCancellationCompleted(OperationId),
    BatchCompleted {
        producer_id: ProducerId,
        operation_ids: BTreeSet<OperationId>,
    },
    ConcurrentActorsStarted(crate::runner_protocol_concurrent::ConcurrentExpectation),
    ConcurrentActorsCompleted(crate::runner_protocol_concurrent::ConcurrentExpectation),
    AssignedConsumerCreated(ConsumerId),
    AssignmentCompleted(ConsumerId),
    AssignedConsumerControlCompleted(testlab_schema::AssignedConsumerControlCompletion),
    ReceiveCompleted(OperationId),
    AssignedConsumerClosed(ConsumerId),
    GroupConsumerCreated(ConsumerId),
    GroupReceiveCompleted(OperationId),
    GroupAssignmentsObserved(OperationId),
    GroupReceiveSetCompleted(OperationId),
    GroupConsumerControlCompleted(testlab_schema::GroupConsumerControlCompletion),
    GroupConsumerShutdownCompleted(testlab_schema::GroupConsumerShutdownCompletion),
    GroupConsumerClosed(ConsumerId),
    ShareConsumerCreated(ConsumerId),
    ShareReceiveCompleted(OperationId),
    ShareAcknowledgementCompleted(OperationId),
    ShareBatchDropped(OperationId),
    ShareConsumerClosed(ConsumerId),
    TopicCreated {
        operation_id: OperationId,
        topic: String,
    },
    TopicCreationValidated {
        operation_id: OperationId,
        topic: String,
    },
    TopicsCreationCompleted {
        operation_id: OperationId,
    },
    TopicPartitionsCreated {
        operation_id: OperationId,
        topic: String,
    },
    TopicPartitionIncreaseValidated {
        operation_id: OperationId,
        topic: String,
    },
    TopicDeleted {
        operation_id: OperationId,
        topic: String,
    },
    TopicsDeleted {
        operation_id: OperationId,
        topics: Vec<String>,
    },
    TopicDescribed {
        operation_id: OperationId,
        topic: String,
    },
    TopicsDescribed {
        operation_id: OperationId,
        topics: Vec<String>,
    },
    TopicsListed {
        operation_id: OperationId,
    },
    OffsetListed {
        operation_id: OperationId,
        topic: String,
        partition: i32,
    },
    OffsetsListed {
        operation_id: OperationId,
    },
    RecordsDeleted {
        operation_id: OperationId,
        topic: String,
        partition: i32,
    },
    RecordsBatchDeleted {
        operation_id: OperationId,
        targets: Vec<(String, i32)>,
    },
    TopicConfigDescribed {
        operation_id: OperationId,
        topic: String,
        config_name: String,
    },
    TopicConfigsDescribed {
        operation_id: OperationId,
        topics: Vec<(String, String)>,
    },
    TopicConfigsAltered {
        operation_id: OperationId,
        topics: Vec<(String, String)>,
    },
    TopicConfigAltered {
        operation_id: OperationId,
        topic: String,
        config_name: String,
    },
    TopicConfigAlterationValidated {
        operation_id: OperationId,
        topic: String,
        config_name: String,
    },
    ClusterDescribed {
        operation_id: OperationId,
    },
    FeaturesDescribed(OperationId),
    MetadataQuorumDescribed(OperationId),
    ProducerStatesDescribed {
        operation_id: OperationId,
        topic: String,
        partition: i32,
    },
    LogDirsDescribed {
        operation_id: OperationId,
        topic: String,
        partition: i32,
    },
    ReplicaLogDirsDescribed(OperationId, String, i32),
    TransactionsListed(OperationId),
    TransactionsDescribed(OperationId, Vec<String>),
    ProducersFenced(OperationId, Vec<String>),
    ConsumerGroupsListed {
        operation_id: OperationId,
    },
    ConsumerGroupDescribed {
        operation_id: OperationId,
        group_id: String,
    },
    ShareGroupDescribed {
        operation_id: OperationId,
        group_id: String,
    },
    ShareGroupsDescribed {
        operation_id: OperationId,
        group_ids: Vec<String>,
    },
    ShareGroupOffsetsListed {
        operation_id: OperationId,
        group_id: String,
        topic: String,
        partition: i32,
    },
    ShareGroupsOffsetsListed {
        operation_id: OperationId,
        group_ids: Vec<String>,
    },
    ShareGroupOffsetsAltered {
        operation_id: OperationId,
        group_id: String,
        topic: String,
        partition: i32,
    },
    ShareGroupOffsetsDeleted {
        operation_id: OperationId,
        group_id: String,
        topic: String,
    },
    ShareGroupsDeleted {
        operation_id: OperationId,
        group_ids: Vec<String>,
    },
    ConsumerGroupOffsetListed {
        operation_id: OperationId,
        group_id: String,
        topic: String,
        partition: i32,
    },
    ConsumerGroupOffsetAltered {
        operation_id: OperationId,
        group_id: String,
        topic: String,
        partition: i32,
    },
    ConsumerGroupOffsetDeleted {
        operation_id: OperationId,
        group_id: String,
        topic: String,
        partition: i32,
    },
    ConsumerGroupDeleted {
        operation_id: OperationId,
        group_id: String,
    },
    ConsumerGroupsDeleted {
        operation_id: OperationId,
        group_ids: Vec<String>,
    },
    ConsumerGroupOffsetsListed {
        operation_id: OperationId,
    },
    ConsumerGroupsOffsetsListed {
        operation_id: OperationId,
    },
    ConsumerGroupOffsetsAltered {
        operation_id: OperationId,
    },
    ConsumerGroupOffsetsDeleted {
        operation_id: OperationId,
    },
    ClassicGroupsDescribed {
        operation_id: OperationId,
    },
    AclsCreated(OperationId),
    AclsDescribed(OperationId),
    AclsDeleted(OperationId),
    ClientQuotaAltered(OperationId),
    ClientQuotaDescribed(OperationId),
    UserScramCredentialAltered(OperationId),
    UserScramCredentialDescribed(OperationId),
    TransactionalProducerCreated(ProducerId),
    TransactionCompleted {
        transaction_id: OperationId,
        operation_ids: BTreeSet<OperationId>,
    },
    TransactionFenceCompleted {
        transaction_id: OperationId,
        operation_id: OperationId,
        replacement_producer_id: ProducerId,
    },
    TransactionalProducerClosed(ProducerId),
    FlushCompleted(ProducerId),
    ProducerClosed(ProducerId),
    ClientShutdown(ClientId),
    Finished,
    Aborted,
}

impl ExpectedEvent {
    pub(crate) fn classify(&self, event: &AdapterEvent) -> Result<EventDisposition, RunFailure> {
        if matches!(event, AdapterEvent::CommandFailed { .. }) {
            return Ok(EventDisposition::Complete);
        }
        if let Some(disposition) = crate::runner_protocol_concurrent::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_group(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_share::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_admin_config(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_admin_group_batch(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_acl::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_client_quota::classify(self, event)
        {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_user_scram::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_share_group::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_producers::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_admin_transactions::classify(self, event)
        {
            return disposition;
        }
        if let Some(disposition) = classify_admin(self, event) {
            return disposition;
        }
        if let Some(disposition) = crate::runner_protocol_cancel::classify(self, event) {
            return disposition;
        }
        if let Some(disposition) = classify_transaction(self, event) {
            return disposition;
        }
        classify_core(self, event)
    }
}
