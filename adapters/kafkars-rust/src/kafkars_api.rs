//! Curated imports bind the adapter to Kafkars' public module facades.

pub(crate) use kafkars::admin::{
    AccessControlEntry, AclBinding, AclBindingFilter, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType, Admin, ClientQuotaAlteration,
    ClientQuotaAlterationOperation, ClientQuotaEntity, ClientQuotaEntityComponent,
    ClientQuotaEntry, ClientQuotaFilterComponent, ClusterBroker, ConfigAlteration,
    ConfigResourceAlterations, ConfigResourceQuery, ConfigResourceType, ConsumerGroupAssignment,
    ConsumerGroupDescription, ConsumerGroupDescriptionDetails, ConsumerGroupMember,
    ConsumerGroupMemberDetails, ConsumerGroupMemberRemoval, ConsumerGroupOffset,
    ConsumerGroupOffsetAlteration, CreateAclOutcome, CreateAclResult, DelegationToken,
    DelegationTokenHmac, DelegationTokenPrincipal, DeleteAclFilterOutcome, DeleteAclFilterResult,
    DeleteAclMatchResult, DeleteRecordsTarget, DescribeStreamsGroupResult,
    DescribeStreamsGroupsResult, DescribeTopicPartitionsTopic, FeatureUpdate, LeaderElectionTarget,
    LeaderElectionType, LegacyConfigResourceReplacement, LegacyTopicConfigEntry,
    LegacyTopicConfigReplacement, ListConsumerGroupOffsetsQuery, ListConsumerGroupOffsetsResult,
    ListOffsetsQuery, ListShareGroupOffsetsQuery, ListShareGroupOffsetsResult,
    ListStreamsGroupOffsetsQuery, ListStreamsGroupOffsetsResult, ListStreamsGroupsOffsetsResult,
    MetadataQuorumListener, MetadataQuorumNode, MetadataQuorumReplica, NewPartitions, NewTopic,
    OffsetSpec, PartitionReassignment, PartitionReassignmentChange, ReplicaLogDirAssignment,
    ResourcePattern, ScramCredentialInfo, ScramMechanism, ShareGroupDescription, ShareGroupOffset,
    ShareGroupOffsetAlteration, StreamsGroupDescription, TopicConfigAlterations, TopicConfigQuery,
    TopicDescription, TopicPartitionReplica, UserScramCredentialAlteration,
};
pub(crate) use kafkars::client::{Client, ClientBuilder};
pub(crate) use kafkars::consumer::{
    AssignedConsumer, AssignedConsumerBuildError, Checkpoint, ClassicGroupAssignor,
    ClassicGroupConfig, Consumer, ConsumerAssignment, ConsumerBatch, ConsumerBuildError,
    ConsumerCommitAdmissionError, ConsumerEvent, ConsumerGroupProtocol, ConsumerRecord,
    GroupConsumerRecord, GroupMembershipEpoch, GroupMetadata, OffsetReset, ReadIsolation,
    StartPosition, TopicPartition,
};
#[cfg(kafkars_share_candidate)]
pub(crate) use kafkars::consumer::{
    CloseShareConsumer, ShareConsumer, ShareConsumerBatch, ShareConsumerFetchConfig,
    ShareConsumerRecord, ShareDisposition,
};
pub(crate) use kafkars::error::{DeliveryStatus, Error as KafkaError, ErrorKind, RetryAdvice};
pub(crate) use kafkars::metrics::{
    LatencyMetric as KafkarsLatencyMetric, MetricsSnapshot as KafkarsMetricsSnapshot,
};
pub(crate) use kafkars::producer::{
    CancellationOutcome, Compression, Delivery, Header, Producer, ProducerConfig, ProducerLimits,
    ProducerRetryConfig, Record, RecordMetadata, TrySendError,
};
pub(crate) use kafkars::security::{Sasl, Security, Tls};
pub(crate) use kafkars::transaction::{Transaction, TransactionalProducer};
