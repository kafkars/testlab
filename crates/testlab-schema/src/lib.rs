//! Versioned data contracts shared across the testlab trust boundary.

mod adapter;
mod admin_action_validation;
mod admin_classic_group;
mod admin_classic_group_transition_validation;
mod admin_cluster;
mod admin_config;
mod admin_config_action_validation;
mod admin_config_transition_validation;
mod admin_create_topics_batch;
mod admin_create_topics_batch_validation;
mod admin_delete_records;
mod admin_delete_records_action_validation;
mod admin_delete_records_transition_validation;
mod admin_expected_error;
mod admin_group;
mod admin_group_action_validation;
mod admin_group_offset;
mod admin_group_offset_batch;
mod admin_group_offset_batch_mutation;
mod admin_group_offset_mutation;
mod admin_group_offset_transition_validation;
mod admin_group_plural_action_validation;
mod admin_offset_position;
mod admin_scenario_action;
mod admin_topic;
mod admin_topic_action_validation;
mod admin_transition_validation;
mod admin_validate_only_validation;
mod assigned_consumer_control;
mod assigned_consumer_control_validation;
mod broker_policy;
mod broker_role;
mod broker_state;
mod bytes;
mod client_metrics;
mod concurrent;
mod concurrent_validation;
mod consumer_action_validation;
mod consumer_control_validation;
mod consumer_group_ownership;
mod consumer_group_ownership_validation;
mod contract;
mod environment;
mod environment_adversary_validation;
mod environment_validation;
mod evidence;
mod expected_client_error;
mod group_consumer_control;
mod group_consumer_control_validation;
mod group_consumer_shutdown;
mod group_consumer_shutdown_validation;
mod ids;
mod network_proxy;
mod pack;
mod producer_cancellation;
mod producer_cancellation_validation;
mod producer_configuration;
mod producer_configuration_validation;
mod protocol;
mod protocol_adversary;
mod protocol_command;
mod protocol_event;
mod protocol_group;
mod protocol_security;
mod qualification;
mod qualification_evidence;
mod receive_action_validation;
mod record;
mod scenario;
mod scenario_action;
mod scenario_action_lifecycle_validation;
mod scenario_action_state;
mod scenario_action_validation;
mod scenario_assertion_validation;
mod scenario_broker_policy_validation;
mod scenario_capability_validation;
mod scenario_environment_action_validation;
mod scenario_error;
mod scenario_types;
mod scenario_validation;
mod share;
mod share_action_validation;
mod subject;
mod transaction_action_validation;
mod transaction_offsets;
mod transaction_state_validation;
mod transaction_transform_validation;
mod verdict;

pub use adapter::{AdapterDescriptor, Capability};
pub use admin_classic_group::*;
pub use admin_cluster::{AdminClusterDescription, DescribeClusterAction, DescribeClusterCommand};
pub use admin_config::{
    AdminTopicConfigCompletion, AdminTopicConfigDescription, AlterTopicConfigAction,
    AlterTopicConfigCommand, BrokerTopicConfigState, DescribeTopicConfigAction,
    DescribeTopicConfigCommand,
};
pub use admin_create_topics_batch::{
    AdminTopicCreationOutcome, AdminTopicsCreationBatch, CreateTopicBatchActionItem,
    CreateTopicBatchCommandItem, CreateTopicsBatchAction, CreateTopicsBatchCommand,
};
pub use admin_delete_records::{AdminRecordsDeleted, DeleteRecordsAction, DeleteRecordsCommand};
pub use admin_expected_error::expected_admin_error;
pub use admin_group::*;
pub use admin_group_offset::{
    AdminConsumerGroupOffsetListing, ListConsumerGroupOffsetsAction,
    ListConsumerGroupOffsetsCommand,
};
pub use admin_group_offset_batch::{
    AdminConsumerGroupOffsetOutcome, AdminConsumerGroupOffsetsListing,
    AdminConsumerGroupOffsetsOutcome, AdminConsumerGroupsOffsetsListing,
    ConsumerGroupOffsetExpectation, ConsumerGroupOffsetSelection, ConsumerGroupOffsetsExpectation,
    ConsumerGroupOffsetsSelection, ListConsumerGroupOffsetsBatchAction,
    ListConsumerGroupOffsetsBatchCommand, ListConsumerGroupsOffsetsAction,
    ListConsumerGroupsOffsetsCommand,
};
pub use admin_group_offset_batch_mutation::{
    AdminConsumerGroupOffsetMutationOutcome, AdminConsumerGroupOffsetsMutation,
    AlterConsumerGroupOffsetsAction, AlterConsumerGroupOffsetsCommand,
    ConsumerGroupOffsetAlteration, DeleteConsumerGroupOffsetsAction,
    DeleteConsumerGroupOffsetsCommand,
};
pub use admin_group_offset_mutation::{
    AdminConsumerGroupOffsetCompletion, AlterConsumerGroupOffsetAction,
    AlterConsumerGroupOffsetCommand, DeleteConsumerGroupOffsetAction,
    DeleteConsumerGroupOffsetCommand,
};
pub use admin_offset_position::AdminOffsetPosition;
pub use admin_scenario_action::{
    CreatePartitionsAction, DescribeTopicAction, ListOffsetsAction, ListTopicsAction,
};
pub use admin_topic::{
    AdminOffsetListing, AdminTopicCompletion, AdminTopicDescription, AdminTopicsListing,
    CreatePartitionsCommand, CreateTopicAction, CreateTopicCommand, DeleteTopicAction,
    DeleteTopicCommand, DescribeTopicCommand, ListOffsetsCommand, ListTopicsCommand,
    TOPIC_ALREADY_EXISTS_ERROR_CODE, UNKNOWN_TOPIC_OR_PARTITION_ERROR_CODE,
};
pub use assigned_consumer_control::*;
pub use broker_policy::{
    ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE, BrokerAclOperation, BrokerAclResource, BrokerPolicy,
    BrokerPolicyAction, BrokerPolicyState, BrokerQuotaDirection, GROUP_AUTHORIZATION_ERROR_CODE,
    PRODUCER_TOPIC_AUTHORIZATION_ERROR_CODE, TRANSACTIONAL_ID_AUTHORIZATION_ERROR_CODE,
};
pub use broker_role::BrokerRoleTarget;
pub use broker_state::{
    BrokerClusterState, BrokerConsumerGroupOffset, BrokerConsumerGroupState,
    BrokerPartitionOffsets, BrokerTopicState,
};
pub use bytes::{ByteEncoding, ByteString, ByteStringError};
pub use client_metrics::*;
pub use concurrent::*;
pub use consumer_group_ownership::*;
pub use contract::{ContractDefinition, ContractRegistry, ContractRegistryError};
pub use environment::{
    Authentication, BrokerIdentity, ENVIRONMENT_SCHEMA_VERSION, EnvironmentDriver,
    EnvironmentError, EnvironmentManifest, SecurityProfile, TransportSecurity,
};
pub use evidence::{
    BrokerObservation, BrokerStateObservation, EVIDENCE_SCHEMA_VERSION, EnvironmentOperation,
    EnvironmentOperationKind, EnvironmentOperationStatus, EvidenceManifest, HarnessError,
    HistoryEntry, HistoryPayload,
};
pub use expected_client_error::expected_client_error;
pub use group_consumer_control::*;
pub use group_consumer_shutdown::*;
pub use ids::{
    ActorId, AdapterId, CellId, ClientId, CommandId, ConcurrencyId, ConsumerId, ContractId,
    EnvironmentId, EnvironmentOperationId, IdError, OperationId, PackId, ProducerId,
    QualificationId, RunId, ScenarioId, StepId, SubjectId,
};
pub use network_proxy::{
    NETWORK_PROXY_PROTOCOL_VERSION, NetworkConnectionCutAction, NetworkConnectionsCutObservation,
    NetworkDirection, NetworkFault, NetworkFaultAction, NetworkFaultState,
    NetworkFaultWindowObservation, NetworkProxyControl, NetworkProxyControlEnvelope,
    NetworkProxyEvent, NetworkProxyObservation, NetworkProxyRoute,
};
pub use pack::{ScenarioPack, ScenarioPackError};
pub use producer_cancellation::{
    CancelProducerSendCommand, ProducerCancellationCompletion, ProducerCancellationOutcome,
};
pub use producer_configuration::*;
pub use protocol::{AdapterEventEnvelope, CommandEnvelope, PROTOCOL_VERSION};
pub use protocol_adversary::{
    ADVERSARY_PROTOCOL_VERSION, AdversaryControlEnvelope, AdversaryEvent, AdversaryOutcome,
    DisconnectPoint, KafkaApi, ProtocolAdversaryObservation, ProtocolFault, ProtocolFaultAction,
};
pub use protocol_command::AdapterCommand;
pub use protocol_event::AdapterEvent;
pub use protocol_group::*;
pub use protocol_security::{
    AdapterSaslMechanism, AdapterSecurity, SASL_PASSWORD_ENVIRONMENT, SASL_USERNAME_ENVIRONMENT,
    TLS_CA_PEM_ENVIRONMENT, TerminalStatus,
};
pub use qualification::{
    QUALIFICATION_SCHEMA_VERSION, QualificationCell, QualificationError, QualificationManifest,
};
pub use qualification_evidence::{
    QUALIFICATION_EVIDENCE_SCHEMA_VERSION, QualificationCellEvidence, QualificationEvidenceError,
    QualificationEvidenceManifest, QualificationRunEvidence,
};
pub use record::{ConsumedRecord, HeaderSpec, RecordError, RecordSpec};
pub use scenario::{SCENARIO_SCHEMA_VERSION, Scenario};
pub use scenario_action::ScenarioAction;
pub use scenario_error::ScenarioError;
pub use scenario_types::{
    BatchRecord, BrokerBehavior, CloseTransactionalProducerAction, OperationAssertion,
    ScenarioStep, TransactionDisposition, VisibilityExpectation,
};
pub use share::{ShareConsumedRecord, ShareConsumerFetchConfiguration, ShareDisposition};
pub use subject::{SUBJECT_SCHEMA_VERSION, SubjectArtifact, SubjectError, SubjectManifest};
pub use transaction_offsets::{
    TransactionalTransformAction, TransactionalTransformCommand, TransactionalTransformCompletion,
};
pub use verdict::{Verdict, VerdictStatus, Violation};
#[cfg(test)]
mod admin_action_validation_test;
#[cfg(test)]
mod admin_config_test;
#[cfg(test)]
mod admin_create_topics_batch_test;
#[cfg(test)]
mod admin_delete_records_target_test;
#[cfg(test)]
mod admin_delete_records_test;
#[cfg(test)]
mod admin_group_offset_test;
#[cfg(test)]
mod admin_group_plural_protocol_test;
#[cfg(test)]
mod admin_group_plural_transition_test;
#[cfg(test)]
mod admin_group_plural_validation_test;
#[cfg(test)]
mod admin_protocol_test;
#[cfg(test)]
mod admin_query_ownership_test;
#[cfg(test)]
mod admin_query_protocol_test;
#[cfg(test)]
mod admin_query_validation_test;
#[cfg(test)]
mod admin_topic_failure_validation_test;
#[cfg(test)]
mod admin_transition_validation_test;
#[cfg(test)]
mod admin_v17_validation_test;
#[cfg(test)]
mod admin_v18_protocol_test;
#[cfg(test)]
mod admin_validate_only_validation_test;
#[cfg(test)]
mod assigned_consumer_control_test;
#[cfg(test)]
mod broker_policy_test;
#[cfg(test)]
mod broker_role_test;
#[cfg(test)]
mod bytes_test;
#[cfg(test)]
mod client_metrics_test;
#[cfg(test)]
mod concurrent_test;
#[cfg(test)]
mod consumer_group_ownership_test;
#[cfg(test)]
mod environment_test;
#[cfg(test)]
mod group_consumer_control_test;
#[cfg(test)]
mod group_consumer_shutdown_test;
#[cfg(test)]
mod ids_test;
#[cfg(test)]
mod network_proxy_test;
#[cfg(test)]
mod producer_cancellation_test;
#[cfg(test)]
mod producer_configuration_test;
#[cfg(test)]
mod protocol_adversary_test;
#[cfg(test)]
mod protocol_group_test;
#[cfg(test)]
mod qualification_evidence_test;
#[cfg(test)]
mod qualification_test;
#[cfg(test)]
mod record_test;
#[cfg(test)]
mod scenario_test;
#[cfg(test)]
mod share_action_validation_test;
#[cfg(test)]
mod subject_test;
#[cfg(test)]
mod transaction_offsets_test;
