//! Independent admin-state observations are indexed without merging adapter claims.

use testlab_schema::BrokerStateObservation;

use super::{
    HistoryIndex, IndexedClusterObservation, IndexedConsumerGroupObservation,
    IndexedConsumerGroupOffsetObservation, IndexedPartitionOffsetsObservation,
    IndexedTopicConfigObservation, IndexedTopicIdentityObservation, IndexedTopicObservation,
};

impl HistoryIndex {
    #[allow(
        clippy::too_many_lines,
        reason = "exhaustive broker-state indexing keeps every observation source explicit"
    )]
    pub(super) fn record_state(&mut self, observation: &BrokerStateObservation, sequence: u64) {
        if self.admin_lifecycles.record_state(observation, sequence) {
            return;
        }
        if self
            .admin_config_resources
            .record_state(observation, sequence)
        {
            return;
        }
        if self
            .admin_leader_elections
            .record_state(observation, sequence)
        {
            return;
        }
        if self
            .admin_partition_reassignments
            .record_state(observation, sequence)
        {
            return;
        }
        if self.admin_share_groups.record_state(observation, sequence) {
            return;
        }
        if self.admin_user_scram.record_state(observation, sequence) {
            return;
        }
        if self.admin_client_quotas.record_state(observation, sequence) {
            return;
        }
        if self.admin_features.record_state(observation, sequence) {
            return;
        }
        if self.admin_acls.record_state(observation, sequence) {
            return;
        }
        match observation {
            BrokerStateObservation::Topic(value) => self
                .topics_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedTopicObservation {
                    history_sequence: sequence,
                    observation: value.observation,
                    topic: value.topic.clone(),
                    exists: value.exists,
                    partitions: value.partitions.clone(),
                }),
            BrokerStateObservation::TopicIdentity(value) => self
                .topic_identities_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedTopicIdentityObservation {
                    history_sequence: sequence,
                    observation: value.observation,
                    topic: value.topic.clone(),
                    topic_id: value.topic_id,
                    partitions: value.partitions.clone(),
                }),
            BrokerStateObservation::Cluster(value) => self
                .clusters_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedClusterObservation {
                    history_sequence: sequence,
                    observation: value.observation,
                    cluster_id: value.cluster_id.clone(),
                    broker_ids: value.broker_ids.clone(),
                }),
            BrokerStateObservation::ConsumerGroup(value) => self
                .consumer_groups_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedConsumerGroupObservation {
                    history_sequence: sequence,
                    observation: value.observation,
                    group_id: value.group_id.clone(),
                    exists: value.exists,
                    member_count: value.member_count,
                }),
            BrokerStateObservation::ConsumerGroupOffset(value) => self
                .consumer_group_offsets_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedConsumerGroupOffsetObservation {
                    history_sequence: sequence,
                    observation: value.observation,
                    group_id: value.group_id.clone(),
                    topic: value.topic.clone(),
                    partition: value.partition,
                    offset: value.offset,
                }),
            BrokerStateObservation::TopicConfig(value) => self
                .topic_configs_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedTopicConfigObservation {
                    history_sequence: sequence,
                    observation: value.observation,
                    topic: value.topic.clone(),
                    config_name: value.config_name.clone(),
                    value: value.value.clone(),
                }),
            BrokerStateObservation::ConfigResources(_) => {
                unreachable!("configuration resources are indexed before generic admin state")
            }
            BrokerStateObservation::PartitionOffsets(value) => self
                .partition_offsets_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedPartitionOffsetsObservation {
                    history_sequence: sequence,
                    observation: value.observation,
                    topic: value.topic.clone(),
                    partition: value.partition,
                    low_watermark: value.low_watermark,
                    high_watermark: value.high_watermark,
                }),
            BrokerStateObservation::Acl(_) => {
                unreachable!("ACL observations are indexed before generic admin state")
            }
            BrokerStateObservation::ClientQuota(_) => {
                unreachable!("client-quota observations are indexed before generic admin state")
            }
            BrokerStateObservation::Features(_) => {
                unreachable!("feature observations are indexed before generic admin state")
            }
            BrokerStateObservation::MetadataQuorum(_) => {
                unreachable!("quorum observations are indexed before generic admin state")
            }
            BrokerStateObservation::Producers(_) => {
                unreachable!("producer observations are indexed before generic admin state")
            }
            BrokerStateObservation::LogDirs(_) => {
                unreachable!("log-directory observations are indexed before generic admin state")
            }
            BrokerStateObservation::Transactions(_) | BrokerStateObservation::Transaction(_) => {
                unreachable!("transaction observations are indexed before generic admin state")
            }
            BrokerStateObservation::PartitionAssignments(_)
            | BrokerStateObservation::PartitionReassignments(_) => {
                unreachable!("reassignment observations are indexed before generic admin state")
            }
            BrokerStateObservation::LeaderElection(_) => {
                unreachable!("leader-election observations are indexed before generic admin state")
            }
            BrokerStateObservation::UserScramCredential(_) => {
                unreachable!("user SCRAM observations are indexed before generic admin state")
            }
            BrokerStateObservation::DelegationTokens(_) => {
                unreachable!("delegation-token observations are indexed before generic admin state")
            }
            BrokerStateObservation::StreamsGroups(_) => {
                unreachable!("Streams-group observations are indexed before generic admin state")
            }
            BrokerStateObservation::ShareGroup(_) => {
                unreachable!("Share-group observations are indexed before generic admin state")
            }
            BrokerStateObservation::ShareGroupOffset(_) => {
                unreachable!(
                    "Share-group offset observations are indexed before generic admin state"
                )
            }
        }
    }
}
