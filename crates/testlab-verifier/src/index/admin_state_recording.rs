//! Independent admin-state observations are indexed without merging adapter claims.

use testlab_schema::BrokerStateObservation;

use super::{
    HistoryIndex, IndexedClusterObservation, IndexedConsumerGroupObservation,
    IndexedConsumerGroupOffsetObservation, IndexedPartitionOffsetsObservation,
    IndexedTopicConfigObservation, IndexedTopicObservation,
};

impl HistoryIndex {
    pub(super) fn record_state(&mut self, observation: &BrokerStateObservation, sequence: u64) {
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
        }
    }
}
