//! Admin history recording keeps each public operation and result shape distinct.

use testlab_schema::{AdapterCommand, AdapterEvent};

use super::{
    HistoryIndex, IndexedAdminTopicCompletion, IndexedOffsetList, IndexedTopicDescription,
    IndexedTopicsList,
};

impl HistoryIndex {
    pub(super) fn record_admin_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let topic_completion = match event {
            AdapterEvent::TopicCreated {
                operation_id,
                topic,
            } => Some((operation_id, topic, &mut self.topics_created)),
            AdapterEvent::TopicPartitionsCreated {
                operation_id,
                topic,
            } => Some((operation_id, topic, &mut self.topic_partitions_created)),
            _ => None,
        };
        if let Some((operation_id, topic, completions)) = topic_completion {
            completions.entry(operation_id.clone()).or_default().push(
                IndexedAdminTopicCompletion {
                    history_sequence: sequence,
                    topic: topic.clone(),
                },
            );
            return true;
        }
        match event {
            AdapterEvent::TopicDescribed {
                operation_id,
                topic,
                partitions,
            } => self
                .topics_described
                .entry(operation_id.clone())
                .or_default()
                .push(IndexedTopicDescription {
                    history_sequence: sequence,
                    topic: topic.clone(),
                    partitions: partitions.clone(),
                }),
            AdapterEvent::TopicsListed {
                operation_id,
                topics,
            } => self
                .topics_listed
                .entry(operation_id.clone())
                .or_default()
                .push(IndexedTopicsList {
                    history_sequence: sequence,
                    topics: topics.clone(),
                }),
            AdapterEvent::OffsetListed {
                operation_id,
                topic,
                partition,
                offset,
            } => self
                .offsets_listed
                .entry(operation_id.clone())
                .or_default()
                .push(IndexedOffsetList {
                    history_sequence: sequence,
                    topic: topic.clone(),
                    partition: *partition,
                    offset: *offset,
                }),
            _ => return false,
        }
        true
    }

    pub(super) fn record_admin_command(&mut self, command: &AdapterCommand) -> bool {
        match command {
            AdapterCommand::CreateTopic { operation_id, .. } => {
                self.topics_create_issued.insert(operation_id.clone());
            }
            AdapterCommand::CreatePartitions { operation_id, .. } => {
                self.topic_partitions_create_issued
                    .insert(operation_id.clone());
            }
            AdapterCommand::DescribeTopic { operation_id, .. } => {
                self.topics_describe_issued.insert(operation_id.clone());
            }
            AdapterCommand::ListTopics { operation_id, .. } => {
                self.topics_list_issued.insert(operation_id.clone());
            }
            AdapterCommand::ListOffsets { operation_id, .. } => {
                self.offsets_list_issued.insert(operation_id.clone());
            }
            _ => return false,
        }
        true
    }
}
