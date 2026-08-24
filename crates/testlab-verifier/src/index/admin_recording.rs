//! Admin history recording keeps topic creation and partition growth distinct.

use testlab_schema::{AdapterCommand, AdapterEvent};

use super::{HistoryIndex, IndexedAdminTopicCompletion};

impl HistoryIndex {
    pub(super) fn record_admin_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let (operation_id, topic, completions) = match event {
            AdapterEvent::TopicCreated {
                operation_id,
                topic,
            } => (operation_id, topic, &mut self.topics_created),
            AdapterEvent::TopicPartitionsCreated {
                operation_id,
                topic,
            } => (operation_id, topic, &mut self.topic_partitions_created),
            _ => return false,
        };
        completions
            .entry(operation_id.clone())
            .or_default()
            .push(IndexedAdminTopicCompletion {
                history_sequence: sequence,
                topic: topic.clone(),
            });
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
            _ => return false,
        }
        true
    }
}
