//! Admin history recording keeps each public operation and result shape distinct.

use testlab_schema::{
    AdapterCommand, AdapterEvent, BrokerStateObservation, ListConsumerGroupOffsetsAction,
    ListConsumerGroupOffsetsCommand,
};

use super::{
    HistoryIndex, IndexedAdminTopicCompletion, IndexedConsumerGroupOffset,
    IndexedConsumerGroupOffsetObservation, IndexedOffsetList, IndexedTopicDescription,
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
            AdapterEvent::ConsumerGroupOffsetListed {
                operation_id,
                group_id,
                topic,
                partition,
                offset,
            } => self
                .consumer_group_offsets_listed
                .entry(operation_id.clone())
                .or_default()
                .push(IndexedConsumerGroupOffset {
                    history_sequence: sequence,
                    group_id: group_id.clone(),
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
            AdapterCommand::ListConsumerGroupOffsets(command) => {
                self.consumer_group_offset_commands.push(command.clone());
            }
            _ => return false,
        }
        true
    }

    pub(super) fn group_offset_action_issued(
        &self,
        action: &ListConsumerGroupOffsetsAction,
    ) -> bool {
        self.group_offset_command_state(action).0
    }

    pub(crate) fn group_offset_command_state(
        &self,
        action: &ListConsumerGroupOffsetsAction,
    ) -> (bool, usize) {
        let commands: Vec<_> = self
            .consumer_group_offset_commands
            .iter()
            .filter(|command| command.operation_id == action.operation_id)
            .collect();
        let exact = commands.len() == 1
            && commands
                .first()
                .is_some_and(|command| group_offset_command_matches(command, action));
        (exact, commands.len())
    }

    pub(super) fn record_state(&mut self, observation: &BrokerStateObservation) {
        let BrokerStateObservation::ConsumerGroupOffset {
            observation,
            operation_id,
            group_id,
            topic,
            partition,
            offset,
        } = observation;
        self.consumer_group_offsets_observed
            .entry(operation_id.clone())
            .or_default()
            .push(IndexedConsumerGroupOffsetObservation {
                observation: *observation,
                group_id: group_id.clone(),
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
            });
    }
}

fn group_offset_command_matches(
    command: &ListConsumerGroupOffsetsCommand,
    action: &ListConsumerGroupOffsetsAction,
) -> bool {
    command.client_id == action.client_id
        && command.operation_id == action.operation_id
        && command.group_id == action.group_id
        && command.topic == action.topic
        && command.partition == action.partition
        && command.require_stable == action.require_stable
        && command.timeout_ms == action.timeout_ms
}
