//! Admin history recording keeps each public operation and result shape distinct.
use super::{
    HistoryIndex, IndexedAdminGroupCompletion, IndexedAdminGroupOffsetCompletion,
    IndexedAdminTopicCompletion, IndexedAdminTopicsCreationBatch, IndexedAdminTopicsDeletion,
    IndexedAdminTopicsDescription, IndexedClusterDescription, IndexedConsumerGroupDescription,
    IndexedConsumerGroupOffset, IndexedConsumerGroupsList, IndexedOffsetList,
    IndexedTopicDescription, IndexedTopicsList,
    admin_command_router::{action_operation_id, command_matches, command_operation_id},
};
use testlab_schema::{AdapterCommand, AdapterEvent, CommandId, ScenarioAction};
impl HistoryIndex {
    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive event recorder keeps every public admin result visibly indexed"
    )]
    pub(super) fn record_admin_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        if self.admin_lifecycles.record_event(event, sequence) {
            return true;
        }
        if self.admin_config_resources.record_event(event, sequence) {
            return true;
        }
        if self.admin_leader_elections.record_event(event, sequence) {
            return true;
        }
        if self
            .admin_partition_reassignments
            .record_event(event, sequence)
        {
            return true;
        }
        if self.admin_replica_log_dirs.record_event(event, sequence) {
            return true;
        }
        if self.admin_share_groups.record_event(event, sequence) {
            return true;
        }
        if self.admin_user_scram.record_event(event, sequence) {
            return true;
        }
        if self.admin_client_quotas.record_event(event, sequence) {
            return true;
        }
        if self.admin_features.record_event(event, sequence) {
            return true;
        }
        if self.admin_acls.record_event(event, sequence) {
            return true;
        }
        if self.admin_offset_batches.record_event(event, sequence) {
            return true;
        }
        if self.admin_validations.record(event, sequence) {
            return true;
        }
        if self.admin_group_batches.record_event(event, sequence) {
            return true;
        }
        if super::admin_records::record(self, event, sequence) {
            return true;
        }
        if super::admin_config_command_match::record(self, event, sequence) {
            return true;
        }
        match event {
            AdapterEvent::TopicCreated(value) => self
                .topics_created
                .entry(value.operation_id.clone())
                .or_default()
                .push(topic_completion(sequence, value.topic.clone())),
            AdapterEvent::TopicsCreationCompleted(value) => self
                .topics_creation_completed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedAdminTopicsCreationBatch {
                    history_sequence: sequence,
                    outcomes: value.outcomes.clone(),
                }),
            AdapterEvent::TopicPartitionsCreated(value) => self
                .topic_partitions_created
                .entry(value.operation_id.clone())
                .or_default()
                .push(topic_completion(sequence, value.topic.clone())),
            AdapterEvent::TopicDeleted(value) => self
                .topics_deleted
                .entry(value.operation_id.clone())
                .or_default()
                .push(topic_completion(sequence, value.topic.clone())),
            AdapterEvent::TopicsDeleted(value) => self
                .topics_batch_deleted
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedAdminTopicsDeletion {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            AdapterEvent::TopicDescribed(value) => self
                .topics_described
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedTopicDescription {
                    history_sequence: sequence,
                    topic: value.topic.clone(),
                    partitions: value.partitions.clone(),
                    pages: value.pages.clone(),
                }),
            AdapterEvent::TopicsDescribed(value) => self
                .topics_batch_described
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedAdminTopicsDescription {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            AdapterEvent::TopicsListed(value) => self
                .topics_listed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedTopicsList {
                    history_sequence: sequence,
                    outcomes: value.outcomes.clone(),
                }),
            AdapterEvent::OffsetListed(value) => self
                .offsets_listed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedOffsetList {
                    history_sequence: sequence,
                    topic: value.topic.clone(),
                    partition: value.partition,
                    offset: value.offset,
                    timestamp_millis: value.timestamp_millis,
                }),
            AdapterEvent::ClusterDescribed(value) => self
                .clusters_described
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedClusterDescription {
                    history_sequence: sequence,
                    cluster_id: value.cluster_id.clone(),
                    broker_ids: value.broker_ids.clone(),
                    fenced_broker_ids: value.fenced_broker_ids.clone(),
                    authorized_operations: value.authorized_operations,
                }),
            AdapterEvent::ConsumerGroupsListed(value) => self
                .consumer_groups_listed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedConsumerGroupsList {
                    history_sequence: sequence,
                    group_ids: value.group_ids.clone(),
                    broker_errors: value.broker_errors.clone(),
                }),
            AdapterEvent::ConsumerGroupDescribed(value) => self
                .consumer_groups_described
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedConsumerGroupDescription {
                    history_sequence: sequence,
                    group_id: value.group_id.clone(),
                    member_count: value.member_count,
                    authorized_operations: value.authorized_operations,
                }),
            AdapterEvent::ConsumerGroupOffsetListed(value) => self
                .consumer_group_offsets_listed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedConsumerGroupOffset {
                    history_sequence: sequence,
                    group_id: value.group_id.clone(),
                    topic: value.topic.clone(),
                    partition: value.partition,
                    offset: value.offset,
                }),
            AdapterEvent::ConsumerGroupOffsetAltered(value) => self
                .consumer_group_offsets_altered
                .entry(value.operation_id.clone())
                .or_default()
                .push(group_offset_completion(sequence, value)),
            AdapterEvent::ConsumerGroupOffsetDeleted(value) => self
                .consumer_group_offsets_deleted
                .entry(value.operation_id.clone())
                .or_default()
                .push(group_offset_completion(sequence, value)),
            AdapterEvent::ConsumerGroupDeleted(value) => self
                .consumer_groups_deleted
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedAdminGroupCompletion {
                    history_sequence: sequence,
                    group_id: value.group_id.clone(),
                }),
            _ => return false,
        }
        true
    }

    pub(super) fn record_admin_command(
        &mut self,
        command_id: &CommandId,
        command: &AdapterCommand,
        sequence: u64,
    ) -> bool {
        if command_operation_id(command).is_none() {
            return false;
        }
        self.admin_commands
            .push((sequence, command_id.clone(), command.clone()));
        true
    }

    pub(super) fn admin_action_issued(&self, action: &ScenarioAction) -> Option<bool> {
        action_operation_id(action).map(|_| self.admin_command_state(action).0)
    }

    pub(crate) fn admin_command_state(&self, action: &ScenarioAction) -> (bool, usize) {
        let Some(operation_id) = action_operation_id(action) else {
            return (false, 0);
        };
        let commands = self
            .admin_commands
            .iter()
            .filter(|(_, _, command)| command_operation_id(command) == Some(operation_id))
            .collect::<Vec<_>>();
        let exact = commands.len() == 1
            && commands
                .first()
                .is_some_and(|(_, _, command)| command_matches(action, command));
        (exact, commands.len())
    }

    pub(crate) fn admin_command_sequence(&self, action: &ScenarioAction) -> Option<u64> {
        self.admin_command_window(action)
            .map(|(command, _)| command)
    }

    pub(crate) fn admin_command_window(
        &self,
        action: &ScenarioAction,
    ) -> Option<crate::admin::AdminCommandWindow> {
        let operation_id = action_operation_id(action)?;
        let mut matching = self.admin_commands.iter().filter(|(_, _, command)| {
            command_operation_id(command) == Some(operation_id) && command_matches(action, command)
        });
        let (sequence, _, _) = matching.next()?;
        if matching.next().is_some() {
            return None;
        }
        let next = self
            .command_sequences
            .iter()
            .copied()
            .filter(|candidate| candidate > sequence)
            .min();
        Some((*sequence, next))
    }

    pub(crate) fn admin_command_failures(
        &self,
        action: &ScenarioAction,
    ) -> Vec<&super::IndexedCommandFailure> {
        let Some(operation_id) = action_operation_id(action) else {
            return Vec::new();
        };
        let commands = self
            .admin_commands
            .iter()
            .filter(|(_, _, command)| {
                command_operation_id(command) == Some(operation_id)
                    && command_matches(action, command)
            })
            .collect::<Vec<_>>();
        let [(_, command_id, _)] = commands.as_slice() else {
            return Vec::new();
        };
        self.command_failures
            .iter()
            .filter(|failure| &failure.command_id == command_id)
            .collect()
    }
}

fn topic_completion(history_sequence: u64, topic: String) -> IndexedAdminTopicCompletion {
    IndexedAdminTopicCompletion {
        history_sequence,
        topic,
    }
}

fn group_offset_completion(
    history_sequence: u64,
    value: &testlab_schema::AdminConsumerGroupOffsetCompletion,
) -> IndexedAdminGroupOffsetCompletion {
    IndexedAdminGroupOffsetCompletion {
        history_sequence,
        group_id: value.group_id.clone(),
        topic: value.topic.clone(),
        partition: value.partition,
    }
}
