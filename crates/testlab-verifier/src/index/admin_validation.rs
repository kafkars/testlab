//! Validate-only results stay separate from mutation completions in the history index.

use std::collections::BTreeMap;

use testlab_schema::{AdapterEvent, OperationId};

use super::{IndexedAdminTopicCompletion, IndexedAdminTopicConfigCompletion};

#[derive(Debug, Default)]
pub(crate) struct AdminValidationIndex {
    pub(crate) topic_creations: BTreeMap<OperationId, Vec<IndexedAdminTopicCompletion>>,
    pub(crate) partition_increases: BTreeMap<OperationId, Vec<IndexedAdminTopicCompletion>>,
    pub(crate) config_alterations: BTreeMap<OperationId, Vec<IndexedAdminTopicConfigCompletion>>,
}

impl AdminValidationIndex {
    pub(super) fn record(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::TopicCreationValidated(value) => self
                .topic_creations
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedAdminTopicCompletion {
                    history_sequence: sequence,
                    topic: value.topic.clone(),
                }),
            AdapterEvent::TopicPartitionIncreaseValidated(value) => self
                .partition_increases
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedAdminTopicCompletion {
                    history_sequence: sequence,
                    topic: value.topic.clone(),
                }),
            AdapterEvent::TopicConfigAlterationValidated(value) => self
                .config_alterations
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedAdminTopicConfigCompletion {
                    history_sequence: sequence,
                    topic: value.topic.clone(),
                    config_name: value.config_name.clone(),
                }),
            _ => return false,
        }
        true
    }
}
