//! Batched group-admin indexing retains ordered public outcomes and exact wire commands.

use std::collections::BTreeMap;

use testlab_schema::{AdapterCommand, AdapterEvent, OperationId, ScenarioAction};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConsumerGroupOffsetsListing {
    pub(crate) history_sequence: u64,
    pub(crate) group_id: String,
    pub(crate) outcomes: Vec<testlab_schema::AdminConsumerGroupOffsetOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConsumerGroupsOffsetsListing {
    pub(crate) history_sequence: u64,
    pub(crate) groups: Vec<testlab_schema::AdminConsumerGroupOffsetsOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedConsumerGroupOffsetsMutation {
    pub(crate) history_sequence: u64,
    pub(crate) group_id: String,
    pub(crate) outcomes: Vec<testlab_schema::AdminConsumerGroupOffsetMutationOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IndexedClassicGroupsDescription {
    pub(crate) history_sequence: u64,
    pub(crate) outcomes: Vec<testlab_schema::AdminClassicGroupDescriptionOutcome>,
}

#[derive(Debug, Default)]
pub(crate) struct AdminGroupBatchIndex {
    pub(crate) offsets_listed: BTreeMap<OperationId, Vec<IndexedConsumerGroupOffsetsListing>>,
    pub(crate) groups_offsets_listed:
        BTreeMap<OperationId, Vec<IndexedConsumerGroupsOffsetsListing>>,
    pub(crate) offsets_altered: BTreeMap<OperationId, Vec<IndexedConsumerGroupOffsetsMutation>>,
    pub(crate) offsets_deleted: BTreeMap<OperationId, Vec<IndexedConsumerGroupOffsetsMutation>>,
    pub(crate) classic_groups_described:
        BTreeMap<OperationId, Vec<IndexedClassicGroupsDescription>>,
}

impl AdminGroupBatchIndex {
    pub(super) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::ConsumerGroupOffsetsListed(value) => self
                .offsets_listed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedConsumerGroupOffsetsListing {
                    history_sequence: sequence,
                    group_id: value.group_id.clone(),
                    outcomes: value.outcomes.clone(),
                }),
            AdapterEvent::ConsumerGroupsOffsetsListed(value) => self
                .groups_offsets_listed
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedConsumerGroupsOffsetsListing {
                    history_sequence: sequence,
                    groups: value.groups.clone(),
                }),
            AdapterEvent::ConsumerGroupOffsetsAltered(value) => self
                .offsets_altered
                .entry(value.operation_id.clone())
                .or_default()
                .push(mutation(sequence, value)),
            AdapterEvent::ConsumerGroupOffsetsDeleted(value) => self
                .offsets_deleted
                .entry(value.operation_id.clone())
                .or_default()
                .push(mutation(sequence, value)),
            AdapterEvent::ClassicGroupsDescribed(value) => self
                .classic_groups_described
                .entry(value.operation_id.clone())
                .or_default()
                .push(IndexedClassicGroupsDescription {
                    history_sequence: sequence,
                    outcomes: value.outcomes.clone(),
                }),
            _ => return false,
        }
        true
    }
}

pub(super) fn action_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    Some(match action {
        ScenarioAction::ListConsumerGroupOffsetsBatch(value) => &value.operation_id,
        ScenarioAction::ListConsumerGroupsOffsets(value) => &value.operation_id,
        ScenarioAction::AlterConsumerGroupOffsets(value) => &value.operation_id,
        ScenarioAction::DeleteConsumerGroupOffsets(value) => &value.operation_id,
        ScenarioAction::DescribeClassicGroups(value) => &value.operation_id,
        _ => return None,
    })
}

pub(super) fn command_operation_id(command: &AdapterCommand) -> Option<&OperationId> {
    Some(match command {
        AdapterCommand::ListConsumerGroupOffsetsBatch(value) => &value.operation_id,
        AdapterCommand::ListConsumerGroupsOffsets(value) => &value.operation_id,
        AdapterCommand::AlterConsumerGroupOffsets(value) => &value.operation_id,
        AdapterCommand::DeleteConsumerGroupOffsets(value) => &value.operation_id,
        AdapterCommand::DescribeClassicGroups(value) => &value.operation_id,
        _ => return None,
    })
}

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> Option<bool> {
    Some(match (action, command) {
        (
            ScenarioAction::ListConsumerGroupOffsetsBatch(action),
            AdapterCommand::ListConsumerGroupOffsetsBatch(command),
        ) => {
            base(action, command)
                && action.require_stable == command.require_stable
                && selections(&action.partitions) == command.partitions
        }
        (
            ScenarioAction::ListConsumerGroupsOffsets(action),
            AdapterCommand::ListConsumerGroupsOffsets(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.require_stable == command.require_stable
                && action.timeout_ms == command.timeout_ms
                && action.groups.len() == command.groups.len()
                && action
                    .groups
                    .iter()
                    .zip(&command.groups)
                    .all(|(left, right)| {
                        left.group_id == right.group_id
                            && selections(&left.partitions) == right.partitions
                    })
        }
        (
            ScenarioAction::AlterConsumerGroupOffsets(action),
            AdapterCommand::AlterConsumerGroupOffsets(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.group_id == command.group_id
                && action.offsets == command.offsets
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DeleteConsumerGroupOffsets(action),
            AdapterCommand::DeleteConsumerGroupOffsets(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.group_id == command.group_id
                && action.partitions == command.partitions
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::DescribeClassicGroups(action),
            AdapterCommand::DescribeClassicGroups(command),
        ) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action
                    .groups
                    .iter()
                    .map(|group| &group.group_id)
                    .eq(command.group_ids.iter())
                && action.timeout_ms == command.timeout_ms
        }
        _ => return None,
    })
}

fn base(
    action: &testlab_schema::ListConsumerGroupOffsetsBatchAction,
    command: &testlab_schema::ListConsumerGroupOffsetsBatchCommand,
) -> bool {
    action.client_id == command.client_id
        && action.operation_id == command.operation_id
        && action.group_id == command.group_id
        && action.timeout_ms == command.timeout_ms
}

fn selections(
    values: &[testlab_schema::ConsumerGroupOffsetExpectation],
) -> Vec<testlab_schema::ConsumerGroupOffsetSelection> {
    values
        .iter()
        .map(|value| testlab_schema::ConsumerGroupOffsetSelection {
            topic: value.topic.clone(),
            partition: value.partition,
        })
        .collect()
}

fn mutation(
    history_sequence: u64,
    value: &testlab_schema::AdminConsumerGroupOffsetsMutation,
) -> IndexedConsumerGroupOffsetsMutation {
    IndexedConsumerGroupOffsetsMutation {
        history_sequence,
        group_id: value.group_id.clone(),
        outcomes: value.outcomes.clone(),
    }
}
