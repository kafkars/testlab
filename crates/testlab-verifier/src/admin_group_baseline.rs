//! Plural offset mutations require a prior independently corroborated visible value per key.

use testlab_schema::{OperationId, Scenario, ScenarioAction};

use crate::admin_group_batch::{batch_listing_matches, groups_listing_matches};
use crate::index::HistoryIndex;

pub(crate) fn has_prior_baseline(
    scenario: &Scenario,
    index: &HistoryIndex,
    mutation_id: &OperationId,
    group_id: &str,
    topic: &str,
    partition: i32,
    changed_to: Option<i64>,
) -> bool {
    let Some(mutation_sequence) = scenario
        .steps
        .iter()
        .position(|step| mutation_operation_id(&step.action) == Some(mutation_id))
    else {
        return false;
    };
    scenario.steps[..mutation_sequence].iter().any(|step| {
        baseline_value(&step.action, index, group_id, topic, partition)
            .is_some_and(|value| changed_to.is_none() || changed_to != Some(value))
    })
}

fn baseline_value(
    action: &ScenarioAction,
    index: &HistoryIndex,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> Option<i64> {
    match action {
        ScenarioAction::ListConsumerGroupOffsets(expected)
            if expected.group_id == group_id
                && expected.topic == topic
                && expected.partition == partition
                && index.admin_command_state(action) == (true, 1) =>
        {
            valid_single(expected, index).then_some(expected.expected_offset)
        }
        ScenarioAction::ListConsumerGroupOffsetsBatch(expected)
            if expected.group_id == group_id && index.admin_command_state(action) == (true, 1) =>
        {
            let value = expected
                .partitions
                .iter()
                .find(|value| value.topic == topic && value.partition == partition)?;
            batch_listing_matches(
                expected,
                index
                    .admin_group_batches
                    .offsets_listed
                    .get(&expected.operation_id),
                index
                    .consumer_group_offsets_observed
                    .get(&expected.operation_id),
                index.admin_command_window(action),
            )
            .then_some(value.expected_offset)
        }
        ScenarioAction::ListConsumerGroupsOffsets(expected)
            if index.admin_command_state(action) == (true, 1) =>
        {
            let value = expected
                .groups
                .iter()
                .find(|value| value.group_id == group_id)?
                .partitions
                .iter()
                .find(|value| value.topic == topic && value.partition == partition)?;
            groups_listing_matches(
                expected,
                index
                    .admin_group_batches
                    .groups_offsets_listed
                    .get(&expected.operation_id),
                index
                    .consumer_group_offsets_observed
                    .get(&expected.operation_id),
                index.admin_command_window(action),
            )
            .then_some(value.expected_offset)
        }
        _ => None,
    }
}

fn valid_single(
    expected: &testlab_schema::ListConsumerGroupOffsetsAction,
    index: &HistoryIndex,
) -> bool {
    let window =
        index.admin_command_window(&ScenarioAction::ListConsumerGroupOffsets(expected.clone()));
    let public = index
        .consumer_group_offsets_listed
        .get(&expected.operation_id)
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let independent = index
        .consumer_group_offsets_observed
        .get(&expected.operation_id)
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    public.is_some_and(|public| {
        public.group_id == expected.group_id
            && public.topic == expected.topic
            && public.partition == expected.partition
            && public.offset == Some(expected.expected_offset)
            && crate::admin::public_after_command(window, public.history_sequence)
            && independent.is_some_and(|independent| {
                independent.group_id == expected.group_id
                    && independent.topic == expected.topic
                    && independent.partition == expected.partition
                    && independent.offset == Some(expected.expected_offset)
                    && crate::admin::immediate_after_public(
                        window,
                        public.history_sequence,
                        independent.history_sequence,
                    )
            })
    })
}

fn mutation_operation_id(action: &ScenarioAction) -> Option<&OperationId> {
    match action {
        ScenarioAction::AlterConsumerGroupOffsets(value) => Some(&value.operation_id),
        ScenarioAction::DeleteConsumerGroupOffsets(value) => Some(&value.operation_id),
        _ => None,
    }
}
