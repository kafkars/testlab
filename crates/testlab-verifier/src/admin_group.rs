//! Consumer-group admin verification joins public claims to independent committed state.

use testlab_schema::{ListConsumerGroupOffsetsAction, ScenarioAction, Violation};

use crate::index::{
    HistoryIndex, IndexedConsumerGroupOffset, IndexedConsumerGroupOffsetObservation,
};
use crate::support::violation;

pub(crate) fn verify_group_offset_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::ListConsumerGroupOffsets(expected) = action else {
        return false;
    };
    let public = index
        .consumer_group_offsets_listed
        .get(&expected.operation_id);
    let independent = index
        .consumer_group_offsets_observed
        .get(&expected.operation_id);
    let (command_exact, command_count) = index.group_offset_command_state(expected);
    if command_count == 0
        && public.is_none()
        && independent.is_none()
        && !index.command_failures.is_empty()
    {
        return true;
    }
    if command_exact
        && public_matches(public, expected)
        && independent_matches(independent, expected)
    {
        return true;
    }
    violations.push(violation(
        "ADMIN-006",
        format!(
            "admin operation {} expected one exact command, one public result, and one independent committed offset {} for group {} at {}[{}]; observed {} same-operation command(s) with exact match {}, {} public and {} independent result(s)",
            expected.operation_id,
            expected.expected_offset,
            expected.group_id,
            expected.topic,
            expected.partition,
            command_count,
            command_exact,
            public.map_or(0, Vec::len),
            independent.map_or(0, Vec::len),
        ),
        Some(expected.operation_id.clone()),
        evidence(expected, public, independent),
    ));
    true
}

fn public_matches(
    values: Option<&Vec<IndexedConsumerGroupOffset>>,
    expected: &ListConsumerGroupOffsetsAction,
) -> bool {
    values.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.group_id == expected.group_id
                    && value.topic == expected.topic
                    && value.partition == expected.partition
                    && value.offset == Some(expected.expected_offset)
            })
    })
}

fn independent_matches(
    values: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    expected: &ListConsumerGroupOffsetsAction,
) -> bool {
    values.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.group_id == expected.group_id
                    && value.topic == expected.topic
                    && value.partition == expected.partition
                    && value.offset == Some(expected.expected_offset)
            })
    })
}

fn evidence(
    expected: &ListConsumerGroupOffsetsAction,
    public: Option<&Vec<IndexedConsumerGroupOffset>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
) -> Vec<String> {
    std::iter::once(format!("scenario:operation:{}", expected.operation_id))
        .chain(
            public
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence)),
        )
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.observation)),
        )
        .collect()
}
