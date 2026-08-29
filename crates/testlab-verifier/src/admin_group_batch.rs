//! Plural group-offset listings require exact ordered public and independent results.

use testlab_schema::{Scenario, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::admin_classic_groups::verify_classic_groups;
use crate::admin_group_batch_mutation::verify_group_offsets_mutation;
use crate::index::admin_group_batch::{
    IndexedConsumerGroupOffsetsListing, IndexedConsumerGroupsOffsetsListing,
};
use crate::index::{HistoryIndex, IndexedConsumerGroupOffsetObservation};
use crate::support::violation;

pub(crate) fn verify_group_batch_action(
    scenario: &Scenario,
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    match action {
        ScenarioAction::ListConsumerGroupOffsetsBatch(expected) => verify_batch_listing(
            expected,
            index
                .admin_group_batches
                .offsets_listed
                .get(&expected.operation_id),
            index
                .consumer_group_offsets_observed
                .get(&expected.operation_id),
            index.admin_command_window(action),
            violations,
        ),
        ScenarioAction::ListConsumerGroupsOffsets(expected) => verify_groups_listing(
            expected,
            index
                .admin_group_batches
                .groups_offsets_listed
                .get(&expected.operation_id),
            index
                .consumer_group_offsets_observed
                .get(&expected.operation_id),
            index.admin_command_window(action),
            violations,
        ),
        ScenarioAction::AlterConsumerGroupOffsets(_)
        | ScenarioAction::DeleteConsumerGroupOffsets(_) => {
            verify_group_offsets_mutation(scenario, action, index, violations);
        }
        ScenarioAction::DescribeClassicGroups(_) => {
            verify_classic_groups(scenario, action, index, violations);
        }
        _ => return false,
    }
    true
}

fn verify_batch_listing(
    expected: &testlab_schema::ListConsumerGroupOffsetsBatchAction,
    public: Option<&Vec<IndexedConsumerGroupOffsetsListing>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    if batch_listing_matches(expected, public, independent, window) {
        return;
    }
    violations.push(listing_violation(
        "ADMIN-023",
        &expected.operation_id,
        "one exact ordered multi-partition group-offset listing",
        public
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence),
        independent,
    ));
}

fn verify_groups_listing(
    expected: &testlab_schema::ListConsumerGroupsOffsetsAction,
    public: Option<&Vec<IndexedConsumerGroupsOffsetsListing>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    if groups_listing_matches(expected, public, independent, window) {
        return;
    }
    violations.push(listing_violation(
        "ADMIN-024",
        &expected.operation_id,
        "one exact ordered multi-group selected-offset listing",
        public
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence),
        independent,
    ));
}

pub(crate) fn batch_listing_matches(
    expected: &testlab_schema::ListConsumerGroupOffsetsBatchAction,
    public: Option<&Vec<IndexedConsumerGroupOffsetsListing>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    window: Option<AdminCommandWindow>,
) -> bool {
    let public_value = one(public);
    public_value.is_some_and(|value| {
        value.group_id == expected.group_id
            && public_after_command(window, value.history_sequence)
            && exact_public_offsets(&expected.partitions, &value.outcomes)
            && exact_independent_offsets(
                &expected.group_id,
                &expected.partitions,
                independent,
                window,
                value.history_sequence,
            )
    })
}

pub(crate) fn groups_listing_matches(
    expected: &testlab_schema::ListConsumerGroupsOffsetsAction,
    public: Option<&Vec<IndexedConsumerGroupsOffsetsListing>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    window: Option<AdminCommandWindow>,
) -> bool {
    let public_value = one(public);
    public_value.is_some_and(|value| {
        value.groups.len() == expected.groups.len()
            && public_after_command(window, value.history_sequence)
            && value
                .groups
                .iter()
                .zip(&expected.groups)
                .all(|(actual, expected)| {
                    actual.group_id == expected.group_id
                        && actual.error_code.is_none()
                        && exact_public_offsets(&expected.partitions, &actual.offsets)
                })
            && exact_independent_groups(expected, independent, window, value.history_sequence)
    })
}

fn exact_public_offsets(
    expected: &[testlab_schema::ConsumerGroupOffsetExpectation],
    actual: &[testlab_schema::AdminConsumerGroupOffsetOutcome],
) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            actual.topic == expected.topic
                && actual.partition == expected.partition
                && actual.offset == Some(expected.expected_offset)
                && actual.error_code.is_none()
        })
}

fn exact_independent_offsets(
    group_id: &str,
    expected: &[testlab_schema::ConsumerGroupOffsetExpectation],
    actual: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    window: Option<AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    actual.is_some_and(|actual| {
        actual.len() == expected.len()
            && contiguous_observations(actual)
            && actual.iter().zip(expected).all(|(actual, expected)| {
                actual.group_id == group_id
                    && actual.topic == expected.topic
                    && actual.partition == expected.partition
                    && actual.offset == Some(expected.expected_offset)
                    && immediate_after_public(window, public_sequence, actual.history_sequence)
            })
    })
}

fn exact_independent_groups(
    expected: &testlab_schema::ListConsumerGroupsOffsetsAction,
    actual: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
    window: Option<AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    let flattened = expected.groups.iter().flat_map(|group| {
        group
            .partitions
            .iter()
            .map(move |partition| (group.group_id.as_str(), partition))
    });
    actual.is_some_and(|actual| {
        let expected_len = expected
            .groups
            .iter()
            .map(|group| group.partitions.len())
            .sum::<usize>();
        actual.len() == expected_len
            && contiguous_observations(actual)
            && actual
                .iter()
                .zip(flattened)
                .all(|(actual, (group, expected))| {
                    actual.group_id == group
                        && actual.topic == expected.topic
                        && actual.partition == expected.partition
                        && actual.offset == Some(expected.expected_offset)
                        && immediate_after_public(window, public_sequence, actual.history_sequence)
                })
    })
}

pub(crate) fn contiguous_observations(values: &[IndexedConsumerGroupOffsetObservation]) -> bool {
    values
        .windows(2)
        .all(|pair| pair[1].observation == pair[0].observation.saturating_add(1))
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    values.filter(|values| values.len() == 1)?.first()
}

fn listing_violation(
    contract: &str,
    operation_id: &testlab_schema::OperationId,
    expectation: &str,
    public_sequences: impl IntoIterator<Item = u64>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
) -> Violation {
    violation(
        contract,
        format!(
            "admin operation {operation_id} expected {expectation} with immediate independent broker facts"
        ),
        Some(operation_id.clone()),
        public_sequences
            .into_iter()
            .map(|sequence| format!("history:{sequence}"))
            .chain(
                independent
                    .into_iter()
                    .flatten()
                    .map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .collect(),
    )
}
