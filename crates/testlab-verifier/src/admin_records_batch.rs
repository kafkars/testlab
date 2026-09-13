//! Batched record deletion joins ordered public outcomes to independent ranges.

use testlab_schema::{DeleteRecordsBatchAction, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::admin_records::{offsets_match, prior_offsets};
use crate::index::{HistoryIndex, IndexedPartitionOffsetsObservation, IndexedRecordsDeleted};
use crate::support::violation;

pub(crate) fn verify_records_batch_action(
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::DeleteRecordsBatch(action) = scenario_action else {
        return false;
    };
    let window = index.admin_command_window(scenario_action);
    let public = index.records_deleted.get(&action.operation_id);
    let post = index.partition_offsets_observed.get(&action.operation_id);
    if batch_matches(action, public, post, window, index) {
        return true;
    }
    violations.push(violation(
        "ADMIN-047",
        format!(
            "admin operation {} expected caller-ordered public deletion outcomes and immediate independent watermark transitions for every target",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        evidence(action, index, window, public, post),
    ));
    true
}

fn batch_matches(
    action: &DeleteRecordsBatchAction,
    public: Option<&Vec<IndexedRecordsDeleted>>,
    post: Option<&Vec<IndexedPartitionOffsetsObservation>>,
    window: Option<AdminCommandWindow>,
    index: &HistoryIndex,
) -> bool {
    let Some((command, _)) = window else {
        return false;
    };
    let Some(public) = public.filter(|values| values.len() == action.targets.len()) else {
        return false;
    };
    let Some(public_sequence) = public.first().map(|value| value.history_sequence) else {
        return false;
    };
    let public_matches = public_after_command(window, public_sequence)
        && public
            .iter()
            .all(|value| value.history_sequence == public_sequence)
        && public
            .iter()
            .zip(&action.targets)
            .all(|(actual, expected)| {
                actual.topic == expected.topic
                    && actual.partition == expected.partition
                    && actual.low_watermark
                        == Some(
                            expected
                                .boundary
                                .expected_low_watermark(expected.expected_high_watermark),
                        )
                    && actual.error_code.is_none()
            });
    public_matches
        && baselines_match(action, index, command)
        && post_matches(action, post, window, public_sequence)
}

fn baselines_match(action: &DeleteRecordsBatchAction, index: &HistoryIndex, command: u64) -> bool {
    action.targets.iter().all(|target| {
        prior_offsets(index, &target.topic, target.partition, command).is_some_and(|value| {
            offsets_match(
                value,
                &target.topic,
                target.partition,
                0,
                target.expected_high_watermark,
            )
        })
    })
}

fn post_matches(
    action: &DeleteRecordsBatchAction,
    post: Option<&Vec<IndexedPartitionOffsetsObservation>>,
    window: Option<AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    post.is_some_and(|post| {
        post.len() == action.targets.len()
            && contiguous(post.iter().map(|value| value.observation))
            && contiguous(post.iter().map(|value| value.history_sequence))
            && post.iter().zip(&action.targets).all(|(actual, expected)| {
                offsets_match(
                    actual,
                    &expected.topic,
                    expected.partition,
                    expected
                        .boundary
                        .expected_low_watermark(expected.expected_high_watermark),
                    expected.expected_high_watermark,
                ) && immediate_after_public(window, public_sequence, actual.history_sequence)
            })
    })
}

fn contiguous(values: impl Iterator<Item = u64>) -> bool {
    let mut prior: Option<u64> = None;
    for value in values {
        if prior.is_some_and(|prior| value != prior.saturating_add(1)) {
            return false;
        }
        prior = Some(value);
    }
    true
}

fn evidence(
    action: &DeleteRecordsBatchAction,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    public: Option<&Vec<IndexedRecordsDeleted>>,
    post: Option<&Vec<IndexedPartitionOffsetsObservation>>,
) -> Vec<String> {
    let command = window.map(|(command, _)| command);
    action
        .targets
        .iter()
        .filter_map(|target| {
            prior_offsets(index, &target.topic, target.partition, command?)
                .map(|value| format!("broker-state-observation:{}", value.observation))
        })
        .chain(
            public
                .and_then(|values| values.first())
                .map(|value| format!("history:{}", value.history_sequence)),
        )
        .chain(
            post.into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.observation)),
        )
        .collect()
}
