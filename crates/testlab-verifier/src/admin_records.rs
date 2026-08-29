//! Record deletion verification joins public low watermarks to independent transitions.

use testlab_schema::{ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedPartitionOffsetsObservation, IndexedRecordsDeleted};
use crate::support::violation;

pub(crate) fn verify_records_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::DeleteRecords(action) = action else {
        return false;
    };
    let command_window = index.admin_command_window(&ScenarioAction::DeleteRecords(action.clone()));
    let public = one(index.records_deleted.get(&action.operation_id));
    let post = one(index.partition_offsets_observed.get(&action.operation_id));
    let baseline = command_window
        .and_then(|(command, _)| prior_offsets(index, &action.topic, action.partition, command));
    let public_matches = public.is_some_and(|value| {
        value.topic == action.topic
            && value.partition == action.partition
            && value.low_watermark == action.before_offset
            && public_after_command(command_window, value.history_sequence)
    });
    let post_matches = post.is_some_and(|value| {
        offsets_match(
            value,
            &action.topic,
            action.partition,
            action.before_offset,
            action.expected_high_watermark,
        ) && public.is_some_and(|public| {
            immediate_after_public(
                command_window,
                public.history_sequence,
                value.history_sequence,
            )
        })
    });
    let baseline_matches = baseline.is_some_and(|value| {
        offsets_match(
            value,
            &action.topic,
            action.partition,
            0,
            action.expected_high_watermark,
        )
    });
    if public_matches && post_matches && baseline_matches {
        return true;
    }
    violations.push(violation(
        "ADMIN-017",
        format!(
            "admin operation {} expected {}[{}] watermarks 0..{} to become {}..{} with matching public low watermark",
            action.operation_id,
            action.topic,
            action.partition,
            action.expected_high_watermark,
            action.before_offset,
            action.expected_high_watermark
        ),
        Some(action.operation_id.clone()),
        evidence(public, baseline, post),
    ));
    true
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    values.filter(|values| values.len() == 1)?.first()
}

fn prior_offsets<'a>(
    index: &'a HistoryIndex,
    topic: &str,
    partition: i32,
    command_sequence: u64,
) -> Option<&'a IndexedPartitionOffsetsObservation> {
    index
        .partition_offsets_observed
        .values()
        .flatten()
        .filter(|value| {
            value.topic == topic
                && value.partition == partition
                && value.history_sequence < command_sequence
        })
        .max_by_key(|value| value.history_sequence)
}

fn offsets_match(
    value: &IndexedPartitionOffsetsObservation,
    topic: &str,
    partition: i32,
    low: i64,
    high: i64,
) -> bool {
    value.topic == topic
        && value.partition == partition
        && value.low_watermark == low
        && value.high_watermark == high
}

fn evidence(
    public: Option<&IndexedRecordsDeleted>,
    baseline: Option<&IndexedPartitionOffsetsObservation>,
    post: Option<&IndexedPartitionOffsetsObservation>,
) -> Vec<String> {
    public
        .map(|value| format!("history:{}", value.history_sequence))
        .into_iter()
        .chain(
            baseline
                .into_iter()
                .chain(post)
                .map(|value| format!("broker-state-observation:{}", value.observation)),
        )
        .collect()
}
