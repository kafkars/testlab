//! Batch offset verification joins ordered public outcomes to independent watermarks.

use testlab_schema::{AdminOffsetPosition, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::admin_offset_batch::IndexedAdminOffsetsListing;
use crate::index::{HistoryIndex, IndexedPartitionOffsetsObservation};
use crate::support::violation;

pub(crate) fn verify_offset_batch_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::ListOffsetsBatch(action) = action else {
        return false;
    };
    let public = index
        .admin_offset_batches
        .offsets_listed
        .get(&action.operation_id);
    let independent = index.partition_offsets_observed.get(&action.operation_id);
    let window = index.admin_command_window(&ScenarioAction::ListOffsetsBatch(action.clone()));
    if batch_matches(action, public, independent, window) {
        return true;
    }
    violations.push(violation(
        "ADMIN-028",
        format!(
            "admin operation {} expected one caller-ordered batch offset listing with immediate independent watermarks",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        evidence(public, independent),
    ));
    true
}

fn batch_matches(
    expected: &testlab_schema::ListOffsetsBatchAction,
    public: Option<&Vec<IndexedAdminOffsetsListing>>,
    independent: Option<&Vec<IndexedPartitionOffsetsObservation>>,
    window: Option<AdminCommandWindow>,
) -> bool {
    let public = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    public.is_some_and(|public| {
        public_after_command(window, public.history_sequence)
            && public.outcomes.len() == expected.queries.len()
            && public
                .outcomes
                .iter()
                .zip(&expected.queries)
                .all(|(actual, expected)| {
                    actual.topic == expected.topic
                        && actual.partition == expected.partition
                        && actual.offset == Some(expected.expected_offset)
                        && actual.error_code.is_none()
                })
            && independent_matches(
                &expected.queries,
                independent,
                window,
                public.history_sequence,
            )
    })
}

fn independent_matches(
    expected: &[testlab_schema::OffsetListingExpectation],
    actual: Option<&Vec<IndexedPartitionOffsetsObservation>>,
    window: Option<AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    actual.is_some_and(|actual| {
        actual.len() == expected.len()
            && actual
                .windows(2)
                .all(|pair| pair[1].observation == pair[0].observation.saturating_add(1))
            && actual.iter().zip(expected).all(|(actual, expected)| {
                let selected = match expected.position {
                    AdminOffsetPosition::Earliest => actual.low_watermark,
                    AdminOffsetPosition::Latest => actual.high_watermark,
                };
                actual.topic == expected.topic
                    && actual.partition == expected.partition
                    && actual.low_watermark >= 0
                    && actual.high_watermark >= actual.low_watermark
                    && selected == expected.expected_offset
                    && immediate_after_public(window, public_sequence, actual.history_sequence)
            })
    })
}

fn evidence(
    public: Option<&Vec<IndexedAdminOffsetsListing>>,
    independent: Option<&Vec<IndexedPartitionOffsetsObservation>>,
) -> Vec<String> {
    public
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.observation)),
        )
        .collect()
}
