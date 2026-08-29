//! Plural group-offset mutations join ordered public outcomes to proven state changes.

use testlab_schema::{OperationId, Scenario, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::admin_group_baseline::has_prior_baseline;
use crate::admin_group_batch::contiguous_observations;
use crate::index::admin_group_batch::IndexedConsumerGroupOffsetsMutation;
use crate::index::{HistoryIndex, IndexedConsumerGroupOffsetObservation};
use crate::support::violation;

struct ExpectedOffset<'a> {
    topic: &'a str,
    partition: i32,
    offset: Option<i64>,
}

pub(crate) fn verify_group_offsets_mutation(
    scenario: &Scenario,
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    match action {
        ScenarioAction::AlterConsumerGroupOffsets(expected) => {
            let offsets = expected
                .offsets
                .iter()
                .map(|value| ExpectedOffset {
                    topic: &value.topic,
                    partition: value.partition,
                    offset: Some(value.offset),
                })
                .collect::<Vec<_>>();
            verify(
                "ADMIN-025",
                scenario,
                index,
                action,
                &expected.operation_id,
                &expected.group_id,
                &offsets,
                index
                    .admin_group_batches
                    .offsets_altered
                    .get(&expected.operation_id),
                violations,
            );
        }
        ScenarioAction::DeleteConsumerGroupOffsets(expected) => {
            let offsets = expected
                .partitions
                .iter()
                .map(|value| ExpectedOffset {
                    topic: &value.topic,
                    partition: value.partition,
                    offset: None,
                })
                .collect::<Vec<_>>();
            verify(
                "ADMIN-026",
                scenario,
                index,
                action,
                &expected.operation_id,
                &expected.group_id,
                &offsets,
                index
                    .admin_group_batches
                    .offsets_deleted
                    .get(&expected.operation_id),
                violations,
            );
        }
        _ => {}
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "mutation verification keeps each exact evidence source explicit"
)]
fn verify(
    contract: &str,
    scenario: &Scenario,
    index: &HistoryIndex,
    action: &ScenarioAction,
    operation_id: &OperationId,
    group_id: &str,
    expected: &[ExpectedOffset<'_>],
    public: Option<&Vec<IndexedConsumerGroupOffsetsMutation>>,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(action);
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        value.group_id == group_id
            && value.outcomes.len() == expected.len()
            && public_after_command(window, value.history_sequence)
            && value
                .outcomes
                .iter()
                .zip(expected)
                .all(|(actual, expected)| {
                    actual.topic == expected.topic
                        && actual.partition == expected.partition
                        && actual.error_code.is_none()
                })
    });
    let independent = index.consumer_group_offsets_observed.get(operation_id);
    let independent_matches = public_value.is_some_and(|public| {
        exact_independent(
            group_id,
            expected,
            independent,
            window,
            public.history_sequence,
        )
    });
    let baselines_match = expected.iter().all(|expected| {
        has_prior_baseline(
            scenario,
            index,
            operation_id,
            group_id,
            expected.topic,
            expected.partition,
            expected.offset,
        )
    });
    if public_matches && independent_matches && baselines_match {
        return;
    }
    violations.push(violation(
        contract,
        format!(
            "admin operation {operation_id} expected one exact ordered plural offset mutation, immediate independent post-state, and a distinct corroborated baseline for every partition"
        ),
        Some(operation_id.clone()),
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
            .collect(),
    ));
}

fn exact_independent(
    group_id: &str,
    expected: &[ExpectedOffset<'_>],
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
                    && actual.offset == expected.offset
                    && immediate_after_public(window, public_sequence, actual.history_sequence)
            })
    })
}
