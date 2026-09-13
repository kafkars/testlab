//! Topic listings retain public detail while independent metadata anchors required topics.

use std::collections::BTreeSet;

use testlab_schema::{AdminListedTopicOutcome, OperationId, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{IndexedTopicObservation, IndexedTopicsList};
use crate::support::violation;

pub(super) fn verify(
    operation_id: &OperationId,
    required_topics: &[String],
    include_authorized_operations: bool,
    completions: Option<&Vec<IndexedTopicsList>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let required: BTreeSet<&str> = required_topics.iter().map(String::as_str).collect();
    let independently_present: BTreeSet<&str> = independent
        .into_iter()
        .flatten()
        .filter(|value| value.exists && required.contains(value.topic.as_str()))
        .map(|value| value.topic.as_str())
        .collect();
    let public = completions
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let independent_matches = independent.is_some_and(|values| {
        values.len() == required.len()
            && independently_present.len() == required.len()
            && public.is_some_and(|public| {
                values.iter().all(|value| {
                    immediate_after_public(
                        command_window,
                        public.history_sequence,
                        value.history_sequence,
                    ) && required_outcome_matches(&public.outcomes, value)
                })
            })
    });
    let public_matches = public.is_some_and(|value| {
        sorted_unique(&value.outcomes)
            && value
                .outcomes
                .iter()
                .all(|outcome| valid_outcome(outcome, include_authorized_operations))
            && required_topics.iter().all(|topic| {
                value
                    .outcomes
                    .binary_search_by(|outcome| outcome.topic.as_str().cmp(topic))
                    .is_ok()
            })
            && public_after_command(command_window, value.history_sequence)
    });
    if public_matches && independent_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-004",
        format!(
            "admin operation {operation_id} expected one detailed sorted topic list with authorization option {include_authorized_operations} containing independently matched topics {required_topics:?}; independently present {independently_present:?}"
        ),
        Some(operation_id.clone()),
        super::evidence(
            completions.into_iter().flatten().map(|value| value.history_sequence),
            std::iter::empty(),
        ),
    ));
    super::append_state_evidence(violations, independent);
}

fn required_outcome_matches(
    outcomes: &[AdminListedTopicOutcome],
    observed: &IndexedTopicObservation,
) -> bool {
    let Ok(position) = outcomes.binary_search_by(|value| value.topic.cmp(&observed.topic)) else {
        return false;
    };
    outcomes[position]
        .description
        .as_ref()
        .is_some_and(|description| {
            description.partitions.len() == observed.partitions.len()
                && description.partitions.iter().zip(&observed.partitions).all(
                    |(public, observed)| {
                        public.partition == *observed && public.error_code.is_none()
                    },
                )
        })
        && outcomes[position].error_code.is_none()
}

fn valid_outcome(outcome: &AdminListedTopicOutcome, include_authorization: bool) -> bool {
    match (&outcome.description, &outcome.error_code) {
        (Some(description), None) => {
            description.authorized_operations.is_some() == include_authorization
                && description.topic_id.is_none_or(|id| id != [0; 16])
                && description
                    .partitions
                    .iter()
                    .all(|partition| partition.partition >= 0)
                && description
                    .partitions
                    .windows(2)
                    .all(|pair| pair[0].partition < pair[1].partition)
        }
        (None, Some(error)) => !error.is_empty(),
        _ => false,
    }
}

fn sorted_unique(outcomes: &[AdminListedTopicOutcome]) -> bool {
    outcomes
        .windows(2)
        .all(|pair| pair[0].topic.as_bytes() < pair[1].topic.as_bytes())
}
