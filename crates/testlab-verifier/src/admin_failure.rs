//! Expected admin failures require exact public errors and unchanged broker state.

use testlab_schema::{ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedTopicObservation};
use crate::support::violation;

pub(crate) fn verify_expected_failure(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let Some((operation_id, expected_code)) = testlab_schema::expected_admin_error(action) else {
        return false;
    };
    let contract = if matches!(action, ScenarioAction::CreateTopic(_)) {
        "ADMIN-014"
    } else {
        "ADMIN-019"
    };
    let command_window = index.admin_command_window(action);
    let failures = index.admin_command_failures(action);
    let failure = failures
        .as_slice()
        .first()
        .copied()
        .filter(|_| failures.len() == 1);
    let failure_matches = failure.is_some_and(|value| {
        value.code == expected_code && public_after_command(command_window, value.history_sequence)
    });
    let successes = success_sequences(action, index);
    let independent = index.topics_observed.get(operation_id);
    let state_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                expected_state(action, value)
                    && failure.is_some_and(|failure| {
                        immediate_after_public(
                            command_window,
                            failure.history_sequence,
                            value.history_sequence,
                        )
                    })
            })
    });
    if failure_matches && successes.is_empty() && state_matches {
        return true;
    }
    violations.push(violation(
        contract,
        format!(
            "admin operation {operation_id} expected exact error {expected_code}, no success completion, and unchanged independent topic state"
        ),
        Some(operation_id.clone()),
        evidence(&successes, &failures, independent),
    ));
    true
}

fn expected_state(action: &ScenarioAction, observed: &IndexedTopicObservation) -> bool {
    match action {
        ScenarioAction::CreateTopic(action) => {
            if action.expected_error_code.as_deref()
                == Some(testlab_schema::ADMIN_TOPIC_AUTHORIZATION_ERROR_CODE)
            {
                absent(&action.topic, observed)
            } else {
                observed.topic == action.topic
                    && observed.exists
                    && observed.partitions == (0..action.partitions).collect::<Vec<_>>()
            }
        }
        ScenarioAction::CreatePartitions(action) => absent(&action.topic, observed),
        ScenarioAction::DeleteTopic(action) => absent(&action.topic, observed),
        ScenarioAction::DescribeTopic(action) => absent(&action.topic, observed),
        ScenarioAction::ListOffsets(action) => {
            observed.topic == action.topic
                && observed.exists
                && observed.partitions == (0..action.partition).collect::<Vec<_>>()
        }
        _ => false,
    }
}

fn absent(topic: &str, observed: &IndexedTopicObservation) -> bool {
    observed.topic == topic && !observed.exists && observed.partitions.is_empty()
}

fn success_sequences(action: &ScenarioAction, index: &HistoryIndex) -> Vec<u64> {
    match action {
        ScenarioAction::CreateTopic(action) => index
            .topics_created
            .get(&action.operation_id)
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence)
            .collect(),
        ScenarioAction::CreatePartitions(action) => index
            .topic_partitions_created
            .get(&action.operation_id)
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence)
            .collect(),
        ScenarioAction::DeleteTopic(action) => index
            .topics_deleted
            .get(&action.operation_id)
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence)
            .collect(),
        ScenarioAction::DescribeTopic(action) => index
            .topics_described
            .get(&action.operation_id)
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence)
            .collect(),
        ScenarioAction::ListOffsets(action) => index
            .offsets_listed
            .get(&action.operation_id)
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence)
            .collect(),
        _ => Vec::new(),
    }
}

fn evidence(
    successes: &[u64],
    failures: &[&crate::index::IndexedCommandFailure],
    independent: Option<&Vec<IndexedTopicObservation>>,
) -> Vec<String> {
    successes
        .iter()
        .map(|sequence| format!("history:{sequence}"))
        .chain(
            failures
                .iter()
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
