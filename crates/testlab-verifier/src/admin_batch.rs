//! Batch topic verification joins ordered public outcomes to per-topic broker topology.

use testlab_schema::{AdminTopicCreationOutcome, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedAdminTopicsCreationBatch, IndexedTopicObservation};
use crate::support::violation;

pub(crate) fn verify_batch_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::CreateTopicsBatch(action) = action else {
        return false;
    };
    let public = index.topics_creation_completed.get(&action.operation_id);
    let independent = index.topics_observed.get(&action.operation_id);
    let window = index.admin_command_window(&ScenarioAction::CreateTopicsBatch(action.clone()));
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let expected_outcomes = action
        .topics
        .iter()
        .map(|item| AdminTopicCreationOutcome {
            topic: item.topic.clone(),
            error_code: item.expected_error_code.clone(),
        })
        .collect::<Vec<_>>();
    let public_matches = public_value.is_some_and(|value| {
        value.outcomes == expected_outcomes && public_after_command(window, value.history_sequence)
    });
    let independent_matches = public_value.is_some_and(|completion| {
        topology_matches(action, independent, window, completion.history_sequence)
    });
    if !public_matches || !independent_matches {
        violations.push(violation(
            "ADMIN-018",
            format!(
                "admin operation {} expected ordered topic outcomes {:?} and exact independent topology for every batch item",
                action.operation_id, expected_outcomes
            ),
            Some(action.operation_id.clone()),
            evidence(public, independent),
        ));
    }
    true
}

fn topology_matches(
    action: &testlab_schema::CreateTopicsBatchAction,
    independent: Option<&Vec<IndexedTopicObservation>>,
    window: Option<crate::admin::AdminCommandWindow>,
    public_sequence: u64,
) -> bool {
    independent.is_some_and(|values| {
        values.len() == action.topics.len()
            && action.topics.iter().zip(values).all(|(expected, actual)| {
                actual.topic == expected.topic
                    && actual.exists
                    && actual.partitions == (0..expected.partitions).collect::<Vec<_>>()
                    && immediate_after_public(window, public_sequence, actual.history_sequence)
            })
    })
}

fn evidence(
    public: Option<&Vec<IndexedAdminTopicsCreationBatch>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
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
