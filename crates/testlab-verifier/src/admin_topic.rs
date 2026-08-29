//! Topic mutation verification requires one public result and one immediate metadata snapshot.

use testlab_schema::{OperationId, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedAdminTopicCompletion, IndexedTopicObservation};
use crate::support::violation;

pub(crate) fn verify_topic_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    match action {
        ScenarioAction::CreateTopic(action)
            if action.expected_error_code.is_none() && !action.validate_only =>
        {
            verify(
                "ADMIN-001",
                "topic creation",
                &action.operation_id,
                &action.topic,
                Some((0..action.partitions).collect()),
                index.topics_created.get(&action.operation_id),
                index.topics_observed.get(&action.operation_id),
                command_window,
                violations,
            );
        }
        ScenarioAction::CreatePartitions(action)
            if action.expected_error_code.is_none() && !action.validate_only =>
        {
            verify(
                "ADMIN-002",
                "partition creation",
                &action.operation_id,
                &action.topic,
                Some((0..action.total_count).collect()),
                index.topic_partitions_created.get(&action.operation_id),
                index.topics_observed.get(&action.operation_id),
                command_window,
                violations,
            );
        }
        ScenarioAction::DeleteTopic(action) if action.expected_error_code.is_none() => verify(
            "ADMIN-007",
            "topic deletion",
            &action.operation_id,
            &action.topic,
            None,
            index.topics_deleted.get(&action.operation_id),
            index.topics_observed.get(&action.operation_id),
            command_window,
            violations,
        ),
        _ => return false,
    }
    true
}

#[allow(
    clippy::too_many_arguments,
    clippy::needless_pass_by_value,
    reason = "the verifier joins explicit fields and owns its derived expected partition vector"
)]
fn verify(
    contract: &str,
    operation: &str,
    operation_id: &OperationId,
    topic: &str,
    expected_partitions: Option<Vec<i32>>,
    public: Option<&Vec<IndexedAdminTopicCompletion>>,
    independent: Option<&Vec<IndexedTopicObservation>>,
    command_window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        value.topic == topic && public_after_command(command_window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                value.topic == topic
                    && public_value.is_some_and(|public| {
                        immediate_after_public(
                            command_window,
                            public.history_sequence,
                            value.history_sequence,
                        )
                    })
                    && match &expected_partitions {
                        Some(partitions) => value.exists && &value.partitions == partitions,
                        None => !value.exists && value.partitions.is_empty(),
                    }
            })
    });
    if public_matches && independent_matches {
        return;
    }
    let expectation = expected_partitions.as_ref().map_or_else(
        || "absent".to_owned(),
        |value| format!("present with partitions {value:?}"),
    );
    violations.push(violation(
        contract,
        format!("admin operation {operation_id} expected one exact {operation} for {topic} and independent state {expectation}"),
        Some(operation_id.clone()),
        evidence(public, independent),
    ));
}

fn evidence(
    public: Option<&Vec<IndexedAdminTopicCompletion>>,
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
