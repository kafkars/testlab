//! Plural topic deletion joins caller-ordered outcomes to before-and-after broker facts.

use testlab_schema::{
    DeleteTopicExpectation, DeleteTopicsAction, DescribeTopicExpectation, DescribeTopicsAction,
    Scenario, ScenarioAction, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::{HistoryIndex, IndexedAdminTopicsDeletion, IndexedTopicObservation};
use crate::support::violation;

pub(crate) fn verify_topics_deletion_action(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let ScenarioAction::DeleteTopics(action) = scenario_action else {
        return false;
    };
    verify(scenario, scenario_action, action, index, violations);
    true
}

fn verify(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    action: &DeleteTopicsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index.topics_batch_deleted.get(&action.operation_id));
    let description = prior_description(scenario, action);
    let baseline = description.and_then(|value| index.topics_observed.get(&value.operation_id));
    let observed = index.topics_observed.get(&action.operation_id);
    let public_matches = public.is_some_and(|value| {
        public_after_command(window, value.history_sequence)
            && value.value.operation_id == action.operation_id
            && value.value.outcomes.len() == action.topics.len()
            && value
                .value
                .outcomes
                .iter()
                .zip(&action.topics)
                .all(|(actual, expected)| {
                    actual.topic == expected.topic
                        && actual.error_code == expected.expected_error_code
                })
    });
    let baseline_matches = description.is_some_and(|description| {
        baseline.is_some_and(|values| baseline_is_exact(values, description, window))
    });
    let observed_matches = public.is_some_and(|public| {
        observed.is_some_and(|values| post_delete_is_exact(values, action, window, public))
    });
    if public_matches && baseline_matches && observed_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-045",
        format!(
            "admin operation {} expected prior caller-ordered topic state, exact mixed deletion outcomes, and immediate independent absence for every requested topic",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        baseline
            .into_iter()
            .flatten()
            .map(observation_evidence)
            .chain(
                public
                    .map(|value| format!("history:{}", value.history_sequence)),
            )
            .chain(observed.into_iter().flatten().map(observation_evidence))
            .collect(),
    ));
}

fn prior_description<'a>(
    scenario: &'a Scenario,
    deletion: &DeleteTopicsAction,
) -> Option<&'a DescribeTopicsAction> {
    let mut matching = None;
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DeleteTopics(action)
                if action.operation_id == deletion.operation_id =>
            {
                break;
            }
            ScenarioAction::DescribeTopics(action) if expectations_match(action, deletion) => {
                matching = Some(action);
            }
            _ => {}
        }
    }
    matching
}

fn expectations_match(description: &DescribeTopicsAction, deletion: &DeleteTopicsAction) -> bool {
    description.topics.len() == deletion.topics.len()
        && description
            .topics
            .iter()
            .zip(&deletion.topics)
            .all(|(before, deleted)| {
                before.topic == deleted.topic && expectation_matches(before, deleted)
            })
}

fn expectation_matches(
    before: &DescribeTopicExpectation,
    deleted: &DeleteTopicExpectation,
) -> bool {
    match deleted.expected_error_code.as_deref() {
        None => before.expected_partitions.is_some() && before.expected_error_code.is_none(),
        Some(error) => {
            before.expected_partitions.is_none()
                && before.expected_error_code.as_deref() == Some(error)
        }
    }
}

fn baseline_is_exact(
    values: &[IndexedTopicObservation],
    description: &DescribeTopicsAction,
    window: Option<crate::admin::AdminCommandWindow>,
) -> bool {
    let command = window.map(|(command, _)| command);
    values.len() == description.topics.len()
        && contiguous(values.iter().map(|value| value.observation))
        && contiguous(values.iter().map(|value| value.history_sequence))
        && values
            .iter()
            .zip(&description.topics)
            .all(|(actual, expected)| {
                command.is_some_and(|command| actual.history_sequence < command)
                    && actual.topic == expected.topic
                    && match expected.expected_partitions.as_deref() {
                        Some(partitions) => actual.exists && actual.partitions == partitions,
                        None => !actual.exists && actual.partitions.is_empty(),
                    }
            })
}

fn post_delete_is_exact(
    values: &[IndexedTopicObservation],
    action: &DeleteTopicsAction,
    window: Option<crate::admin::AdminCommandWindow>,
    public: &IndexedAdminTopicsDeletion,
) -> bool {
    values.len() == action.topics.len()
        && contiguous(values.iter().map(|value| value.observation))
        && contiguous(values.iter().map(|value| value.history_sequence))
        && values.iter().zip(&action.topics).all(|(actual, expected)| {
            actual.topic == expected.topic
                && !actual.exists
                && actual.partitions.is_empty()
                && immediate_after_public(window, public.history_sequence, actual.history_sequence)
        })
}

fn contiguous(values: impl Iterator<Item = u64>) -> bool {
    let mut previous: Option<u64> = None;
    for value in values {
        if previous.is_some_and(|previous| value != previous.saturating_add(1)) {
            return false;
        }
        previous = Some(value);
    }
    true
}

fn observation_evidence(value: &IndexedTopicObservation) -> String {
    format!("broker-state-observation:{}", value.observation)
}

fn one(values: Option<&Vec<IndexedAdminTopicsDeletion>>) -> Option<&IndexedAdminTopicsDeletion> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
