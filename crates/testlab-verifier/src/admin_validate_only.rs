//! Validate-only verification proves public acceptance without a broker mutation.

use testlab_schema::{
    AlterTopicConfigAction, CreatePartitionsAction, CreateTopicAction, ScenarioAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::admin_validate_only_evidence::{config_violation, topic_violation};
use crate::index::{
    HistoryIndex, IndexedAdminTopicCompletion, IndexedAdminTopicConfigCompletion,
    IndexedTopicConfigObservation, IndexedTopicObservation,
};

pub(crate) fn verify_validate_only_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    match action {
        ScenarioAction::CreateTopic(action) if action.validate_only => {
            verify_topic_creation(action, index, violations);
        }
        ScenarioAction::CreatePartitions(action) if action.validate_only => {
            verify_partition_increase(action, index, violations);
        }
        ScenarioAction::AlterTopicConfig(action) if action.validate_only => {
            verify_config_alteration(action, index, violations);
        }
        _ => return false,
    }
    true
}

fn verify_topic_creation(
    action: &CreateTopicAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(&ScenarioAction::CreateTopic(action.clone()));
    let public = index
        .admin_validations
        .topic_creations
        .get(&action.operation_id);
    let mutation = index.topics_created.get(&action.operation_id);
    let independent = index.topics_observed.get(&action.operation_id);
    let public_value = exact_topic_completion(public, &action.topic, window);
    let state_matches =
        exact_topic_state(independent, &action.topic, false, &[], window, public_value);
    if public_value.is_some() && state_matches && is_empty(mutation) {
        return;
    }
    violations.push(topic_violation(
        "ADMIN-020",
        &action.operation_id,
        "topic creation validation",
        public,
        mutation,
        independent,
        None,
    ));
}

fn verify_partition_increase(
    action: &CreatePartitionsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let scenario_action = ScenarioAction::CreatePartitions(action.clone());
    let window = index.admin_command_window(&scenario_action);
    let public = index
        .admin_validations
        .partition_increases
        .get(&action.operation_id);
    let mutation = index.topic_partitions_created.get(&action.operation_id);
    let independent = index.topics_observed.get(&action.operation_id);
    let public_value = exact_topic_completion(public, &action.topic, window);
    let expected = action
        .expected_current_count
        .map(|count| (0..count).collect::<Vec<_>>());
    let state_matches = expected.as_deref().is_some_and(|partitions| {
        exact_topic_state(
            independent,
            &action.topic,
            true,
            partitions,
            window,
            public_value,
        )
    });
    let baseline = window.and_then(|(command, _)| {
        expected
            .as_deref()
            .and_then(|partitions| prior_topic_state(index, &action.topic, partitions, command))
    });
    if public_value.is_some() && state_matches && is_empty(mutation) && baseline.is_some() {
        return;
    }
    violations.push(topic_violation(
        "ADMIN-021",
        &action.operation_id,
        "partition-increase validation",
        public,
        mutation,
        independent,
        baseline.map(|value| value.observation),
    ));
}

fn verify_config_alteration(
    action: &AlterTopicConfigAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let scenario_action = ScenarioAction::AlterTopicConfig(action.clone());
    let window = index.admin_command_window(&scenario_action);
    let public = index
        .admin_validations
        .config_alterations
        .get(&action.operation_id);
    let mutation = index.topic_configs_altered.get(&action.operation_id);
    let independent = index.topic_configs_observed.get(&action.operation_id);
    let public_value = exact_config_completion(public, action, window);
    let state_matches = action
        .expected_current_value
        .as_deref()
        .is_some_and(|expected| {
            exact_config_state(independent, action, expected, window, public_value)
        });
    let baseline = window.and_then(|(command, _)| {
        action
            .expected_current_value
            .as_deref()
            .and_then(|expected| prior_config_state(index, action, expected, command))
    });
    if public_value.is_some() && state_matches && is_empty(mutation) && baseline.is_some() {
        return;
    }
    violations.push(config_violation(
        action,
        public,
        mutation,
        independent,
        baseline,
    ));
}

fn exact_topic_completion<'a>(
    values: Option<&'a Vec<IndexedAdminTopicCompletion>>,
    topic: &str,
    window: Option<AdminCommandWindow>,
) -> Option<&'a IndexedAdminTopicCompletion> {
    let [value] = values?.as_slice() else {
        return None;
    };
    (value.topic == topic && public_after_command(window, value.history_sequence)).then_some(value)
}

fn exact_topic_state(
    values: Option<&Vec<IndexedTopicObservation>>,
    topic: &str,
    exists: bool,
    partitions: &[i32],
    window: Option<AdminCommandWindow>,
    public: Option<&IndexedAdminTopicCompletion>,
) -> bool {
    let Some([value]) = values.map(Vec::as_slice) else {
        return false;
    };
    value.topic == topic
        && value.exists == exists
        && value.partitions == partitions
        && public.is_some_and(|public| {
            immediate_after_public(window, public.history_sequence, value.history_sequence)
        })
}

fn exact_config_completion<'a>(
    values: Option<&'a Vec<IndexedAdminTopicConfigCompletion>>,
    action: &AlterTopicConfigAction,
    window: Option<AdminCommandWindow>,
) -> Option<&'a IndexedAdminTopicConfigCompletion> {
    let [value] = values?.as_slice() else {
        return None;
    };
    (value.topic == action.topic
        && value.config_name == action.config_name
        && public_after_command(window, value.history_sequence))
    .then_some(value)
}

fn exact_config_state(
    values: Option<&Vec<IndexedTopicConfigObservation>>,
    action: &AlterTopicConfigAction,
    expected: &str,
    window: Option<AdminCommandWindow>,
    public: Option<&IndexedAdminTopicConfigCompletion>,
) -> bool {
    let Some([value]) = values.map(Vec::as_slice) else {
        return false;
    };
    value.topic == action.topic
        && value.config_name == action.config_name
        && value.value == expected
        && public.is_some_and(|public| {
            immediate_after_public(window, public.history_sequence, value.history_sequence)
        })
}

fn prior_topic_state<'a>(
    index: &'a HistoryIndex,
    topic: &str,
    partitions: &[i32],
    command: u64,
) -> Option<&'a IndexedTopicObservation> {
    index
        .topics_observed
        .values()
        .flatten()
        .filter(|value| {
            value.history_sequence < command
                && value.topic == topic
                && value.exists
                && value.partitions == partitions
        })
        .max_by_key(|value| value.history_sequence)
}

fn prior_config_state<'a>(
    index: &'a HistoryIndex,
    action: &AlterTopicConfigAction,
    expected: &str,
    command: u64,
) -> Option<&'a IndexedTopicConfigObservation> {
    index
        .topic_configs_observed
        .values()
        .flatten()
        .filter(|value| {
            value.history_sequence < command
                && value.topic == action.topic
                && value.config_name == action.config_name
                && value.value == expected
        })
        .max_by_key(|value| value.history_sequence)
}

fn is_empty<T>(values: Option<&Vec<T>>) -> bool {
    values.is_none_or(Vec::is_empty)
}
