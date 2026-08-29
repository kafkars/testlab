//! Topic-configuration verification joins public results to immediate independent reads.

use testlab_schema::{OperationId, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{
    HistoryIndex, IndexedAdminTopicConfigCompletion, IndexedTopicConfigDescription,
    IndexedTopicConfigObservation,
};
use crate::support::violation;

pub(crate) fn verify_config_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let window = index.admin_command_window(action);
    match action {
        ScenarioAction::DescribeTopicConfig(action) => verify_description(
            &action.operation_id,
            &action.topic,
            &action.config_name,
            &action.expected_value,
            index.topic_configs_described.get(&action.operation_id),
            index.topic_configs_observed.get(&action.operation_id),
            window,
            violations,
        ),
        ScenarioAction::AlterTopicConfig(action) if !action.validate_only => verify_alteration(
            &action.operation_id,
            &action.topic,
            &action.config_name,
            &action.value,
            index.topic_configs_altered.get(&action.operation_id),
            index.topic_configs_observed.get(&action.operation_id),
            index,
            window,
            violations,
        ),
        _ => return false,
    }
    true
}

#[allow(
    clippy::too_many_arguments,
    reason = "the verifier keeps exact configuration identity and evidence explicit"
)]
fn verify_description(
    operation_id: &OperationId,
    topic: &str,
    config_name: &str,
    expected_value: &str,
    public: Option<&Vec<IndexedTopicConfigDescription>>,
    independent: Option<&Vec<IndexedTopicConfigObservation>>,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        exact(
            topic,
            config_name,
            expected_value,
            &value.topic,
            &value.config_name,
            value.value.as_deref(),
        ) && public_after_command(window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                exact(
                    topic,
                    config_name,
                    expected_value,
                    &value.topic,
                    &value.config_name,
                    Some(&value.value),
                ) && public_value.is_some_and(|public| {
                    immediate_after_public(window, public.history_sequence, value.history_sequence)
                })
            })
    });
    if !(public_matches && independent_matches) {
        violations.push(config_violation(
            "ADMIN-015",
            operation_id,
            topic,
            config_name,
            expected_value,
            public
                .into_iter()
                .flatten()
                .map(|value| value.history_sequence),
            independent,
            None,
        ));
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the verifier keeps exact configuration identity and evidence explicit"
)]
fn verify_alteration(
    operation_id: &OperationId,
    topic: &str,
    config_name: &str,
    expected_value: &str,
    public: Option<&Vec<IndexedAdminTopicConfigCompletion>>,
    independent: Option<&Vec<IndexedTopicConfigObservation>>,
    index: &HistoryIndex,
    window: Option<AdminCommandWindow>,
    violations: &mut Vec<Violation>,
) {
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        value.topic == topic
            && value.config_name == config_name
            && public_after_command(window, value.history_sequence)
    });
    let independent_matches = independent.is_some_and(|values| {
        values.len() == 1
            && values.first().is_some_and(|value| {
                exact(
                    topic,
                    config_name,
                    expected_value,
                    &value.topic,
                    &value.config_name,
                    Some(&value.value),
                ) && public_value.is_some_and(|public| {
                    immediate_after_public(window, public.history_sequence, value.history_sequence)
                })
            })
    });
    let baseline = prior_distinct_description(index, topic, config_name, expected_value, window);
    if !(public_matches && independent_matches && baseline.is_some()) {
        violations.push(config_violation(
            "ADMIN-016",
            operation_id,
            topic,
            config_name,
            expected_value,
            public
                .into_iter()
                .flatten()
                .map(|value| value.history_sequence),
            independent,
            baseline,
        ));
    }
}

fn exact(
    topic: &str,
    config_name: &str,
    expected_value: &str,
    actual_topic: &str,
    actual_name: &str,
    actual_value: Option<&str>,
) -> bool {
    actual_topic == topic && actual_name == config_name && actual_value == Some(expected_value)
}

fn prior_distinct_description<'a>(
    index: &'a HistoryIndex,
    topic: &str,
    config_name: &str,
    replacement: &str,
    window: Option<AdminCommandWindow>,
) -> Option<&'a IndexedTopicConfigObservation> {
    let (alter_command, _) = window?;
    index
        .topic_configs_described
        .iter()
        .filter_map(|(operation_id, public)| {
            let [public] = public.as_slice() else {
                return None;
            };
            let [observed] = index.topic_configs_observed.get(operation_id)?.as_slice() else {
                return None;
            };
            (public.history_sequence < observed.history_sequence
                && observed.history_sequence < alter_command
                && exact(
                    topic,
                    config_name,
                    &observed.value,
                    &public.topic,
                    &public.config_name,
                    public.value.as_deref(),
                )
                && observed.topic == topic
                && observed.config_name == config_name
                && observed.value != replacement
                && !index.topic_config_altered_between(
                    topic,
                    config_name,
                    observed.history_sequence,
                    alter_command,
                ))
            .then_some(observed)
        })
        .max_by_key(|observed| observed.history_sequence)
}

#[allow(
    clippy::too_many_arguments,
    reason = "configuration violations retain exact public, independent, and baseline evidence"
)]
fn config_violation(
    contract: &str,
    operation_id: &OperationId,
    topic: &str,
    config_name: &str,
    expected_value: &str,
    public_sequences: impl Iterator<Item = u64>,
    independent: Option<&Vec<IndexedTopicConfigObservation>>,
    baseline: Option<&IndexedTopicConfigObservation>,
) -> Violation {
    violation(
        contract,
        format!(
            "admin operation {operation_id} expected public completion and independent {topic} configuration {config_name}={expected_value:?}"
        ),
        Some(operation_id.clone()),
        public_sequences
            .map(|sequence| format!("history:{sequence}"))
            .chain(
                independent
                    .into_iter()
                    .flatten()
                    .map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .chain(
                baseline
                    .into_iter()
                    .map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .collect(),
    )
}
