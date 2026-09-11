//! Plural topic-configuration calls join ordered public outcomes to broker reads.

use testlab_schema::{
    AlterTopicConfigsAction, DescribeTopicConfigsAction, Scenario, ScenarioAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::{
    HistoryIndex, IndexedAdminTopicConfigsDescription, IndexedTopicConfigObservation,
};
use crate::support::violation;

pub(crate) fn verify_config_batch_action(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    match scenario_action {
        ScenarioAction::DescribeTopicConfigs(action) => {
            verify_description(scenario_action, action, index, violations);
        }
        ScenarioAction::AlterTopicConfigs(action) => {
            verify_alteration(scenario, scenario_action, action, index, violations);
        }
        _ => return false,
    }
    true
}

fn verify_description(
    scenario_action: &ScenarioAction,
    action: &DescribeTopicConfigsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    if description_evidence(scenario_action, action, index).is_some() {
        return;
    }
    let public = index
        .topic_configs_batch_described
        .get(&action.operation_id);
    let independent = index.topic_configs_observed.get(&action.operation_id);
    violations.push(violation(
        description_contract(action.api),
        format!(
            "admin operation {} expected caller-ordered selected configuration values and immediate independent reads for every requested topic",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        references(public, independent),
    ));
}

fn verify_alteration(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    action: &AlterTopicConfigsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index.topic_configs_batch_altered.get(&action.operation_id));
    let independent = index.topic_configs_observed.get(&action.operation_id);
    let public_matches = public.is_some_and(|indexed| {
        public_after_command(window, indexed.history_sequence)
            && indexed.value.operation_id == action.operation_id
            && indexed.value.outcomes.len() == action.topics.len()
            && indexed
                .value
                .outcomes
                .iter()
                .zip(&action.topics)
                .all(|(actual, expected)| {
                    actual.topic == expected.topic
                        && actual.config_name == expected.config_name
                        && actual.error_code.is_none()
                })
    });
    let independent_matches = public.is_some_and(|public| {
        independent.is_some_and(|values| {
            values.len() == action.topics.len()
                && contiguous(values.iter().map(|value| value.observation))
                && contiguous(values.iter().map(|value| value.history_sequence))
                && values.iter().zip(&action.topics).all(|(actual, expected)| {
                    exact(
                        actual,
                        &expected.topic,
                        &expected.config_name,
                        &expected.value,
                    ) && immediate_after_public(
                        window,
                        public.history_sequence,
                        actual.history_sequence,
                    )
                })
        })
    });
    let baseline = baseline_evidence(scenario, action, index, window);
    if public_matches && independent_matches && baseline.is_some() {
        return;
    }
    let baseline_refs = baseline
        .into_iter()
        .flat_map(|evidence| evidence.independent.iter())
        .map(|value| format!("broker-state-observation:{}", value.observation));
    violations.push(violation(
        alteration_contract(action.api),
        format!(
            "admin operation {} expected one caller-ordered successful plural configuration replacement, immediate independent post-state, and exact distinct baseline {}",
            action.operation_id, action.baseline_operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(independent.into_iter().flatten().map(|value| {
                format!("broker-state-observation:{}", value.observation)
            }))
            .chain(baseline_refs)
            .collect(),
    ));
}

fn baseline_evidence<'a>(
    scenario: &'a Scenario,
    action: &AlterTopicConfigsAction,
    index: &'a HistoryIndex,
    window: Option<AdminCommandWindow>,
) -> Option<DescriptionEvidence<'a>> {
    let (mutation_command, _) = window?;
    let mutation_step = scenario.steps.iter().position(|step| {
        matches!(
            &step.action,
            ScenarioAction::AlterTopicConfigs(candidate)
                if candidate.operation_id == action.operation_id
        )
    })?;
    let baseline_step = scenario.steps[..mutation_step].iter().find(|step| {
        matches!(
            &step.action,
            ScenarioAction::DescribeTopicConfigs(candidate)
                if candidate.operation_id == action.baseline_operation_id
        )
    })?;
    let ScenarioAction::DescribeTopicConfigs(baseline) = &baseline_step.action else {
        return None;
    };
    if baseline.api != action.api.description_api()
        || baseline.topics.len() != action.topics.len()
        || !baseline
            .topics
            .iter()
            .zip(&action.topics)
            .all(|(before, after)| {
                before.topic == after.topic
                    && before.config_name == after.config_name
                    && before.expected_value == after.expected_previous_value
                    && before.expected_value != after.value
            })
    {
        return None;
    }
    let evidence = description_evidence(&baseline_step.action, baseline, index)?;
    if evidence
        .independent
        .iter()
        .zip(&action.topics)
        .any(|(observed, selected)| {
            observed.history_sequence >= mutation_command
                || index.topic_config_altered_between(
                    &selected.topic,
                    &selected.config_name,
                    observed.history_sequence,
                    mutation_command,
                )
        })
    {
        return None;
    }
    Some(evidence)
}

struct DescriptionEvidence<'a> {
    independent: &'a [IndexedTopicConfigObservation],
}

fn description_evidence<'a>(
    scenario_action: &ScenarioAction,
    action: &DescribeTopicConfigsAction,
    index: &'a HistoryIndex,
) -> Option<DescriptionEvidence<'a>> {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .topic_configs_batch_described
        .get(&action.operation_id))?;
    let independent = index.topic_configs_observed.get(&action.operation_id)?;
    let public_matches = public_after_command(window, public.history_sequence)
        && public.value.operation_id == action.operation_id
        && public.value.outcomes.len() == action.topics.len()
        && public
            .value
            .outcomes
            .iter()
            .zip(&action.topics)
            .all(|(actual, expected)| {
                actual.topic == expected.topic
                    && actual.config_name == expected.config_name
                    && actual.value.as_deref() == Some(expected.expected_value.as_str())
                    && actual.error_code.is_none()
            });
    let independent_matches = independent.len() == action.topics.len()
        && contiguous(independent.iter().map(|value| value.observation))
        && contiguous(independent.iter().map(|value| value.history_sequence))
        && independent
            .iter()
            .zip(&action.topics)
            .all(|(actual, expected)| {
                exact(
                    actual,
                    &expected.topic,
                    &expected.config_name,
                    &expected.expected_value,
                ) && immediate_after_public(
                    window,
                    public.history_sequence,
                    actual.history_sequence,
                )
            });
    (public_matches && independent_matches).then_some(DescriptionEvidence { independent })
}

fn exact(
    actual: &IndexedTopicConfigObservation,
    topic: &str,
    config_name: &str,
    value: &str,
) -> bool {
    actual.topic == topic && actual.config_name == config_name && actual.value == value
}

fn references(
    public: Option<&Vec<IndexedAdminTopicConfigsDescription>>,
    independent: Option<&Vec<IndexedTopicConfigObservation>>,
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

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}

fn description_contract(api: testlab_schema::TopicConfigApi) -> &'static str {
    match api {
        testlab_schema::TopicConfigApi::Topic => "ADMIN-048",
        testlab_schema::TopicConfigApi::Resource => "ADMIN-064",
    }
}

fn alteration_contract(api: testlab_schema::TopicConfigMutationApi) -> &'static str {
    match api {
        testlab_schema::TopicConfigMutationApi::Topic => "ADMIN-049",
        testlab_schema::TopicConfigMutationApi::Resource => "ADMIN-065",
        testlab_schema::TopicConfigMutationApi::LegacyTopic => "ADMIN-066",
        testlab_schema::TopicConfigMutationApi::LegacyResource => "ADMIN-067",
    }
}
