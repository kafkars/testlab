//! Configured batch items require ordered public and independent value proof.

use testlab_schema::{
    CreateTopicBatchActionItem, CreateTopicsBatchAction, Scenario, ScenarioAction,
    TopicCreationConfig, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(
    scenario: &Scenario,
    action: &CreateTopicsBatchAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    if !action
        .topics
        .iter()
        .any(|item| item.expected_error_code.is_none() && !item.configs.is_empty())
    {
        return;
    }
    let creation_step = scenario.steps.iter().position(|step| {
        matches!(
            &step.action,
            ScenarioAction::CreateTopicsBatch(value) if value.operation_id == action.operation_id
        )
    });
    let created = index
        .topics_observed
        .get(&action.operation_id)
        .filter(|values| values.len() == action.topics.len());
    let mut evidence = Vec::new();
    let mut exact = creation_step.is_some() && created.is_some();

    for (item_index, item) in action
        .topics
        .iter()
        .enumerate()
        .filter(|(_, item)| item.expected_error_code.is_none() && !item.configs.is_empty())
    {
        let item_state = created.and_then(|values| values.get(item_index));
        evidence.extend(
            item_state.map(|value| format!("broker-state-observation:{}", value.observation)),
        );
        exact &= item_state.is_some_and(|value| {
            value.topic == item.topic
                && value.exists
                && value.partitions == (0..item.partitions).collect::<Vec<_>>()
        });
        exact &= verify_item(
            scenario,
            creation_step.unwrap_or(scenario.steps.len()),
            item,
            item_state.map(|value| value.history_sequence),
            index,
            &mut evidence,
        );
    }

    if !exact {
        violations.push(violation(
            "ADMIN-096",
            format!(
                "admin operation {} expected every successful configured batch item to preserve ordered creation configs and have exact later public and independent values",
                action.operation_id
            ),
            Some(action.operation_id.clone()),
            evidence,
        ));
    }
}

fn verify_item(
    scenario: &Scenario,
    creation_step: usize,
    item: &CreateTopicBatchActionItem,
    created_sequence: Option<u64>,
    index: &HistoryIndex,
    evidence: &mut Vec<String>,
) -> bool {
    let mut prior_step = creation_step;
    let mut exact = created_sequence.is_some();
    for config in &item.configs {
        let Some((step_index, step)) = later_description(scenario, prior_step, item, config) else {
            return false;
        };
        let ScenarioAction::DescribeTopicConfig(description) = &step.action else {
            unreachable!("later_description returns only configuration descriptions");
        };
        let window = index.admin_command_window(&step.action);
        let public = index
            .topic_configs_described
            .get(&description.operation_id)
            .filter(|values| values.len() == 1)
            .and_then(|values| values.first());
        let independent = index
            .topic_configs_observed
            .get(&description.operation_id)
            .filter(|values| values.len() == 1)
            .and_then(|values| values.first());
        evidence.extend(
            public
                .map(|value| format!("history:{}", value.history_sequence))
                .into_iter(),
        );
        evidence.extend(
            independent
                .map(|value| format!("broker-state-observation:{}", value.observation))
                .into_iter(),
        );
        exact &= created_sequence
            .is_some_and(|created| window.is_some_and(|(command, _)| created < command))
            && public.is_some_and(|value| {
                value.topic == item.topic
                    && value.config_name == config.name
                    && value.value.as_deref() == Some(config.value.as_str())
                    && public_after_command(window, value.history_sequence)
            })
            && independent.is_some_and(|value| {
                value.topic == item.topic
                    && value.config_name == config.name
                    && value.value == config.value
                    && public.is_some_and(|public| {
                        immediate_after_public(
                            window,
                            public.history_sequence,
                            value.history_sequence,
                        )
                    })
            });
        prior_step = step_index;
    }
    exact
}

fn later_description<'a>(
    scenario: &'a Scenario,
    prior_step: usize,
    item: &CreateTopicBatchActionItem,
    config: &TopicCreationConfig,
) -> Option<(usize, &'a testlab_schema::ScenarioStep)> {
    scenario
        .steps
        .iter()
        .enumerate()
        .skip(prior_step.saturating_add(1))
        .find(|(_, step)| {
            matches!(
                &step.action,
                ScenarioAction::DescribeTopicConfig(action)
                    if action.topic == item.topic
                        && action.config_name == config.name
                        && action.expected_value == config.value
            )
        })
}
