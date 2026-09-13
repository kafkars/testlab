//! Configured creation requires ordered public and independent value proof after topology proof.

use testlab_schema::{CreateTopicAction, Scenario, ScenarioAction, TopicCreationConfig, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(
    scenario: &Scenario,
    creation: &CreateTopicAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let creation_step = scenario.steps.iter().position(|step| {
        matches!(
            &step.action,
            ScenarioAction::CreateTopic(action) if action.operation_id == creation.operation_id
        )
    });
    let created = index
        .topics_observed
        .get(&creation.operation_id)
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let mut evidence = created
        .map(|value| format!("broker-state-observation:{}", value.observation))
        .into_iter()
        .collect::<Vec<_>>();
    let mut prior_step = creation_step.unwrap_or(scenario.steps.len());
    let mut exact = creation_step.is_some() && created.is_some();

    for config in &creation.configs {
        let matching = later_description(scenario, prior_step, creation, config);
        let Some((step_index, step)) = matching else {
            exact = false;
            break;
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
        exact &= created.is_some_and(|created| {
            window.is_some_and(|(command, _)| created.history_sequence < command)
        }) && public.is_some_and(|public| {
            public.topic == creation.topic
                && public.config_name == config.name
                && public.value.as_deref() == Some(config.value.as_str())
                && public_after_command(window, public.history_sequence)
        }) && independent.is_some_and(|independent| {
            independent.topic == creation.topic
                && independent.config_name == config.name
                && independent.value == config.value
                && public.is_some_and(|public| {
                    immediate_after_public(
                        window,
                        public.history_sequence,
                        independent.history_sequence,
                    )
                })
        });
        prior_step = step_index;
    }

    if !exact {
        violations.push(violation(
            "ADMIN-094",
            format!(
                "admin operation {} expected every caller-ordered creation config to have one later exact public description and immediate independent value",
                creation.operation_id
            ),
            Some(creation.operation_id.clone()),
            evidence,
        ));
    }
}

fn later_description<'a>(
    scenario: &'a Scenario,
    prior_step: usize,
    creation: &CreateTopicAction,
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
                    if action.topic == creation.topic
                        && action.config_name == config.name
                        && action.expected_value == config.value
            )
        })
}
