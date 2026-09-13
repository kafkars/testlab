//! `DescribeConfigs` option evidence retains requested public entry metadata.

use testlab_schema::{DescribeTopicConfigsAction, ScenarioAction, Violation};

use crate::admin::public_after_command;
use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(
    scenario_action: &ScenarioAction,
    action: &DescribeTopicConfigsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    if !action.include_synonyms && !action.include_documentation {
        return;
    }
    let window = index.admin_command_window(scenario_action);
    let public = index
        .topic_configs_batch_described
        .get(&action.operation_id);
    let (exact_command, _) = index.admin_command_state(scenario_action);
    let exact = public.and_then(|values| match values.as_slice() {
        [value] => Some(value),
        _ => None,
    });
    let matches =
        exact_command
            && exact.is_some_and(|indexed| {
                public_after_command(window, indexed.history_sequence)
                    && indexed.value.operation_id == action.operation_id
                    && indexed.value.outcomes.len() == action.topics.len()
                    && indexed.value.outcomes.iter().zip(&action.topics).all(
                        |(outcome, selected)| {
                            outcome.topic == selected.topic
                                && outcome.config_name == selected.config_name
                                && outcome.error_code.is_none()
                                && outcome.metadata.as_ref().is_some_and(|metadata| {
                                    metadata_matches(outcome, metadata, action)
                                })
                        },
                    )
            });
    if matches {
        return;
    }
    let references = window
        .map(|(sequence, _)| format!("history:{sequence}"))
        .into_iter()
        .chain(
            public
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence)),
        )
        .collect();
    violations.push(violation(
        "ADMIN-081",
        format!(
            "admin operation {} expected exact DescribeConfigs option flags and complete selected-entry metadata",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        references,
    ));
}

fn metadata_matches(
    outcome: &testlab_schema::AdminTopicConfigDescriptionOutcome,
    metadata: &testlab_schema::AdminConfigEntryMetadata,
    action: &DescribeTopicConfigsAction,
) -> bool {
    let synonyms_match = !action.include_synonyms
        || (!metadata.synonyms.is_empty()
            && metadata
                .synonyms
                .iter()
                .all(|synonym| !synonym.name.trim().is_empty())
            && metadata
                .synonyms
                .iter()
                .any(|synonym| synonym.value == outcome.value));
    let documentation_matches = !action.include_documentation
        || (metadata.config_type.is_some()
            && metadata
                .documentation
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty()));
    synonyms_match && documentation_matches
}
