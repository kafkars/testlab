//! Group-consumer control verification binds exact commands to public completions.

use testlab_schema::{GroupConsumerControlCommand, Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::ControlGroupConsumer(action) = &step.action else {
            continue;
        };
        let expected_command = GroupConsumerControlCommand {
            operation_id: action.operation_id.clone(),
            consumer_id: action.consumer_id.clone(),
            control: action.control.clone(),
            timeout_ms: action.timeout_ms,
        };
        let command = index.group_controls_issued.get(&action.operation_id);
        let completions = index.group_controls.get(&action.operation_id);
        let exact = command == Some(&expected_command)
            && completions.map_or(0, Vec::len) == 1
            && completions
                .and_then(|values| values.first())
                .is_some_and(|value| {
                    value.completion.operation_id == action.operation_id
                        && value.completion.consumer_id == action.consumer_id
                        && value.completion.control == action.control.kind()
                });
        if !exact {
            violations.push(violation(
                "CONS-014",
                format!(
                    "group-consumer control {} expected one exact issued command and matching {:?} completion",
                    action.operation_id,
                    action.control.kind()
                ),
                Some(action.operation_id.clone()),
                references(completions),
            ));
        }
    }
}

fn references(completions: Option<&Vec<crate::index::IndexedGroupConsumerControl>>) -> Vec<String> {
    completions
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .collect()
}
