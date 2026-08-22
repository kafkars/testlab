//! Admin verification requires one exact public completion per declared operation.

use testlab_schema::{Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::{references, violation};

pub(crate) fn verify_admin(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        let ScenarioAction::CreateTopic {
            operation_id,
            topic,
            ..
        } = &step.action
        else {
            continue;
        };
        if !index.action_issued(&step.action) {
            continue;
        }
        let completions = index.topics_created.get(operation_id);
        let exact = completions.is_some_and(|values| {
            values.len() == 1 && values.first().is_some_and(|value| value.topic == *topic)
        });
        if !exact {
            violations.push(violation(
                "ADMIN-001",
                format!(
                    "admin operation {operation_id} expected one creation for topic {topic}, observed {} completion(s)",
                    completions.map_or(0, Vec::len)
                ),
                Some(operation_id.clone()),
                references(completions.map(|values| {
                    values
                        .iter()
                        .map(|value| value.history_sequence)
                        .collect::<Vec<_>>()
                }).as_deref()),
            ));
        }
    }
}
