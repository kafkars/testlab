//! Redistribution verification requires public partition owners to change with the member set.

use std::collections::BTreeMap;

use testlab_schema::{ConsumerId, Scenario, ScenarioAction, TopicPartitionIdentity, Violation};

use crate::index::{HistoryIndex, IndexedGroupAssignments};
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    let observations = scenario
        .steps
        .iter()
        .filter_map(|step| {
            let ScenarioAction::ObserveGroupAssignments(action) = &step.action else {
                return None;
            };
            let indexed = index
                .group_assignments
                .get(&action.operation_id)?
                .as_slice()
                .first()?;
            Some((action, indexed))
        })
        .collect::<Vec<_>>();
    for pair in observations.windows(2) {
        let [(previous_action, previous), (current_action, current)] = pair else {
            continue;
        };
        if previous_action.consumer_ids == current_action.consumer_ids
            || previous_action.partitions != current_action.partitions
        {
            continue;
        }
        let changed = ownership(previous) != ownership(current)
            && !current.observation.transitions.is_empty();
        if !changed {
            violations.push(violation(
                "CONS-010",
                format!(
                    "assignment observation {} did not expose redistribution after the member set changed",
                    current_action.operation_id
                ),
                Some(current_action.operation_id.clone()),
                vec![format!("history:{}", current.history_sequence)],
            ));
        }
    }
}

fn ownership(
    observation: &IndexedGroupAssignments,
) -> BTreeMap<TopicPartitionIdentity, ConsumerId> {
    observation
        .observation
        .assignments
        .iter()
        .flat_map(|assignment| {
            assignment
                .partitions
                .iter()
                .cloned()
                .map(|partition| (partition, assignment.consumer_id.clone()))
        })
        .collect()
}
