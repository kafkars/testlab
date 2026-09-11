//! Consumer-group batch deletion joins ordered public outcomes to before-and-after broker facts.

use testlab_schema::{
    DeleteConsumerGroupsAction, DescribeClassicGroupsAction, Scenario, ScenarioAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::admin_group_batch::IndexedConsumerGroupsDeletion;
use crate::index::{HistoryIndex, IndexedConsumerGroupObservation};
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    action: &DeleteConsumerGroupsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_group_batches
        .groups_deleted
        .get(&action.operation_id));
    let description = prior_description(scenario, action);
    let baseline =
        description.and_then(|value| index.consumer_groups_observed.get(&value.operation_id));
    let observed = index.consumer_groups_observed.get(&action.operation_id);
    let public_matches = public.is_some_and(|value| {
        public_after_command(window, value.history_sequence)
            && value.value.operation_id == action.operation_id
            && value.value.outcomes.len() == action.group_ids.len()
            && value
                .value
                .outcomes
                .iter()
                .zip(&action.group_ids)
                .all(|(actual, expected)| {
                    actual.group_id == *expected && actual.error_code.is_none()
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
        "ADMIN-046",
        format!(
            "admin operation {} expected prior caller-ordered empty-group state, exact successful deletion outcomes, and immediate independent absence for every requested consumer group",
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
    deletion: &DeleteConsumerGroupsAction,
) -> Option<&'a DescribeClassicGroupsAction> {
    let mut matching = None;
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DeleteConsumerGroups(action)
                if action.operation_id == deletion.operation_id =>
            {
                break;
            }
            ScenarioAction::DescribeClassicGroups(action)
                if action.groups.len() == deletion.group_ids.len()
                    && action.groups.iter().zip(&deletion.group_ids).all(
                        |(group, expected)| {
                            group.group_id == *expected && group.expected_member_count == 0
                        },
                    ) =>
            {
                matching = Some(action);
            }
            _ => {}
        }
    }
    matching
}

fn baseline_is_exact(
    values: &[IndexedConsumerGroupObservation],
    description: &DescribeClassicGroupsAction,
    window: Option<AdminCommandWindow>,
) -> bool {
    let command = window.map(|(command, _)| command);
    values.len() == description.groups.len()
        && contiguous(values)
        && values
            .iter()
            .zip(&description.groups)
            .all(|(actual, expected)| {
                command.is_some_and(|command| actual.history_sequence < command)
                    && actual.group_id == expected.group_id
                    && actual.exists
                    && actual.member_count == Some(0)
            })
}

fn post_delete_is_exact(
    values: &[IndexedConsumerGroupObservation],
    action: &DeleteConsumerGroupsAction,
    window: Option<AdminCommandWindow>,
    public: &IndexedConsumerGroupsDeletion,
) -> bool {
    values.len() == action.group_ids.len()
        && contiguous(values)
        && values
            .iter()
            .zip(&action.group_ids)
            .all(|(actual, expected)| {
                actual.group_id == *expected
                    && !actual.exists
                    && actual.member_count.is_none()
                    && immediate_after_public(
                        window,
                        public.history_sequence,
                        actual.history_sequence,
                    )
            })
}

fn contiguous(values: &[IndexedConsumerGroupObservation]) -> bool {
    values.windows(2).all(|pair| {
        pair[1].observation == pair[0].observation.saturating_add(1)
            && pair[1].history_sequence == pair[0].history_sequence.saturating_add(1)
    })
}

fn observation_evidence(value: &IndexedConsumerGroupObservation) -> String {
    format!("broker-state-observation:{}", value.observation)
}

fn one(
    values: Option<&Vec<IndexedConsumerGroupsDeletion>>,
) -> Option<&IndexedConsumerGroupsDeletion> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
