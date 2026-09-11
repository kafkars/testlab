//! Share-group deletion joins ordered public outcomes to modeled closure and CLI absence.

use testlab_schema::{DeleteShareGroupsAction, Scenario, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_share_group::Indexed;
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    action: &DeleteShareGroupsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_share_groups
        .groups_deleted
        .get(&action.operation_id));
    let independent = index.admin_share_groups.observed.get(&action.operation_id);
    let public_matches = public.is_some_and(|value| {
        public_after_command(window, value.history_sequence)
            && value.value.operation_id == action.operation_id
            && value.value.outcomes.len() == action.group_ids.len()
            && value
                .value
                .outcomes
                .iter()
                .zip(&action.group_ids)
                .all(|(outcome, group_id)| {
                    outcome.group_id.as_str() == group_id && outcome.error_code.is_none()
                })
    });
    let independent_matches = public.is_some_and(|public| {
        independent.is_some_and(|values| {
            values.len() == action.group_ids.len()
                && values
                    .iter()
                    .zip(&action.group_ids)
                    .all(|(value, group_id)| {
                        value.value.operation_id == action.operation_id
                            && value.value.group_id.as_str() == group_id
                            && !value.value.exists
                            && value.value.state.is_none()
                            && value.value.member_count.is_none()
                            && immediate_after_public(
                                window,
                                public.history_sequence,
                                value.history_sequence,
                            )
                    })
                && values.windows(2).all(|pair| {
                    pair[0].value.observation.checked_add(1) == Some(pair[1].value.observation)
                        && pair[0].history_sequence < pair[1].history_sequence
                })
        })
    });
    let closed_groups = action.group_ids.iter().all(|group_id| {
        crate::admin_share_group_offset_mutation::modeled_group_closed(
            scenario,
            scenario_action,
            group_id,
            index,
            window,
        )
    });
    if public_matches && independent_matches && closed_groups {
        return;
    }
    violations.push(violation(
        "ADMIN-041",
        format!(
            "admin operation {} expected caller-ordered successful deletion and immediate independent absence for every closed modeled Share group",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(
                independent
                    .into_iter()
                    .flatten()
                    .map(|value| {
                        format!("broker-state-observation:{}", value.value.observation)
                    }),
            )
            .collect(),
    ));
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
