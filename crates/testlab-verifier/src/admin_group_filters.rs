//! Group-listing filters retain exact public selection and independent identity evidence.

use testlab_schema::{ListConsumerGroupsAction, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::admin_group_evidence::group_list_evidence;
use crate::index::HistoryIndex;
use crate::support::violation;

pub(super) fn verify(
    scenario_action: &ScenarioAction,
    action: &ListConsumerGroupsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    if !selected(action) {
        return;
    }
    let window = index.admin_command_window(scenario_action);
    let public = index.consumer_groups_listed.get(&action.operation_id);
    let independent = index.consumer_groups_observed.get(&action.operation_id);
    let exact = public.and_then(|values| match values.as_slice() {
        [value] => Some(value),
        _ => None,
    });
    let matches = index.admin_command_state(scenario_action) == (true, 1)
        && exact.is_some_and(|public| {
            public.value.operation_id == action.operation_id
                && public.value.broker_errors.is_empty()
                && action
                    .required_group_ids
                    .iter()
                    .all(|group| public.value.group_ids.binary_search(group).is_ok())
                && public_after_command(window, public.history_sequence)
                && independent.is_some_and(|values| {
                    values.len() == action.required_group_ids.len()
                        && action.required_group_ids.iter().all(|group| {
                            values
                                .iter()
                                .any(|value| value.group_id == *group && value.exists)
                        })
                        && values.iter().all(|value| {
                            immediate_after_public(
                                window,
                                public.history_sequence,
                                value.history_sequence,
                            )
                        })
                })
        });
    if matches {
        return;
    }
    violations.push(violation(
        "ADMIN-082",
        format!(
            "admin operation {} expected exact group-listing filters with public and independent required identities",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        group_list_evidence(public, independent),
    ));
}

pub(crate) fn selected(action: &ListConsumerGroupsAction) -> bool {
    !action.state_filters.is_empty()
        || !action.group_type_filters.is_empty()
        || !action.protocol_type_filters.is_empty()
}
