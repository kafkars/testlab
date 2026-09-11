//! Share-group offset deletion joins public topic success to baseline and CLI absence.

use testlab_schema::{
    BrokerShareGroupOffset, DeleteShareGroupOffsetsAction, Scenario, ScenarioAction, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_share_group::Indexed;
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    action: &DeleteShareGroupOffsetsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_share_groups
        .offsets_deleted
        .get(&action.operation_id));
    let independent = one(index
        .admin_share_groups
        .offsets_observed
        .get(&action.operation_id));
    let public_matches = public.is_some_and(|value| {
        public_after_command(window, value.history_sequence)
            && value.value.operation_id == action.operation_id
            && value.value.group_id == action.group_id
            && value.value.topic == action.topic
            && value.value.topic_id != [0; 16]
            && value.value.error_code.is_none()
    });
    let independent_matches = match (public, independent) {
        (Some(public), Some(independent)) => {
            observation_matches(&independent.value, action)
                && immediate_after_public(
                    window,
                    public.history_sequence,
                    independent.history_sequence,
                )
        }
        _ => false,
    };
    let baseline_matches = super::admin_share_group_offset_mutation::has_prior_baseline(
        scenario,
        scenario_action,
        &action.group_id,
        &action.topic,
        action.partition,
        None,
        index,
    );
    let empty_group_matches = super::admin_share_group_offset_mutation::modeled_group_closed(
        scenario,
        scenario_action,
        &action.group_id,
        index,
        window,
    );
    if public_matches && independent_matches && baseline_matches && empty_group_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-040",
        format!(
            "admin operation {} expected one exact Share-group topic-offset deletion after a listed baseline and modeled empty-group close, plus immediate independent absence",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(
                independent
                    .map(|value| format!("broker-state-observation:{}", value.value.observation)),
            )
            .collect(),
    ));
}

fn observation_matches(
    actual: &BrokerShareGroupOffset,
    expected: &DeleteShareGroupOffsetsAction,
) -> bool {
    actual.operation_id == expected.operation_id
        && actual.group_id == expected.group_id
        && actual.topic == expected.topic
        && actual.partition == expected.partition
        && actual.start_offset.is_none()
        && actual.lag.is_none()
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
