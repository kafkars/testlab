//! Share-group offset alteration joins public success to baseline and CLI post-state.

use testlab_schema::{
    AlterShareGroupOffsetsAction, BrokerShareGroupOffset, Scenario, ScenarioAction, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_share_group::Indexed;
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    action: &AlterShareGroupOffsetsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_share_groups
        .offsets_altered
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
            && value.value.partition == action.partition
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
    let baseline_matches = has_prior_baseline(scenario, action, index);
    let empty_group_matches = modeled_group_closed(scenario, action, index, window);
    if public_matches && independent_matches && baseline_matches && empty_group_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-039",
        format!(
            "admin operation {} expected one exact Share-group offset alteration after a distinct listed baseline and modeled empty-group close, plus immediate independent post-state",
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
    expected: &AlterShareGroupOffsetsAction,
) -> bool {
    actual.operation_id == expected.operation_id
        && actual.group_id == expected.group_id
        && actual.topic == expected.topic
        && actual.partition == expected.partition
        && actual.start_offset == Some(expected.start_offset)
        && actual.lag == Some(expected.expected_lag)
}

fn has_prior_baseline(
    scenario: &Scenario,
    mutation: &AlterShareGroupOffsetsAction,
    index: &HistoryIndex,
) -> bool {
    let Some(mutation_index) = scenario.steps.iter().position(|step| {
        matches!(
            &step.action,
            ScenarioAction::AlterShareGroupOffsets(action)
                if action.operation_id == mutation.operation_id
        )
    }) else {
        return false;
    };
    scenario.steps[..mutation_index].iter().any(|step| {
        let ScenarioAction::ListShareGroupOffsets(baseline) = &step.action else {
            return false;
        };
        baseline.group_id == mutation.group_id
            && baseline.topic == mutation.topic
            && baseline.partition == mutation.partition
            && baseline.expected_start_offset != mutation.start_offset
            && crate::admin_share_group::exact_offset_evidence(&step.action, baseline, index)
    })
}

fn modeled_group_closed(
    scenario: &Scenario,
    mutation: &AlterShareGroupOffsetsAction,
    index: &HistoryIndex,
    window: Option<crate::admin::AdminCommandWindow>,
) -> bool {
    let Some((command_sequence, _)) = window else {
        return false;
    };
    let Some(mutation_index) = scenario.steps.iter().position(|step| {
        matches!(
            &step.action,
            ScenarioAction::AlterShareGroupOffsets(action)
                if action.operation_id == mutation.operation_id
        )
    }) else {
        return false;
    };
    let prior = &scenario.steps[..mutation_index];
    let members = prior.iter().filter_map(|step| match &step.action {
        ScenarioAction::CreateShareConsumer {
            consumer_id,
            group_id,
            ..
        } if group_id == &mutation.group_id => Some(consumer_id),
        _ => None,
    });
    let mut count = 0;
    for member in members {
        count += 1;
        if !prior.iter().any(|step| {
            matches!(
                &step.action,
                ScenarioAction::CloseShareConsumer { consumer_id, .. }
                    if consumer_id == member
            )
        }) {
            return false;
        }
        let Some([close]) = index.share_consumers_closed.get(member).map(Vec::as_slice) else {
            return false;
        };
        if !close.success || close.history_sequence >= command_sequence {
            return false;
        }
    }
    count > 0
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
