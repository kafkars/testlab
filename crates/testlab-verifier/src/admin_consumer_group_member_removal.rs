//! Static-member removal joins retained offline members to ordered public and broker facts.

use testlab_schema::{RemoveConsumerGroupMembersAction, Scenario, ScenarioAction, Violation};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::admin_group_batch::IndexedConsumerGroupMembersRemoval;
use crate::index::{
    HistoryIndex, IndexedConsumerGroupDescription, IndexedConsumerGroupObservation,
};
use crate::support::violation;

pub(crate) fn verify(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    action: &RemoveConsumerGroupMembersAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_group_batches
        .members_removed
        .get(&action.operation_id));
    let baseline = baseline_action(scenario, action);
    let baseline_public =
        baseline.and_then(|value| one(index.consumer_groups_described.get(&value.operation_id)));
    let baseline_observed =
        baseline.and_then(|value| one(index.consumer_groups_observed.get(&value.operation_id)));
    let baseline_window = baseline.and_then(|value| {
        index.admin_command_window(&ScenarioAction::DescribeConsumerGroup(value.clone()))
    });
    let observed = one(index.consumer_groups_observed.get(&action.operation_id));
    let baseline_matches = baseline.is_some_and(|expected| {
        expected.group_id == action.group_id
            && retained_members_precede_baseline(scenario, action, index, baseline_window)
            && exact_baseline(
                expected,
                baseline_public,
                baseline_observed,
                baseline_window,
                window,
                action.group_instance_ids.len(),
            )
    });
    let public_matches = public.is_some_and(|value| exact_public(action, value, window));
    let observed_matches = public.is_some_and(|public| {
        observed.is_some_and(|observed| exact_post_state(action, observed, public, window))
    });
    if baseline_matches && public_matches && observed_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-068",
        format!(
            "admin operation {} expected abandoned static owners with stopped clients, a named retained-member baseline, exact caller-ordered successful removals, and immediate independent zero-member state",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        baseline_public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(
                baseline_observed
                    .map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .chain(public.map(|value| format!("history:{}", value.history_sequence)))
            .chain(
                observed.map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .collect(),
    ));
}

fn retained_members_precede_baseline(
    scenario: &Scenario,
    action: &RemoveConsumerGroupMembersAction,
    index: &HistoryIndex,
    baseline_window: Option<AdminCommandWindow>,
) -> bool {
    let Some((baseline_command, _)) = baseline_window else {
        return false;
    };
    action.group_instance_ids.iter().all(|identity| {
        let Some((consumer_id, client_id)) = static_member_owner(scenario, action, identity) else {
            return false;
        };
        let abandoned = one(index.group_consumers_abandoned.get(consumer_id));
        let shutdown = one(index.clients_shutdown.get(client_id));
        abandoned.is_some_and(|abandoned| {
            shutdown.is_some_and(|shutdown| *abandoned < *shutdown && *shutdown < baseline_command)
        })
    })
}

fn static_member_owner<'a>(
    scenario: &'a Scenario,
    action: &RemoveConsumerGroupMembersAction,
    identity: &str,
) -> Option<(&'a testlab_schema::ConsumerId, &'a testlab_schema::ClientId)> {
    scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::CreateGroupConsumer {
            client_id,
            consumer_id,
            group_id,
            protocol: testlab_schema::GroupProtocol::Classic,
            configuration: Some(configuration),
            ..
        } if group_id == &action.group_id
            && configuration.group_instance_id.as_deref() == Some(identity)
            && configuration
                .classic_session_timeout_ms
                .is_some_and(|timeout| timeout >= scenario.timeout_ms) =>
        {
            Some((consumer_id, client_id))
        }
        _ => None,
    })
}

fn baseline_action<'a>(
    scenario: &'a Scenario,
    action: &RemoveConsumerGroupMembersAction,
) -> Option<&'a testlab_schema::DescribeConsumerGroupAction> {
    scenario.steps.iter().find_map(|step| match &step.action {
        ScenarioAction::DescribeConsumerGroup(value)
            if value.operation_id == action.baseline_operation_id =>
        {
            Some(value)
        }
        _ => None,
    })
}

fn exact_baseline(
    expected: &testlab_schema::DescribeConsumerGroupAction,
    public: Option<&IndexedConsumerGroupDescription>,
    observed: Option<&IndexedConsumerGroupObservation>,
    baseline_window: Option<AdminCommandWindow>,
    removal_window: Option<AdminCommandWindow>,
    selected_count: usize,
) -> bool {
    let removal_command = removal_window.map(|(command, _)| command);
    let Ok(selected_count) = u32::try_from(selected_count) else {
        return false;
    };
    expected.expected_member_count == selected_count
        && public.is_some_and(|public| {
            public.group_id == expected.group_id
                && public.member_count == selected_count
                && public_after_command(baseline_window, public.history_sequence)
                && observed.is_some_and(|observed| {
                    observed.group_id == expected.group_id
                        && observed.exists
                        && observed.member_count == Some(selected_count)
                        && immediate_after_public(
                            baseline_window,
                            public.history_sequence,
                            observed.history_sequence,
                        )
                        && removal_command
                            .is_some_and(|command| observed.history_sequence < command)
                })
        })
}

fn exact_public(
    action: &RemoveConsumerGroupMembersAction,
    public: &IndexedConsumerGroupMembersRemoval,
    window: Option<AdminCommandWindow>,
) -> bool {
    public_after_command(window, public.history_sequence)
        && public.value.operation_id == action.operation_id
        && public.value.group_id == action.group_id
        && public.value.throttle_time_ms <= action.timeout_ms
        && public.value.outcomes.len() == action.group_instance_ids.len()
        && public
            .value
            .outcomes
            .iter()
            .zip(&action.group_instance_ids)
            .all(|(actual, expected)| {
                actual.group_instance_id == *expected && actual.error_code.is_none()
            })
}

fn exact_post_state(
    action: &RemoveConsumerGroupMembersAction,
    observed: &IndexedConsumerGroupObservation,
    public: &IndexedConsumerGroupMembersRemoval,
    window: Option<AdminCommandWindow>,
) -> bool {
    observed.group_id == action.group_id
        && observed.exists
        && observed.member_count == Some(0)
        && immediate_after_public(window, public.history_sequence, observed.history_sequence)
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    values.filter(|values| values.len() == 1)?.first()
}
