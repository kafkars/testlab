//! Plural Share descriptions join ordered public detail to live batches and CLI state.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{
    ConsumerId, DescribeShareGroupAction, DescribeShareGroupsAction, OperationId, Scenario,
    ScenarioAction, ShareGroupDescriptionExpectation, Violation,
};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_share_group::Indexed;
use crate::support::violation;

#[derive(Clone)]
struct LiveShare {
    group_id: String,
    topic: String,
    retained: BTreeSet<OperationId>,
}

pub(crate) fn verify(
    scenario: &Scenario,
    scenario_action: &ScenarioAction,
    action: &DescribeShareGroupsAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let window = index.admin_command_window(scenario_action);
    let public = one(index
        .admin_share_groups
        .groups_described
        .get(&action.operation_id));
    let independent = index.admin_share_groups.observed.get(&action.operation_id);
    let public_matches = public.is_some_and(|value| {
        public_after_command(window, value.history_sequence)
            && value.value.operation_id == action.operation_id
            && value.value.outcomes.len() == action.groups.len()
            && value
                .value
                .outcomes
                .iter()
                .zip(&action.groups)
                .all(|(actual, expected)| outcome_matches(actual, expected, action))
    });
    let independent_matches = public.is_some_and(|public| {
        independent.is_some_and(|values| {
            values.len() == action.groups.len()
                && contiguous(values.iter().map(|value| value.value.observation))
                && contiguous(values.iter().map(|value| value.history_sequence))
                && values.iter().zip(&action.groups).all(|(actual, expected)| {
                    actual.value.operation_id == action.operation_id
                        && actual.value.group_id == expected.group_id
                        && actual.value.exists
                        && actual.value.state.as_deref() == Some(expected.expected_state.as_str())
                        && actual.value.member_count == Some(expected.expected_member_count)
                        && immediate_after_public(
                            window,
                            public.history_sequence,
                            actual.history_sequence,
                        )
                })
        })
    });
    let live_matches = window.is_some_and(|(command_sequence, _)| {
        exact_live_share_batches(scenario, action, index, command_sequence)
    });
    if public_matches && independent_matches && live_matches {
        return;
    }
    violations.push(violation(
        "ADMIN-042",
        format!(
            "admin operation {} expected caller-ordered detailed descriptions, live retained Share batches, and immediate independent state for every group",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        public
            .map(|value| format!("history:{}", value.history_sequence))
            .into_iter()
            .chain(independent.into_iter().flatten().map(|value| {
                format!("broker-state-observation:{}", value.value.observation)
            }))
            .collect(),
    ));
}

fn outcome_matches(
    actual: &testlab_schema::AdminShareGroupDescriptionOutcome,
    expected: &ShareGroupDescriptionExpectation,
    action: &DescribeShareGroupsAction,
) -> bool {
    actual.group_id == expected.group_id
        && actual.error_code.is_none()
        && actual.description.as_ref().is_some_and(|description| {
            crate::admin_share_group::description_matches(
                description,
                &DescribeShareGroupAction {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    group_id: expected.group_id.clone(),
                    expected_state: expected.expected_state.clone(),
                    expected_member_count: expected.expected_member_count,
                    expected_topic: expected.expected_topic.clone(),
                    expected_partition: expected.expected_partition,
                    timeout_ms: action.timeout_ms,
                },
            )
        })
}

fn exact_live_share_batches(
    scenario: &Scenario,
    expected: &DescribeShareGroupsAction,
    index: &HistoryIndex,
    command_sequence: u64,
) -> bool {
    let live = live_share_consumers(scenario, &expected.operation_id);
    expected.groups.iter().all(|group| {
        let matching = live
            .iter()
            .filter(|(_, consumer)| {
                consumer.group_id == group.group_id && consumer.topic == group.expected_topic
            })
            .collect::<Vec<_>>();
        usize::try_from(group.expected_member_count) == Ok(matching.len())
            && matching.iter().all(|(consumer_id, consumer)| {
                consumer.retained.iter().any(|receive_id| {
                    index
                        .share_receives
                        .get(receive_id)
                        .is_some_and(|receives| {
                            receives.len() == 1
                                && receives.first().is_some_and(|receive| {
                                    receive.history_sequence < command_sequence
                                        && &receive.consumer_id == *consumer_id
                                        && receive.acquisition_count > 0
                                        && receive.member_epoch.is_some_and(|epoch| epoch > 0)
                                        && receive.assignment_epoch.is_some_and(|epoch| epoch > 0)
                                        && receive.records.iter().any(|record| {
                                            record.record.topic == group.expected_topic
                                                && record.record.partition
                                                    == group.expected_partition
                                        })
                                })
                        })
                })
            })
    })
}

fn live_share_consumers(
    scenario: &Scenario,
    description_id: &OperationId,
) -> BTreeMap<ConsumerId, LiveShare> {
    let mut live = BTreeMap::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeShareGroups(value) if &value.operation_id == description_id => {
                break;
            }
            ScenarioAction::CreateShareConsumer {
                consumer_id,
                group_id,
                topic,
                ..
            } => {
                live.insert(
                    consumer_id.clone(),
                    LiveShare {
                        group_id: group_id.clone(),
                        topic: topic.clone(),
                        retained: BTreeSet::new(),
                    },
                );
            }
            ScenarioAction::ShareReceive {
                consumer_id,
                receive_id,
                ..
            } => {
                if let Some(consumer) = live.get_mut(consumer_id) {
                    consumer.retained.insert(receive_id.clone());
                }
            }
            ScenarioAction::ShareAcknowledge {
                consumer_id,
                receive_id,
                ..
            }
            | ScenarioAction::DropShareBatch {
                consumer_id,
                receive_id,
            } => {
                if let Some(consumer) = live.get_mut(consumer_id) {
                    consumer.retained.remove(receive_id);
                }
            }
            ScenarioAction::CloseShareConsumer { consumer_id, .. } => {
                live.remove(consumer_id);
            }
            _ => {}
        }
    }
    live
}

fn contiguous(values: impl Iterator<Item = u64>) -> bool {
    let mut previous: Option<u64> = None;
    for value in values {
        if previous.is_some_and(|previous| value != previous.saturating_add(1)) {
            return false;
        }
        previous = Some(value);
    }
    true
}

fn one<T>(values: Option<&Vec<Indexed<T>>>) -> Option<&Indexed<T>> {
    let [value] = values?.as_slice() else {
        return None;
    };
    Some(value)
}
