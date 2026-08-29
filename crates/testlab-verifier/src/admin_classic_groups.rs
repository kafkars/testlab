//! Classic-group descriptions require exact broker facts and proven live classic epochs.

use std::collections::BTreeMap;

use testlab_schema::{ConsumerId, GroupProtocol, OperationId, Scenario, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_group_batch::IndexedClassicGroupsDescription;
use crate::support::violation;

#[derive(Clone)]
struct LiveClassic {
    group_id: String,
    receive_id: Option<OperationId>,
}

pub(crate) fn verify_classic_groups(
    scenario: &Scenario,
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let ScenarioAction::DescribeClassicGroups(expected) = action else {
        return;
    };
    let window = index.admin_command_window(action);
    let public = index
        .admin_group_batches
        .classic_groups_described
        .get(&expected.operation_id);
    let public_value = public
        .filter(|values| values.len() == 1)
        .and_then(|values| values.first());
    let public_matches = public_value.is_some_and(|value| {
        value.outcomes.len() == expected.groups.len()
            && public_after_command(window, value.history_sequence)
            && value
                .outcomes
                .iter()
                .zip(&expected.groups)
                .all(|(actual, expected)| {
                    actual.group_id == expected.group_id
                        && actual.member_count == Some(expected.expected_member_count)
                        && actual.error_code.is_none()
                })
    });
    let independent = index.consumer_groups_observed.get(&expected.operation_id);
    let independent_matches = public_value.is_some_and(|public| {
        independent.is_some_and(|values| {
            values.len() == expected.groups.len()
                && contiguous(values.iter().map(|value| value.observation))
                && values
                    .iter()
                    .zip(&expected.groups)
                    .all(|(actual, expected)| {
                        actual.group_id == expected.group_id
                            && actual.exists
                            && actual.member_count == Some(expected.expected_member_count)
                            && immediate_after_public(
                                window,
                                public.history_sequence,
                                actual.history_sequence,
                            )
                    })
        })
    });
    let epochs_match = window.is_some_and(|(command_sequence, _)| {
        exact_live_classic_epochs(scenario, expected, index, command_sequence)
    });
    if public_matches && independent_matches && epochs_match {
        return;
    }
    violations.push(classic_violation(expected, public, independent, index));
}

fn exact_live_classic_epochs(
    scenario: &Scenario,
    expected: &testlab_schema::DescribeClassicGroupsAction,
    index: &HistoryIndex,
    command_sequence: u64,
) -> bool {
    let live = live_classic_consumers(scenario, &expected.operation_id);
    expected.groups.iter().all(|group| {
        let matching = live
            .values()
            .filter(|consumer| consumer.group_id == group.group_id)
            .collect::<Vec<_>>();
        usize::try_from(group.expected_member_count) == Ok(matching.len())
            && matching.iter().all(|consumer| {
                consumer.receive_id.as_ref().is_some_and(|receive_id| {
                    index.receives.get(receive_id).is_some_and(|receives| {
                        receives.len() == 1
                            && receives.first().is_some_and(|receive| {
                                receive.history_sequence < command_sequence
                                    && receive.committed == Some(true)
                                    && receive.group_epoch.is_some_and(|epoch| {
                                        epoch.protocol() == GroupProtocol::Classic
                                            && epoch.is_positive()
                                    })
                            })
                    })
                })
            })
    })
}

fn live_classic_consumers(
    scenario: &Scenario,
    description_id: &OperationId,
) -> BTreeMap<ConsumerId, LiveClassic> {
    let mut live = BTreeMap::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeClassicGroups(value)
                if &value.operation_id == description_id =>
            {
                break;
            }
            ScenarioAction::CreateGroupConsumer {
                consumer_id,
                group_id,
                protocol: GroupProtocol::Classic,
                ..
            } => {
                live.insert(
                    consumer_id.clone(),
                    LiveClassic {
                        group_id: group_id.clone(),
                        receive_id: None,
                    },
                );
            }
            ScenarioAction::GroupReceive {
                consumer_id,
                receive_id,
                ..
            } => {
                if let Some(consumer) = live.get_mut(consumer_id) {
                    consumer.receive_id = Some(receive_id.clone());
                }
            }
            ScenarioAction::CloseGroupConsumer { consumer_id } => {
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

fn classic_violation(
    expected: &testlab_schema::DescribeClassicGroupsAction,
    public: Option<&Vec<IndexedClassicGroupsDescription>>,
    independent: Option<&Vec<crate::index::IndexedConsumerGroupObservation>>,
    index: &HistoryIndex,
) -> Violation {
    violation(
        "ADMIN-027",
        format!(
            "admin operation {} expected exact ordered classic-group descriptions, immediate broker membership facts, and positive live classic receive epochs",
            expected.operation_id
        ),
        Some(expected.operation_id.clone()),
        public
            .into_iter()
            .flatten()
            .map(|value| format!("history:{}", value.history_sequence))
            .chain(
                independent
                    .into_iter()
                    .flatten()
                    .map(|value| format!("broker-state-observation:{}", value.observation)),
            )
            .chain(
                index
                    .receives
                    .values()
                    .flatten()
                    .map(|value| format!("history:{}", value.history_sequence)),
            )
            .collect(),
    )
}
