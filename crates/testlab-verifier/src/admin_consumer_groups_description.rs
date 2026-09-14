//! Mixed group descriptions join detailed public variants to live epochs and broker counts.

use std::collections::BTreeMap;

use testlab_schema::{ConsumerId, GroupProtocol, OperationId, Scenario, ScenarioAction, Violation};

use crate::admin::{immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::support::violation;

struct LiveConsumer {
    group_id: String,
    protocol: GroupProtocol,
    topics: Vec<String>,
    receive_id: Option<OperationId>,
}

pub(crate) fn verify(
    scenario: &Scenario,
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let ScenarioAction::DescribeConsumerGroups(expected) = action else {
        return;
    };
    let window = index.admin_command_window(action);
    let public = index
        .admin_group_batches
        .consumer_groups_described
        .get(&expected.operation_id);
    let public_value = one(public);
    let public_matches = public_value.is_some_and(|value| {
        value.outcomes.len() == expected.groups.len()
            && public_after_command(window, value.history_sequence)
            && value
                .outcomes
                .iter()
                .zip(&expected.groups)
                .all(|(actual, group)| {
                    outcome_matches(actual, group, expected.include_authorized_operations)
                })
    });
    let independent = index.consumer_groups_observed.get(&expected.operation_id);
    let independent_matches = public_value.is_some_and(|public| {
        independent.is_some_and(|values| {
            values.len() == expected.groups.len()
                && values
                    .windows(2)
                    .all(|pair| pair[1].observation == pair[0].observation.saturating_add(1))
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
        exact_live_epochs(scenario, expected, index, command_sequence)
    });
    if public_matches && independent_matches && epochs_match {
        return;
    }
    violations.push(violation(
        "ADMIN-069",
        format!(
            "admin operation {} expected caller-ordered mixed-protocol descriptions with requested authorization metadata, detailed public member facts, live matching receive epochs, and immediate broker counts",
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
            .collect(),
    ));
}

fn outcome_matches(
    actual: &testlab_schema::AdminConsumerGroupDescriptionOutcome,
    expected: &testlab_schema::ConsumerGroupDescriptionExpectation,
    include_authorized_operations: bool,
) -> bool {
    actual.group_id == expected.group_id
        && actual.error_code.is_none()
        && actual.description.as_ref().is_some_and(|description| {
            description.state == expected.expected_state
                && description.protocol == expected.protocol
                && description.member_count == expected.expected_member_count
                && description.authorized_operations.is_some() == include_authorized_operations
                && description.assignor_name == expected.expected_assignor_name
                && usize::try_from(expected.expected_member_count) == Ok(description.members.len())
                && strictly_ordered_members(&description.members)
                && protocol_details_match(description, expected)
        })
}

fn protocol_details_match(
    description: &testlab_schema::AdminConsumerGroupDescriptionValue,
    expected: &testlab_schema::ConsumerGroupDescriptionExpectation,
) -> bool {
    match expected.protocol {
        GroupProtocol::Classic => {
            description.protocol_type.as_deref() == Some("consumer")
                && description.group_epoch.is_none()
                && description.assignment_epoch.is_none()
                && description.members.iter().all(classic_member)
        }
        GroupProtocol::Consumer => {
            description.protocol_type.is_none()
                && description.group_epoch.is_some_and(|epoch| epoch > 0)
                && description.assignment_epoch.is_some_and(|epoch| epoch > 0)
                && description
                    .members
                    .iter()
                    .all(|member| consumer_member(member, expected))
        }
    }
}

fn classic_member(member: &testlab_schema::AdminConsumerGroupMemberDescription) -> bool {
    common_member(member)
        && member.rack_id.is_none()
        && member.member_epoch.is_none()
        && member.subscribed_topic_names.is_empty()
        && regex_is_absent(member.subscribed_topic_regex.as_deref())
        && member.assignment.is_empty()
        && member.target_assignment.is_empty()
        && member.member_type.is_none()
        && !member.classic_metadata.is_empty()
        && !member.classic_assignment.is_empty()
}

fn consumer_member(
    member: &testlab_schema::AdminConsumerGroupMemberDescription,
    expected: &testlab_schema::ConsumerGroupDescriptionExpectation,
) -> bool {
    common_member(member)
        && member.member_epoch.is_some_and(|epoch| epoch > 0)
        && member.subscribed_topic_names.len() == 1
        && member.subscribed_topic_names.first() == Some(&expected.expected_topic)
        && regex_is_absent(member.subscribed_topic_regex.as_deref())
        && member.classic_metadata.is_empty()
        && member.classic_assignment.is_empty()
        && exact_assignment(&member.assignment, expected)
        && canonical_assignment(&member.target_assignment)
}

fn regex_is_absent(regex: Option<&str>) -> bool {
    regex.is_none_or(str::is_empty)
}

fn common_member(member: &testlab_schema::AdminConsumerGroupMemberDescription) -> bool {
    !member.member_id.is_empty() && !member.client_id.is_empty() && !member.client_host.is_empty()
}

fn exact_assignment(
    assignment: &[testlab_schema::AdminConsumerGroupTopicAssignment],
    expected: &testlab_schema::ConsumerGroupDescriptionExpectation,
) -> bool {
    canonical_assignment(assignment)
        && assignment.iter().any(|topic| {
            topic.topic_name == expected.expected_topic
                && topic.topic_id != [0; 16]
                && topic
                    .partitions
                    .binary_search(&expected.expected_partition)
                    .is_ok()
        })
}

fn canonical_assignment(assignment: &[testlab_schema::AdminConsumerGroupTopicAssignment]) -> bool {
    assignment.iter().all(|topic| {
        !topic.topic_name.is_empty()
            && topic.topic_id != [0; 16]
            && !topic.partitions.is_empty()
            && topic.partitions.iter().all(|partition| *partition >= 0)
            && topic.partitions.windows(2).all(|pair| pair[0] < pair[1])
    }) && assignment.windows(2).all(|pair| {
        (pair[0].topic_id, pair[0].topic_name.as_str())
            < (pair[1].topic_id, pair[1].topic_name.as_str())
    })
}

fn strictly_ordered_members(
    members: &[testlab_schema::AdminConsumerGroupMemberDescription],
) -> bool {
    members
        .windows(2)
        .all(|pair| pair[0].member_id.as_bytes() < pair[1].member_id.as_bytes())
}

fn exact_live_epochs(
    scenario: &Scenario,
    expected: &testlab_schema::DescribeConsumerGroupsAction,
    index: &HistoryIndex,
    command_sequence: u64,
) -> bool {
    let live = live_consumers(scenario, &expected.operation_id);
    expected.groups.iter().all(|group| {
        let matching = live
            .values()
            .filter(|consumer| {
                consumer.group_id == group.group_id
                    && consumer.protocol == group.protocol
                    && consumer.topics.contains(&group.expected_topic)
            })
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
                                        epoch.protocol() == group.protocol && epoch.is_positive()
                                    })
                                    && receive.records.iter().any(|record| {
                                        record.topic == group.expected_topic
                                            && record.partition == group.expected_partition
                                    })
                            })
                    })
                })
            })
    })
}

fn live_consumers(
    scenario: &Scenario,
    description_id: &OperationId,
) -> BTreeMap<ConsumerId, LiveConsumer> {
    let mut live = BTreeMap::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DescribeConsumerGroups(value)
                if &value.operation_id == description_id =>
            {
                break;
            }
            ScenarioAction::CreateGroupConsumer {
                consumer_id,
                group_id,
                protocol,
                topics,
                ..
            } => {
                live.insert(
                    consumer_id.clone(),
                    LiveConsumer {
                        group_id: group_id.clone(),
                        protocol: *protocol,
                        topics: topics.clone(),
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
            ScenarioAction::CloseGroupConsumer { consumer_id }
            | ScenarioAction::AbandonGroupConsumer(testlab_schema::GroupConsumerAbandonment {
                consumer_id,
            })
            | ScenarioAction::ShutdownGroupConsumer(
                testlab_schema::GroupConsumerShutdownAction { consumer_id, .. },
            ) => {
                live.remove(consumer_id);
            }
            _ => {}
        }
    }
    live
}

fn one<T>(values: Option<&Vec<T>>) -> Option<&T> {
    values.filter(|values| values.len() == 1)?.first()
}
