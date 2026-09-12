//! Mixed descriptions require live received members for each declared protocol.

use std::collections::BTreeMap;

use crate::{ConsumerId, GroupProtocol, Scenario, ScenarioAction};

#[derive(Clone)]
struct LiveConsumer {
    group_id: String,
    topics: Vec<String>,
    protocol: GroupProtocol,
    received: bool,
}

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut consumers = BTreeMap::<ConsumerId, LiveConsumer>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateGroupConsumer {
                consumer_id,
                group_id,
                topics,
                protocol,
                ..
            } => {
                consumers.insert(
                    consumer_id.clone(),
                    LiveConsumer {
                        group_id: group_id.clone(),
                        topics: topics.clone(),
                        protocol: *protocol,
                        received: false,
                    },
                );
            }
            ScenarioAction::GroupReceive { consumer_id, .. } => {
                if let Some(consumer) = consumers.get_mut(consumer_id) {
                    consumer.received = true;
                }
            }
            ScenarioAction::CloseGroupConsumer { consumer_id }
            | ScenarioAction::AbandonGroupConsumer(crate::GroupConsumerAbandonment {
                consumer_id,
            })
            | ScenarioAction::ShutdownGroupConsumer(crate::GroupConsumerShutdownAction {
                consumer_id,
                ..
            }) => {
                consumers.remove(consumer_id);
            }
            ScenarioAction::DescribeConsumerGroups(action) => {
                validate_description(action, &consumers, problems);
            }
            _ => {}
        }
    }
}

fn validate_description(
    action: &crate::DescribeConsumerGroupsAction,
    consumers: &BTreeMap<ConsumerId, LiveConsumer>,
    problems: &mut Vec<String>,
) {
    let has_classic = action
        .groups
        .iter()
        .any(|group| group.protocol == GroupProtocol::Classic);
    let has_consumer = action
        .groups
        .iter()
        .any(|group| group.protocol == GroupProtocol::Consumer);
    if !has_classic || !has_consumer {
        problems.push(format!(
            "admin operation {} must describe both classic and KIP-848 groups",
            action.operation_id
        ));
    }
    for group in &action.groups {
        validate_live_group(action, group, consumers, problems);
    }
}

fn validate_live_group(
    action: &crate::DescribeConsumerGroupsAction,
    group: &crate::ConsumerGroupDescriptionExpectation,
    consumers: &BTreeMap<ConsumerId, LiveConsumer>,
    problems: &mut Vec<String>,
) {
    let matching = consumers
        .values()
        .filter(|consumer| {
            consumer.group_id == group.group_id && consumer.protocol == group.protocol
        })
        .collect::<Vec<_>>();
    if usize::try_from(group.expected_member_count) != Ok(matching.len()) {
        problems.push(format!(
            "admin operation {} group {} expects {} members but scenario models {} open {:?} members",
            action.operation_id,
            group.group_id,
            group.expected_member_count,
            matching.len(),
            group.protocol
        ));
    }
    if matching.iter().any(|consumer| !consumer.received) {
        problems.push(format!(
            "admin operation {} group {} requires every live member to complete group_receive",
            action.operation_id, group.group_id
        ));
    }
    if matching
        .iter()
        .any(|consumer| !consumer.topics.contains(&group.expected_topic))
    {
        problems.push(format!(
            "admin operation {} group {} expected_topic does not match its live members",
            action.operation_id, group.group_id
        ));
    }
}
