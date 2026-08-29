//! Classic descriptions require established groups and exact live received members.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ConsumerId, GroupProtocol, Scenario, ScenarioAction};

#[derive(Clone)]
struct ClassicConsumer {
    group_id: String,
    received: bool,
}

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut consumers = BTreeMap::<ConsumerId, ClassicConsumer>::new();
    let mut established_groups = BTreeSet::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateGroupConsumer {
                consumer_id,
                group_id,
                protocol: GroupProtocol::Classic,
                ..
            } => {
                established_groups.insert(group_id.clone());
                consumers.insert(
                    consumer_id.clone(),
                    ClassicConsumer {
                        group_id: group_id.clone(),
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
            | ScenarioAction::ShutdownGroupConsumer(crate::GroupConsumerShutdownAction {
                consumer_id,
                ..
            }) => {
                consumers.remove(consumer_id);
            }
            ScenarioAction::DescribeClassicGroups(action) => {
                validate_description(action, &consumers, &established_groups, problems);
            }
            _ => {}
        }
    }
}

fn validate_description(
    action: &crate::DescribeClassicGroupsAction,
    consumers: &BTreeMap<ConsumerId, ClassicConsumer>,
    established_groups: &BTreeSet<String>,
    problems: &mut Vec<String>,
) {
    for group in &action.groups {
        if !established_groups.contains(&group.group_id) {
            problems.push(format!(
                "admin operation {} classic group {} requires a prior classic group consumer creation",
                action.operation_id, group.group_id
            ));
            continue;
        }
        let modeled = consumers
            .values()
            .filter(|consumer| consumer.received && consumer.group_id == group.group_id)
            .count();
        if usize::try_from(group.expected_member_count) != Ok(modeled) {
            problems.push(format!(
                "admin operation {} classic group {} expects {} members but scenario models {modeled} open classic members with prior group_receive",
                action.operation_id, group.group_id, group.expected_member_count
            ));
        }
    }
}
