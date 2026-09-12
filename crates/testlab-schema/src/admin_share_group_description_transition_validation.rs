//! Share-group descriptions require exact live members retaining acquired public batches.

use std::collections::{BTreeMap, BTreeSet};

use crate::{ConsumerId, OperationId, Scenario, ScenarioAction};

#[derive(Clone)]
struct LiveShare {
    group_id: String,
    topics: Vec<String>,
    retained: BTreeSet<OperationId>,
}

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut consumers = BTreeMap::<ConsumerId, LiveShare>::new();
    let mut receives = BTreeMap::<OperationId, ConsumerId>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateShareConsumer {
                consumer_id,
                group_id,
                topics,
                ..
            } => {
                consumers.insert(
                    consumer_id.clone(),
                    LiveShare {
                        group_id: group_id.clone(),
                        topics: topics.clone(),
                        retained: BTreeSet::new(),
                    },
                );
            }
            ScenarioAction::ShareReceive {
                consumer_id,
                receive_id,
                ..
            } => {
                receives.insert(receive_id.clone(), consumer_id.clone());
                if let Some(consumer) = consumers.get_mut(consumer_id) {
                    consumer.retained.insert(receive_id.clone());
                }
            }
            ScenarioAction::ShareAcknowledge { receive_id, .. }
            | ScenarioAction::DropShareBatch { receive_id, .. } => {
                settle(receive_id, &receives, &mut consumers);
            }
            ScenarioAction::CloseShareConsumer { consumer_id, .. } => {
                consumers.remove(consumer_id);
            }
            ScenarioAction::DescribeShareGroup(action) => validate_group(
                &action.operation_id,
                &action.group_id,
                &action.expected_topic,
                action.expected_member_count,
                &consumers,
                problems,
            ),
            ScenarioAction::DescribeShareGroups(action) => {
                for group in &action.groups {
                    validate_group(
                        &action.operation_id,
                        &group.group_id,
                        &group.expected_topic,
                        group.expected_member_count,
                        &consumers,
                        problems,
                    );
                }
            }
            _ => {}
        }
    }
}

fn settle(
    receive_id: &OperationId,
    receives: &BTreeMap<OperationId, ConsumerId>,
    consumers: &mut BTreeMap<ConsumerId, LiveShare>,
) {
    if let Some(consumer_id) = receives.get(receive_id)
        && let Some(consumer) = consumers.get_mut(consumer_id)
    {
        consumer.retained.remove(receive_id);
    }
}

fn validate_group(
    operation_id: &OperationId,
    group_id: &str,
    topic: &str,
    member_count: u32,
    consumers: &BTreeMap<ConsumerId, LiveShare>,
    problems: &mut Vec<String>,
) {
    let modeled = consumers
        .values()
        .filter(|consumer| {
            consumer.group_id == group_id
                && consumer.topics.iter().any(|candidate| candidate == topic)
                && !consumer.retained.is_empty()
        })
        .count();
    if usize::try_from(member_count) != Ok(modeled) {
        problems.push(format!(
            "admin operation {operation_id} Share group {group_id} expects {member_count} members but scenario models {modeled} open members retaining a batch from {topic}"
        ));
    }
}
