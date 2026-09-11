//! Leader-election fixtures own topics and explicit disruption preconditions.

use std::collections::{BTreeMap, BTreeSet};

use crate::{BrokerRoleTarget, Scenario, ScenarioAction};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut topics = BTreeMap::<String, i32>::new();
    let mut stopped = BTreeSet::new();
    let mut restored = BTreeSet::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateTopic(action)
                if action.expected_error_code.is_none() && !action.validate_only =>
            {
                topics.insert(action.topic.clone(), action.partitions);
            }
            ScenarioAction::CreateTopicsBatch(action) if !action.validate_only => {
                for topic in &action.topics {
                    if topic.expected_error_code.is_none() {
                        topics.insert(topic.topic.clone(), topic.partitions);
                    }
                }
            }
            ScenarioAction::CreatePartitions(action)
                if action.expected_error_code.is_none() && !action.validate_only =>
            {
                if let Some(partitions) = topics.get_mut(&action.topic) {
                    *partitions = action.total_count;
                }
            }
            ScenarioAction::StopBrokerRole { target, .. } => {
                stopped.insert(target.clone());
            }
            ScenarioAction::RestoreBrokerRole { target, .. } if stopped.remove(target) => {
                restored.insert(target.clone());
            }
            ScenarioAction::ElectLeaders(action) => {
                let targets = action
                    .targets
                    .as_deref()
                    .unwrap_or(&action.required_partitions);
                for target in targets {
                    if !topics.get(&target.topic).is_some_and(|partitions| {
                        target.partition >= 0 && target.partition < *partitions
                    }) {
                        problems.push(format!(
                            "admin operation {} requires prior successful creation of partition {}-{}",
                            action.operation_id, target.topic, target.partition
                        ));
                    }
                }
                if action.require_leader_change {
                    let target = targets.first();
                    let role = target.map(|target| BrokerRoleTarget::PartitionLeader {
                        topic: target.topic.clone(),
                        partition: target.partition,
                    });
                    if role.as_ref().is_none_or(|role| !restored.remove(role)) {
                        problems.push(format!(
                            "admin operation {} requires a prior stop and restore of its selected leader",
                            action.operation_id
                        ));
                    }
                }
            }
            _ => {}
        }
    }
}
