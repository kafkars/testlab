//! Broker-policy target validation binds external controls to public scenario work.

use std::collections::{BTreeMap, BTreeSet};

use crate::{BrokerAclResource, BrokerPolicy, BrokerQuotaDirection, Scenario, ScenarioAction};

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut topics = BTreeSet::new();
    let mut groups = BTreeMap::new();
    let mut transactions = BTreeSet::new();
    let mut producer_work = false;
    let mut consumer_work = false;
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::Send { record, .. } => {
                topics.insert(record.topic.clone());
                producer_work = true;
            }
            ScenarioAction::SendBatch { operations, .. } => {
                topics.extend(operations.iter().map(|item| item.record.topic.clone()));
                producer_work = true;
            }
            ScenarioAction::CreateTopic(action) => {
                topics.insert(action.topic.clone());
            }
            ScenarioAction::CreateGroupConsumer {
                consumer_id,
                group_id,
                ..
            } => {
                groups.insert(consumer_id.clone(), group_id.clone());
            }
            ScenarioAction::GroupReceive { .. } | ScenarioAction::Receive { .. } => {
                consumer_work = true;
            }
            ScenarioAction::CreateTransactionalProducer {
                transactional_id, ..
            } => {
                transactions.insert(transactional_id.clone());
            }
            _ => {}
        }
    }
    for step in &scenario.steps {
        let ScenarioAction::AlterBrokerPolicy(action) = &step.action else {
            continue;
        };
        let matched = match &action.policy {
            BrokerPolicy::Acl { resource, .. } => match resource {
                BrokerAclResource::Topic { name } => topics.contains(name),
                BrokerAclResource::Group { name } => groups.values().any(|group| group == name),
                BrokerAclResource::TransactionalId { name } => transactions.contains(name),
            },
            BrokerPolicy::Quota { direction, .. } => match direction {
                BrokerQuotaDirection::Producer => producer_work,
                BrokerQuotaDirection::Consumer => consumer_work,
            },
        };
        if !matched {
            problems.push(format!(
                "broker policy {:?} has no matching public scenario work",
                action.policy
            ));
        }
    }
}
