//! Batch topic creation extends the singleton topic transition model.

use std::collections::{BTreeMap, BTreeSet};

use super::TopicDefinition;

pub(super) fn validate(
    action: &crate::CreateTopicsBatchAction,
    created_topics: &mut BTreeMap<String, TopicDefinition>,
    singleton_topics: &mut BTreeSet<String>,
    problems: &mut Vec<String>,
) {
    for item in &action.topics {
        let definition = TopicDefinition {
            partitions: item.partitions,
            replication_factor: item.replication_factor,
            replica_assignments: None,
        };
        if item.expected_error_code.is_some() {
            let has_singleton = singleton_topics.contains(&item.topic)
                && created_topics
                    .get(&item.topic)
                    .is_some_and(|prior| prior.matches(&definition));
            if !has_singleton {
                problems.push(format!(
                    "admin operation {} batch duplicate {} requires a prior identical successful singleton topic creation",
                    action.operation_id, item.topic
                ));
            }
        } else if created_topics.contains_key(&item.topic) {
            problems.push(format!(
                "admin operation {} expects successful creation of existing topic {}",
                action.operation_id, item.topic
            ));
        } else {
            created_topics.insert(item.topic.clone(), definition);
            singleton_topics.remove(&item.topic);
        }
    }
}
