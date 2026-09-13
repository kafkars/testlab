//! Group subscription validation owns topic-list bounds and identity.

use std::collections::BTreeSet;

use crate::ConsumerId;

const MAX_TOPICS: usize = 32;

pub(super) fn validate(consumer_id: &ConsumerId, topics: &[String], problems: &mut Vec<String>) {
    if topics.is_empty() || topics.len() > MAX_TOPICS {
        problems.push(format!(
            "consumer {consumer_id} subscription must contain between 1 and {MAX_TOPICS} topics"
        ));
    }
    let mut unique = BTreeSet::new();
    for topic in topics {
        super::validate_name(consumer_id, "topic", topic, 249, problems);
        if !unique.insert(topic) {
            problems.push(format!(
                "consumer {consumer_id} subscription repeats topic {topic}"
            ));
        }
    }
}

#[cfg(test)]
#[path = "consumer_subscription_validation_test.rs"]
mod tests;
