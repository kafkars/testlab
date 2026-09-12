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
mod tests {
    use super::validate;
    use crate::ConsumerId;

    #[test]
    fn subscription_requires_a_topic() {
        let mut problems = Vec::new();
        validate(&consumer(), &[], &mut problems);
        assert!(
            problems
                .iter()
                .any(|value| value.contains("between 1 and 32"))
        );
    }

    #[test]
    fn subscription_topics_are_distinct_and_valid() {
        let mut problems = Vec::new();
        validate(
            &consumer(),
            &["orders".to_owned(), "orders".to_owned(), String::new()],
            &mut problems,
        );
        assert!(
            problems
                .iter()
                .any(|value| value.contains("repeats topic orders"))
        );
        assert!(problems.iter().any(|value| value.contains("invalid topic")));
    }

    fn consumer() -> ConsumerId {
        ConsumerId::new("consumer-1").unwrap_or_else(|error| panic!("consumer id: {error}"))
    }
}
