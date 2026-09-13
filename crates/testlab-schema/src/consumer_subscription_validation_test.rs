//! Tests for the consumer subscription validation contract.

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
