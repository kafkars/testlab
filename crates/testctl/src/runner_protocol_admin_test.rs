//! Admin protocol tests keep result assertions out of execution validity.

use testlab_schema::{AdapterEvent, OperationId};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn describe_completion_checks_identity_but_not_semantic_partitions() {
    let operation_id = id(OperationId::new("describe-1"));
    let expected = ExpectedEvent::TopicDescribed {
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
    };

    assert_eq!(
        expected
            .classify(&AdapterEvent::TopicDescribed {
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                partitions: vec![9],
            })
            .unwrap_or_else(|error| panic!("describe classification: {error}")),
        EventDisposition::Complete
    );
    assert!(
        expected
            .classify(&AdapterEvent::TopicDescribed {
                operation_id,
                topic: "payments".to_owned(),
                partitions: vec![0],
            })
            .is_err()
    );
}

#[test]
fn list_topics_completion_checks_operation_identity_only() {
    let operation_id = id(OperationId::new("list-topics-1"));
    let expected = ExpectedEvent::TopicsListed {
        operation_id: operation_id.clone(),
    };

    assert_eq!(
        expected
            .classify(&AdapterEvent::TopicsListed {
                operation_id,
                topics: Vec::new(),
            })
            .unwrap_or_else(|error| panic!("list topics classification: {error}")),
        EventDisposition::Complete
    );
}

#[test]
fn list_offset_completion_checks_topic_partition_and_operation() {
    let operation_id = id(OperationId::new("list-offset-1"));
    let expected = ExpectedEvent::OffsetListed {
        operation_id: operation_id.clone(),
        topic: "orders".to_owned(),
        partition: 2,
    };

    assert_eq!(
        expected
            .classify(&AdapterEvent::OffsetListed {
                operation_id: operation_id.clone(),
                topic: "orders".to_owned(),
                partition: 2,
                offset: None,
            })
            .unwrap_or_else(|error| panic!("list offset classification: {error}")),
        EventDisposition::Complete
    );
    assert!(
        expected
            .classify(&AdapterEvent::OffsetListed {
                operation_id,
                topic: "orders".to_owned(),
                partition: 1,
                offset: Some(2),
            })
            .is_err()
    );
}

#[test]
fn group_offset_completion_checks_every_stable_identity() {
    let operation_id = id(OperationId::new("group-offset-1"));
    let expected = ExpectedEvent::ConsumerGroupOffsetListed {
        operation_id: operation_id.clone(),
        group_id: "group-1".to_owned(),
        topic: "orders".to_owned(),
        partition: 2,
    };

    assert_eq!(
        expected
            .classify(&AdapterEvent::ConsumerGroupOffsetListed {
                operation_id: operation_id.clone(),
                group_id: "group-1".to_owned(),
                topic: "orders".to_owned(),
                partition: 2,
                offset: None,
            })
            .unwrap_or_else(|error| panic!("group offset classification: {error}")),
        EventDisposition::Complete
    );
    assert!(
        expected
            .classify(&AdapterEvent::ConsumerGroupOffsetListed {
                operation_id,
                group_id: "other-group".to_owned(),
                topic: "orders".to_owned(),
                partition: 2,
                offset: Some(42),
            })
            .is_err()
    );
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
