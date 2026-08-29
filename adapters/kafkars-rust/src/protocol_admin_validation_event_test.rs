//! Validate-only admin event tests cover validation and mutation selections.

use testlab_schema::{AdapterEvent, AdminTopicCompletion, AdminTopicConfigCompletion, OperationId};

use crate::protocol_admin_validation_event::{
    config_alteration, partition_increase, topic_creation,
};

#[test]
fn topic_creation_selects_validation_event() {
    let completion = topic_completion();
    assert_eq!(
        topic_creation(true, completion.clone()),
        AdapterEvent::TopicCreationValidated(completion)
    );
}

#[test]
fn topic_creation_selects_mutation_event() {
    let completion = topic_completion();
    assert_eq!(
        topic_creation(false, completion.clone()),
        AdapterEvent::TopicCreated(completion)
    );
}

#[test]
fn partition_increase_selects_validation_event() {
    let completion = topic_completion();
    assert_eq!(
        partition_increase(true, completion.clone()),
        AdapterEvent::TopicPartitionIncreaseValidated(completion)
    );
}

#[test]
fn partition_increase_selects_mutation_event() {
    let completion = topic_completion();
    assert_eq!(
        partition_increase(false, completion.clone()),
        AdapterEvent::TopicPartitionsCreated(completion)
    );
}

#[test]
fn config_alteration_selects_validation_event() {
    let completion = config_completion();
    assert_eq!(
        config_alteration(true, completion.clone()),
        AdapterEvent::TopicConfigAlterationValidated(completion)
    );
}

#[test]
fn config_alteration_selects_mutation_event() {
    let completion = config_completion();
    assert_eq!(
        config_alteration(false, completion.clone()),
        AdapterEvent::TopicConfigAltered(completion)
    );
}

fn topic_completion() -> AdminTopicCompletion {
    AdminTopicCompletion {
        operation_id: operation(),
        topic: "orders".to_owned(),
    }
}

fn config_completion() -> AdminTopicConfigCompletion {
    AdminTopicConfigCompletion {
        operation_id: operation(),
        topic: "orders".to_owned(),
        config_name: "cleanup.policy".to_owned(),
    }
}

fn operation() -> OperationId {
    OperationId::new("validate-only").unwrap_or_else(|error| panic!("operation ID: {error}"))
}
