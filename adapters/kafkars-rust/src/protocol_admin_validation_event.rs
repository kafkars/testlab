//! Validate-only admin results remain distinct from mutation completions.

use testlab_schema::{AdapterEvent, AdminTopicCompletion, AdminTopicConfigCompletion};

pub(crate) fn topic_creation(
    validate_only: bool,
    completion: AdminTopicCompletion,
) -> AdapterEvent {
    if validate_only {
        AdapterEvent::TopicCreationValidated(completion)
    } else {
        AdapterEvent::TopicCreated(completion)
    }
}

pub(crate) fn partition_increase(
    validate_only: bool,
    completion: AdminTopicCompletion,
) -> AdapterEvent {
    if validate_only {
        AdapterEvent::TopicPartitionIncreaseValidated(completion)
    } else {
        AdapterEvent::TopicPartitionsCreated(completion)
    }
}

pub(crate) fn config_alteration(
    validate_only: bool,
    completion: AdminTopicConfigCompletion,
) -> AdapterEvent {
    if validate_only {
        AdapterEvent::TopicConfigAlterationValidated(completion)
    } else {
        AdapterEvent::TopicConfigAltered(completion)
    }
}
