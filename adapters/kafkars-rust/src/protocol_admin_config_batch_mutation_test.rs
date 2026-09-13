//! Plural configuration-mutation normalization rejects lossy or reordered results.

use testlab_schema::{OperationId, TopicConfigAlteration, TopicConfigMutationMethod};

use crate::kafkars_api::{ConfigAlterationOperation, ErrorKind, KafkaError};
use crate::protocol_admin_config_alteration::{incremental_alteration, legacy_entry};
use crate::protocol_admin_config_batch_mutation::outcomes;

#[test]
fn every_incremental_method_maps_to_its_exact_public_constructor() {
    for (method, value, expected) in [
        (
            TopicConfigMutationMethod::Set,
            Some("compact"),
            ConfigAlterationOperation::Set("compact".to_owned()),
        ),
        (
            TopicConfigMutationMethod::Delete,
            None,
            ConfigAlterationOperation::Delete,
        ),
        (
            TopicConfigMutationMethod::Append,
            Some("compact"),
            ConfigAlterationOperation::Append("compact".to_owned()),
        ),
        (
            TopicConfigMutationMethod::Subtract,
            Some("delete"),
            ConfigAlterationOperation::Subtract("delete".to_owned()),
        ),
    ] {
        let selected = alteration("topic-z", "cleanup.policy", method, value);
        let mapped = incremental_alteration(&selected, &operation())
            .unwrap_or_else(|error| panic!("map incremental alteration: {error}"));
        assert_eq!(mapped.key(), "cleanup.policy");
        assert_eq!(mapped.operation(), &expected);
    }
}

#[test]
fn legacy_set_and_restoration_map_without_incremental_substitution() {
    let set = alteration(
        "topic-z",
        "cleanup.policy",
        TopicConfigMutationMethod::Set,
        Some("compact"),
    );
    let restore = alteration(
        "topic-z",
        "cleanup.policy",
        TopicConfigMutationMethod::RestoreDefault,
        None,
    );
    let mapped_set =
        legacy_entry(&set, &operation()).unwrap_or_else(|error| panic!("map legacy set: {error}"));
    let mapped_restore = legacy_entry(&restore, &operation())
        .unwrap_or_else(|error| panic!("map legacy restoration: {error}"));
    assert_eq!(mapped_set.value(), Some("compact"));
    assert_eq!(mapped_restore.value(), None);
    assert!(incremental_alteration(&restore, &operation()).is_err());
}

#[test]
fn missing_or_extra_operands_are_rejected_before_public_submission() {
    for (method, value) in [
        (TopicConfigMutationMethod::Append, None),
        (TopicConfigMutationMethod::Delete, Some("delete")),
    ] {
        let selected = alteration("topic-z", "cleanup.policy", method, value);
        assert!(incremental_alteration(&selected, &operation()).is_err());
    }
    for (method, value) in [
        (TopicConfigMutationMethod::Set, None),
        (TopicConfigMutationMethod::RestoreDefault, Some("delete")),
    ] {
        let selected = alteration("topic-z", "cleanup.policy", method, value);
        assert!(legacy_entry(&selected, &operation()).is_err());
    }
}

#[test]
fn every_public_outcome_remains_in_caller_order() {
    let outcomes = outcomes(
        vec![
            ("topic-z".to_owned(), Ok(())),
            (
                "topic-a".to_owned(),
                Err(KafkaError::new(ErrorKind::Broker, "alter failed")),
            ),
        ],
        &alterations(),
        &operation(),
    )
    .unwrap_or_else(|error| panic!("normalize plural configuration mutation: {error}"));

    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].topic, "topic-z");
    assert_eq!(outcomes[0].config_name, "cleanup.policy");
    assert!(outcomes[0].error_code.is_none());
    assert_eq!(outcomes[1].topic, "topic-a");
    assert_eq!(outcomes[1].error_code.as_deref(), Some("broker"));
}

#[test]
fn reordered_missing_and_extra_topic_results_are_rejected() {
    let malformed = [
        vec![
            ("topic-a".to_owned(), Ok(())),
            ("topic-z".to_owned(), Ok(())),
        ],
        vec![("topic-z".to_owned(), Ok(()))],
        vec![
            ("topic-z".to_owned(), Ok(())),
            ("topic-a".to_owned(), Ok(())),
            ("topic-extra".to_owned(), Ok(())),
        ],
    ];
    for entries in malformed {
        assert!(outcomes(entries, &alterations(), &operation()).is_err());
    }
}

fn alterations() -> Vec<TopicConfigAlteration> {
    vec![
        alteration(
            "topic-z",
            "cleanup.policy",
            TopicConfigMutationMethod::Set,
            Some("compact"),
        ),
        alteration(
            "topic-a",
            "cleanup.policy",
            TopicConfigMutationMethod::Set,
            Some("compact"),
        ),
    ]
}

fn alteration(
    topic: &str,
    config_name: &str,
    method: TopicConfigMutationMethod,
    value: Option<&str>,
) -> TopicConfigAlteration {
    TopicConfigAlteration {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        method,
        value: value.map(str::to_owned),
    }
}

fn operation() -> OperationId {
    OperationId::new("alter-topic-configs").unwrap_or_else(|error| panic!("operation: {error}"))
}
