//! Plural configuration-mutation normalization rejects lossy or reordered results.

use testlab_schema::{OperationId, TopicConfigAlteration};

use crate::kafkars_api::{ErrorKind, KafkaError};
use crate::protocol_admin_config_batch_mutation::outcomes;

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
        alteration("topic-z", "cleanup.policy", "compact"),
        alteration("topic-a", "cleanup.policy", "compact"),
    ]
}

fn alteration(topic: &str, config_name: &str, value: &str) -> TopicConfigAlteration {
    TopicConfigAlteration {
        topic: topic.to_owned(),
        config_name: config_name.to_owned(),
        value: value.to_owned(),
    }
}

fn operation() -> OperationId {
    OperationId::new("alter-topic-configs").unwrap_or_else(|error| panic!("operation: {error}"))
}
