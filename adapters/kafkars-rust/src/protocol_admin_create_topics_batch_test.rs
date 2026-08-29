//! Batched topic-creation result tests preserve outcomes and reject malformed identity shapes.

use crate::kafkars_api::{ErrorKind, KafkaError};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::protocol_admin_create_topics_batch::creation_outcomes;

#[test]
fn mixed_outcomes_preserve_caller_order_and_per_topic_errors() {
    let outcomes = creation_outcomes(
        vec![
            ("created".to_owned(), Ok(())),
            (
                "duplicate".to_owned(),
                Err(KafkaError::new(ErrorKind::Broker, "already exists")),
            ),
            ("also-created".to_owned(), Ok(())),
        ],
        &operation_id(),
        &topics(&["created", "duplicate", "also-created"]),
    )
    .unwrap_or_else(|error| panic!("normalize outcomes: {error}"));

    assert_eq!(outcomes[0].topic, "created");
    assert_eq!(outcomes[0].error_code, None);
    assert_eq!(outcomes[1].topic, "duplicate");
    assert_eq!(outcomes[1].error_code.as_deref(), Some("broker"));
    assert_eq!(outcomes[2].topic, "also-created");
    assert_eq!(outcomes[2].error_code, None);
}

#[test]
fn missing_topic_outcome_is_invalid() {
    let result = creation_outcomes(
        vec![("created".to_owned(), Ok(()))],
        &operation_id(),
        &topics(&["created", "missing"]),
    );

    assert!(matches!(result, Err(AdapterError::AdminResult(_))));
}

#[test]
fn extra_topic_outcome_is_invalid() {
    let result = creation_outcomes(
        vec![
            ("created".to_owned(), Ok(())),
            ("unexpected".to_owned(), Ok(())),
        ],
        &operation_id(),
        &topics(&["created"]),
    );

    assert!(matches!(result, Err(AdapterError::AdminResult(_))));
}

#[test]
fn mismatched_topic_key_is_invalid() {
    let result = creation_outcomes(
        vec![("second".to_owned(), Ok(())), ("first".to_owned(), Ok(()))],
        &operation_id(),
        &topics(&["first", "second"]),
    );

    assert!(matches!(result, Err(AdapterError::AdminResult(_))));
}

fn topics(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn operation_id() -> OperationId {
    OperationId::new("admin-create-topics-batch-1")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
