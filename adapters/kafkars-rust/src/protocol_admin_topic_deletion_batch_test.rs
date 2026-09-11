//! Plural topic-deletion normalization preserves every caller-positioned outcome.

use testlab_schema::OperationId;

use crate::AdapterError;
use crate::kafkars_api::{ErrorKind, KafkaError};
use crate::protocol_admin_topic_deletion_batch::outcomes;

#[test]
fn mixed_outcomes_preserve_caller_order_and_errors() {
    let values = outcomes(
        vec![
            ("zulu".to_owned(), Ok(())),
            (
                "missing".to_owned(),
                Err(KafkaError::new(ErrorKind::Broker, "unknown topic")),
            ),
            ("alpha".to_owned(), Ok(())),
        ],
        &topics(&["zulu", "missing", "alpha"]),
        &operation(),
    )
    .unwrap_or_else(|error| panic!("normalize deletion outcomes: {error}"));
    assert_eq!(values[0].topic, "zulu");
    assert_eq!(values[0].error_code, None);
    assert_eq!(values[1].topic, "missing");
    assert_eq!(values[1].error_code.as_deref(), Some("broker"));
    assert_eq!(values[2].topic, "alpha");
    assert_eq!(values[2].error_code, None);
}

#[test]
fn missing_extra_or_reordered_outcomes_are_invalid() {
    for entries in [
        vec![("zulu".to_owned(), Ok(()))],
        vec![
            ("zulu".to_owned(), Ok(())),
            ("alpha".to_owned(), Ok(())),
            ("extra".to_owned(), Ok(())),
        ],
        vec![("alpha".to_owned(), Ok(())), ("zulu".to_owned(), Ok(()))],
    ] {
        let result = outcomes(entries, &topics(&["zulu", "alpha"]), &operation());
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

fn topics(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn operation() -> OperationId {
    OperationId::new("admin-delete-topics").unwrap_or_else(|error| panic!("operation id: {error}"))
}
