//! Topic-ID result normalization preserves caller order and exact request keys.

use testlab_schema::OperationId;

use crate::AdapterError;
use crate::kafkars_api::{ErrorKind, KafkaError};

#[test]
fn deletion_results_retain_caller_topic_names_and_id_keys() {
    let expected = expected();
    let outcomes = crate::protocol_admin_topic_deletion_batch::outcomes_by_id(
        vec![([1; 16], Ok(())), ([2; 16], Ok(()))],
        &expected,
        &operation(),
    )
    .unwrap_or_else(|error| panic!("normalize topic-ID deletion: {error}"));
    assert_eq!(outcomes[0].topic, "zulu");
    assert_eq!(outcomes[0].topic_id, Some([1; 16]));
    assert_eq!(outcomes[1].topic, "alpha");
    assert_eq!(outcomes[1].topic_id, Some([2; 16]));
}

#[test]
fn description_errors_still_retain_exact_id_keys() {
    let expected = expected();
    let outcomes = crate::protocol_admin_topic_description_batch::outcomes_by_id(
        vec![
            ([1; 16], Err(error("zulu"))),
            ([2; 16], Err(error("alpha"))),
        ],
        &expected,
        &operation(),
    )
    .unwrap_or_else(|error| panic!("normalize topic-ID descriptions: {error}"));
    assert_eq!(outcomes[0].topic_id, Some([1; 16]));
    assert_eq!(outcomes[1].topic_id, Some([2; 16]));
    assert!(outcomes.iter().all(|outcome| outcome.error_code.is_some()));
}

#[test]
fn reordered_missing_or_extra_id_results_are_invalid() {
    let expected = expected();
    for entries in [
        vec![([2; 16], Ok(())), ([1; 16], Ok(()))],
        vec![([1; 16], Ok(()))],
        vec![([1; 16], Ok(())), ([2; 16], Ok(())), ([3; 16], Ok(()))],
    ] {
        let result = crate::protocol_admin_topic_deletion_batch::outcomes_by_id(
            entries,
            &expected,
            &operation(),
        );
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

fn expected() -> Vec<(String, [u8; 16])> {
    vec![("zulu".to_owned(), [1; 16]), ("alpha".to_owned(), [2; 16])]
}

fn error(message: &str) -> KafkaError {
    KafkaError::new(ErrorKind::Broker, message)
}

fn operation() -> OperationId {
    OperationId::new("admin-topic-ids").unwrap_or_else(|error| panic!("operation: {error}"))
}
