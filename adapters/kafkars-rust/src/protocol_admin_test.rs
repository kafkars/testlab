//! Admin result tests preserve per-topic failures and reject malformed batch shapes.

use kafkars::{ErrorKind, KafkaError};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::protocol_admin_result::validate_single_topic_result;

#[test]
fn single_matching_topic_result_succeeds() {
    let result = validate_single_topic_result(
        vec![("orders".to_owned(), Ok(()))],
        &operation_id(),
        "orders",
    );

    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn empty_topic_result_is_invalid() {
    let result = validate_single_topic_result(Vec::new(), &operation_id(), "orders");

    assert!(matches!(result, Err(AdapterError::AdminResult(_))));
}

#[test]
fn mismatched_topic_result_is_invalid() {
    let result = validate_single_topic_result(
        vec![("audit".to_owned(), Ok(()))],
        &operation_id(),
        "orders",
    );

    assert!(matches!(result, Err(AdapterError::AdminResult(_))));
}

#[test]
fn extra_topic_result_is_invalid() {
    let result = validate_single_topic_result(
        vec![("orders".to_owned(), Ok(())), ("audit".to_owned(), Ok(()))],
        &operation_id(),
        "orders",
    );

    assert!(matches!(result, Err(AdapterError::AdminResult(_))));
}

#[test]
fn per_topic_error_remains_a_client_failure() {
    let result = validate_single_topic_result(
        vec![(
            "orders".to_owned(),
            Err(KafkaError::new(
                ErrorKind::Broker,
                "partition increase rejected",
            )),
        )],
        &operation_id(),
        "orders",
    );

    assert!(matches!(result, Err(AdapterError::Client(_))));
}

fn operation_id() -> OperationId {
    OperationId::new("admin-partitions-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}
