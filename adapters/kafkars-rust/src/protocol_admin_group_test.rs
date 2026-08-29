//! Group-admin result tests reject malformed public batch identities.

use crate::kafkars_api::{ErrorKind, KafkaError, StartPosition, TopicPartition};
use testlab_schema::{AdminBrokerError, OperationId};

use crate::AdapterError;
use crate::protocol_admin_result::{
    sorted_unique_broker_errors, sorted_unique_nonnegative, sorted_unique_strings,
    take_single_result,
};

#[test]
fn exact_group_result_succeeds() {
    let result = take_group(vec![("payments".to_owned(), Ok(7_u32))], "payments");

    assert_eq!(
        result.unwrap_or_else(|error| panic!("group result: {error}")),
        7
    );
}

#[test]
fn malformed_group_results_are_invalid() {
    let empty = take_group(Vec::<(String, Result<u32, KafkaError>)>::new(), "payments");
    let wrong = take_group(vec![("audit".to_owned(), Ok(1))], "payments");
    let extra = take_group(
        vec![("payments".to_owned(), Ok(1)), ("audit".to_owned(), Ok(2))],
        "payments",
    );

    for result in [empty, wrong, extra] {
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

#[test]
fn group_result_preserves_client_failure() {
    let result = take_group(
        vec![(
            "payments".to_owned(),
            Err(KafkaError::new(ErrorKind::Broker, "group failed")),
        )],
        "payments",
    );

    assert!(matches!(result, Err(AdapterError::Client(_))));
}

#[test]
fn exact_partition_result_rejects_assignment_start_position() {
    let result = take_single_result(
        vec![(
            TopicPartition::new("orders", 0).start_at(StartPosition::Offset(4)),
            Ok(()),
        )],
        &operation_id(),
        |key| key.topic() == "orders" && key.partition() == 0 && key.start_position().is_none(),
        "consumer-group offset",
    );

    assert!(matches!(result, Err(AdapterError::AdminResult(_))));
}

#[test]
fn set_results_are_canonicalized_and_invalid_identities_rejected() {
    let strings = sorted_unique_strings(
        vec!["payments".to_owned(), "audit".to_owned()],
        &operation_id(),
        "groups",
    )
    .unwrap_or_else(|error| panic!("sort groups: {error}"));
    let integers = sorted_unique_nonnegative(vec![7, 2, 4], &operation_id(), "brokers")
        .unwrap_or_else(|error| panic!("sort brokers: {error}"));
    let duplicate = sorted_unique_strings(
        vec!["audit".to_owned(), "audit".to_owned()],
        &operation_id(),
        "groups",
    );
    let negative = sorted_unique_nonnegative(vec![-1], &operation_id(), "brokers");

    assert_eq!(strings, ["audit", "payments"]);
    assert_eq!(integers, [2, 4, 7]);
    assert!(matches!(duplicate, Err(AdapterError::AdminResult(_))));
    assert!(matches!(negative, Err(AdapterError::AdminResult(_))));
}

#[test]
fn broker_errors_are_sorted_and_duplicate_brokers_rejected() {
    let errors = sorted_unique_broker_errors(
        vec![broker_error(7, 31), broker_error(2, -1)],
        &operation_id(),
    )
    .unwrap_or_else(|error| panic!("sort broker errors: {error}"));
    let duplicate = sorted_unique_broker_errors(
        vec![broker_error(2, 1), broker_error(2, 2)],
        &operation_id(),
    );

    assert_eq!(errors, [broker_error(2, -1), broker_error(7, 31)]);
    assert!(matches!(duplicate, Err(AdapterError::AdminResult(_))));
}

fn take_group(
    entries: Vec<(String, Result<u32, KafkaError>)>,
    expected: &str,
) -> Result<u32, AdapterError> {
    take_single_result(
        entries,
        &operation_id(),
        |group_id| group_id == expected,
        "consumer group",
    )
}

fn operation_id() -> OperationId {
    OperationId::new("admin-group-1").unwrap_or_else(|error| panic!("operation id: {error}"))
}

const fn broker_error(broker_id: i32, code: i16) -> AdminBrokerError {
    AdminBrokerError { broker_id, code }
}
