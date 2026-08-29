//! Plural admin result tests reject malformed identities and retain partial errors.

use crate::kafkars_api::{ErrorKind, KafkaError, StartPosition, TopicPartition};
use testlab_schema::OperationId;

use crate::AdapterError;
use crate::protocol_admin_plural_result::{
    ResourceResult, ordered_group_results, ordered_partition_results,
};

#[test]
fn partition_results_are_reconstructed_in_caller_order() {
    let results = ordered_partition_results(
        vec![
            partition("orders", 2, Ok(Some(31))),
            partition(
                "orders",
                0,
                Err(KafkaError::new(ErrorKind::Broker, "fetch failed")),
            ),
        ],
        &requested_partitions(),
        &operation_id(),
        "offset-listing partition",
    )
    .unwrap_or_else(|error| panic!("normalize partitions: {error}"));

    assert_eq!(results[0].partition, 0);
    assert_eq!(
        results[0].result,
        ResourceResult::Failure("broker".to_owned())
    );
    assert_eq!(results[1].partition, 2);
    assert_eq!(results[1].result, ResourceResult::Success(Some(31)));
}

#[test]
fn partition_results_reject_missing_extra_duplicate_and_positioned_identities() {
    let operation_id = operation_id();
    let requested = requested_partitions();
    let malformed = [
        vec![partition("orders", 0, Ok(None))],
        vec![
            partition("orders", 0, Ok(None)),
            partition("orders", 2, Ok(None)),
            partition("orders", 3, Ok(None)),
        ],
        vec![
            partition("orders", 0, Ok(None)),
            partition("orders", 0, Ok(None)),
        ],
        vec![
            (
                TopicPartition::new("orders", 0).start_at(StartPosition::Beginning),
                Ok(None),
            ),
            partition("orders", 2, Ok(None)),
        ],
    ];

    for entries in malformed {
        let result = ordered_partition_results(
            entries,
            &requested,
            &operation_id,
            "offset-listing partition",
        );
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

#[test]
fn group_results_are_reconstructed_and_retain_group_errors() {
    let requested = vec!["alpha".to_owned(), "beta".to_owned()];
    let results = ordered_group_results(
        vec![
            ("beta".to_owned(), Ok(2_u32)),
            (
                "alpha".to_owned(),
                Err(KafkaError::new(ErrorKind::Broker, "describe failed")),
            ),
        ],
        &requested,
        &operation_id(),
        "classic-group description",
    )
    .unwrap_or_else(|error| panic!("normalize groups: {error}"));

    assert_eq!(results[0].group_id, "alpha");
    assert_eq!(
        results[0].result,
        ResourceResult::Failure("broker".to_owned())
    );
    assert_eq!(results[1].group_id, "beta");
    assert_eq!(results[1].result, ResourceResult::Success(2));
}

#[test]
fn group_results_reject_missing_extra_duplicate_and_mismatched_identities() {
    let operation_id = operation_id();
    let requested = vec!["alpha".to_owned(), "beta".to_owned()];
    let malformed = [
        vec![("alpha".to_owned(), Ok(()))],
        vec![
            ("alpha".to_owned(), Ok(())),
            ("beta".to_owned(), Ok(())),
            ("gamma".to_owned(), Ok(())),
        ],
        vec![("alpha".to_owned(), Ok(())), ("alpha".to_owned(), Ok(()))],
        vec![("alpha".to_owned(), Ok(())), ("gamma".to_owned(), Ok(()))],
    ];

    for entries in malformed {
        let result = ordered_group_results(
            entries,
            &requested,
            &operation_id,
            "classic-group description",
        );
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

fn partition(
    topic: &str,
    partition: i32,
    result: Result<Option<i64>, KafkaError>,
) -> (TopicPartition, Result<Option<i64>, KafkaError>) {
    (TopicPartition::new(topic, partition), result)
}

fn requested_partitions() -> Vec<(String, i32)> {
    vec![("orders".to_owned(), 0), ("orders".to_owned(), 2)]
}

fn operation_id() -> OperationId {
    OperationId::new("admin-plural-result-1")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
