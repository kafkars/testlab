//! Batched record-deletion normalization pins sentinel mapping and public order.

use crate::kafkars_api::{ErrorKind, KafkaError, TopicPartition};
use testlab_schema::{DeleteRecordsBatchSelection, DeleteRecordsBoundary, OperationId};

use crate::AdapterError;
use crate::protocol_admin_delete_records_batch::{outcomes, public_targets};

#[test]
fn public_targets_preserve_caller_order_and_high_watermark_sentinel() {
    let targets = selections();
    let public = public_targets(&targets);
    assert_eq!(public.len(), 2);
    assert_eq!((public[0].topic(), public[0].partition()), ("records", 1));
    assert_eq!(public[0].deletion_offset(), 2);
    assert_eq!((public[1].topic(), public[1].partition()), ("records", 0));
    assert_eq!(public[1].deletion_offset(), -1);
}

#[test]
fn outcomes_preserve_success_and_per_target_failure() {
    let targets = selections();
    let actual = outcomes(
        vec![
            entry("records", 1, Ok(2)),
            entry(
                "records",
                0,
                Err(KafkaError::new(ErrorKind::Broker, "delete failed")),
            ),
        ],
        &targets,
        &operation(),
    )
    .unwrap_or_else(|error| panic!("batched deletion outcomes: {error}"));
    assert_eq!(actual[0].low_watermark, Some(2));
    assert_eq!(actual[0].error_code, None);
    assert_eq!(actual[1].low_watermark, None);
    assert!(actual[1].error_code.is_some());
}

#[test]
fn outcomes_reject_missing_reordered_and_positioned_identities() {
    let targets = selections();
    let missing = outcomes(vec![entry("records", 1, Ok(2))], &targets, &operation());
    let reordered = outcomes(
        vec![entry("records", 0, Ok(2)), entry("records", 1, Ok(2))],
        &targets,
        &operation(),
    );
    let positioned = outcomes(
        vec![
            (
                TopicPartition::new("records", 1)
                    .start_at(crate::kafkars_api::StartPosition::Beginning),
                Ok(2),
            ),
            entry("records", 0, Ok(2)),
        ],
        &targets,
        &operation(),
    );
    for result in [missing, reordered, positioned] {
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

fn selections() -> Vec<DeleteRecordsBatchSelection> {
    vec![
        selection(1, DeleteRecordsBoundary::Offset { offset: 2 }),
        selection(0, DeleteRecordsBoundary::HighWatermark),
    ]
}

fn selection(partition: i32, boundary: DeleteRecordsBoundary) -> DeleteRecordsBatchSelection {
    DeleteRecordsBatchSelection {
        topic: "records".to_owned(),
        partition,
        boundary,
    }
}

fn entry(
    topic: &str,
    partition: i32,
    result: Result<i64, KafkaError>,
) -> (TopicPartition, Result<i64, KafkaError>) {
    (TopicPartition::new(topic, partition), result)
}

fn operation() -> OperationId {
    OperationId::new("delete-records-batch").unwrap_or_else(|error| panic!("operation: {error}"))
}
