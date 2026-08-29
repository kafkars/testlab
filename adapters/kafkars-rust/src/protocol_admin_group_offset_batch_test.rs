//! Plural admin event tests keep absence, partial failure, and membership distinct.

use testlab_schema::OperationId;

use crate::protocol_admin_classic_group::description_outcomes;
use crate::protocol_admin_group_offset_batch::listing_outcomes;
use crate::protocol_admin_group_offset_batch_mutation::mutation_outcomes;
use crate::protocol_admin_plural_result::{GroupResult, PartitionResult, ResourceResult};

#[test]
fn listing_event_outcomes_distinguish_absence_from_partition_failure() {
    let outcomes = listing_outcomes(vec![
        partition_result(0, ResourceResult::Success(Some(18))),
        partition_result(1, ResourceResult::Success(None)),
        partition_result(2, ResourceResult::Failure("broker:broker_3".to_owned())),
    ]);

    assert_eq!(outcomes[0].offset, Some(18));
    assert_eq!(outcomes[0].error_code, None);
    assert_eq!(outcomes[1].offset, None);
    assert_eq!(outcomes[1].error_code, None);
    assert_eq!(outcomes[2].offset, None);
    assert_eq!(outcomes[2].error_code.as_deref(), Some("broker:broker_3"));
}

#[test]
fn mutation_event_outcomes_retain_each_partition_error() {
    let outcomes = mutation_outcomes(vec![
        partition_result(0, ResourceResult::Success(())),
        partition_result(2, ResourceResult::Failure("broker:broker_25".to_owned())),
    ]);

    assert_eq!(outcomes[0].partition, 0);
    assert_eq!(outcomes[0].error_code, None);
    assert_eq!(outcomes[1].partition, 2);
    assert_eq!(outcomes[1].error_code.as_deref(), Some("broker:broker_25"));
}

#[test]
fn classic_group_event_outcomes_retain_success_and_group_failure() {
    let outcomes = description_outcomes(
        vec![
            GroupResult {
                group_id: "active".to_owned(),
                result: ResourceResult::Success(2),
            },
            GroupResult {
                group_id: "missing".to_owned(),
                result: ResourceResult::Failure("broker:broker_69".to_owned()),
            },
        ],
        &operation_id(),
    )
    .unwrap_or_else(|error| panic!("describe outcomes: {error}"));

    assert_eq!(outcomes[0].group_id, "active");
    assert_eq!(outcomes[0].member_count, Some(2));
    assert_eq!(outcomes[0].error_code, None);
    assert_eq!(outcomes[1].group_id, "missing");
    assert_eq!(outcomes[1].member_count, None);
    assert_eq!(outcomes[1].error_code.as_deref(), Some("broker:broker_69"));
}

fn partition_result<T>(partition: i32, result: ResourceResult<T>) -> PartitionResult<T> {
    PartitionResult {
        topic: "orders".to_owned(),
        partition,
        result,
    }
}

fn operation_id() -> OperationId {
    OperationId::new("admin-classic-groups-1")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
