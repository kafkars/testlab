//! Batch offset result tests preserve public success and resource-failure order.

use crate::protocol_admin_list_offsets_batch::outcomes;
use crate::protocol_admin_plural_result::{PartitionResult, ResourceResult};

#[test]
fn outcome_normalization_preserves_success_and_resource_failure_order() {
    let outcomes = outcomes(vec![
        PartitionResult {
            topic: "records".to_owned(),
            partition: 2,
            result: ResourceResult::Success(Some(5)),
        },
        PartitionResult {
            topic: "records".to_owned(),
            partition: 0,
            result: ResourceResult::Failure("broker:broker_3".to_owned()),
        },
    ]);

    assert_eq!(outcomes[0].offset, Some(5));
    assert_eq!(outcomes[0].error_code, None);
    assert_eq!(outcomes[1].offset, None);
    assert_eq!(outcomes[1].error_code.as_deref(), Some("broker:broker_3"));
}
