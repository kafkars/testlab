//! Batch admin protocol matching checks identity without deciding outcome truth.

use testlab_schema::{
    AdapterEvent, AdminOffsetListingOutcome, AdminOffsetsListing, AdminTopicCreationOutcome,
    AdminTopicsCreationBatch, OperationId,
};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn batch_completion_checks_only_the_stable_operation_identity() {
    let operation_id =
        OperationId::new("batch-create").unwrap_or_else(|error| panic!("operation: {error}"));
    let expected = ExpectedEvent::TopicsCreationCompleted {
        operation_id: operation_id.clone(),
    };
    let event = AdapterEvent::TopicsCreationCompleted(AdminTopicsCreationBatch {
        operation_id,
        outcomes: vec![AdminTopicCreationOutcome {
            topic: "unexpected-here".to_owned(),
            error_code: Some("ANY_SEMANTIC_RESULT".to_owned()),
        }],
    });

    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classification: {error}")),
        EventDisposition::Complete
    );
}

#[test]
fn offset_batch_completion_checks_only_the_stable_operation_identity() {
    let operation_id =
        OperationId::new("batch-offsets").unwrap_or_else(|error| panic!("operation: {error}"));
    let expected = ExpectedEvent::OffsetsListed {
        operation_id: operation_id.clone(),
    };
    let event = AdapterEvent::OffsetsListed(AdminOffsetsListing {
        operation_id,
        outcomes: vec![AdminOffsetListingOutcome {
            topic: "semantic-result".to_owned(),
            partition: 9,
            offset: None,
            error_code: Some("ANY_SEMANTIC_RESULT".to_owned()),
        }],
    });

    assert_eq!(
        expected
            .classify(&event)
            .unwrap_or_else(|error| panic!("classification: {error}")),
        EventDisposition::Complete
    );
}
