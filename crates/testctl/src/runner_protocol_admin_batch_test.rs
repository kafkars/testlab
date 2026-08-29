//! Batch admin protocol matching checks identity without deciding outcome truth.

use testlab_schema::{
    AdapterEvent, AdminTopicCreationOutcome, AdminTopicsCreationBatch, OperationId,
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
