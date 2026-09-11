//! Consumer-group deletion normalization retains order and exact public failures.

use testlab_schema::OperationId;

use crate::AdapterError;
use crate::kafkars_api::{ErrorKind, KafkaError};
use crate::protocol_admin_consumer_group_deletion_batch::outcomes;

#[test]
fn outcomes_preserve_caller_order_and_errors() {
    let values = outcomes(
        vec![
            ("group-z".to_owned(), Ok(())),
            (
                "group-a".to_owned(),
                Err(KafkaError::new(ErrorKind::Broker, "group deletion failed")),
            ),
        ],
        &group_ids(),
        &operation(),
    )
    .unwrap_or_else(|error| panic!("normalize group deletion: {error}"));
    assert_eq!(values[0].group_id, "group-z");
    assert_eq!(values[0].error_code, None);
    assert_eq!(values[1].group_id, "group-a");
    assert_eq!(values[1].error_code.as_deref(), Some("broker"));
}

#[test]
fn missing_extra_or_reordered_outcomes_are_invalid() {
    for entries in [
        vec![("group-z".to_owned(), Ok(()))],
        vec![
            ("group-z".to_owned(), Ok(())),
            ("group-a".to_owned(), Ok(())),
            ("group-extra".to_owned(), Ok(())),
        ],
        vec![
            ("group-a".to_owned(), Ok(())),
            ("group-z".to_owned(), Ok(())),
        ],
    ] {
        let result = outcomes(entries, &group_ids(), &operation());
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

fn group_ids() -> Vec<String> {
    vec!["group-z".to_owned(), "group-a".to_owned()]
}

fn operation() -> OperationId {
    OperationId::new("admin-delete-consumer-groups")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
