//! Static-member removal normalization retains caller identities and public failures.

use testlab_schema::OperationId;

use crate::AdapterError;
use crate::kafkars_api::{ErrorKind, KafkaError};
use crate::protocol_admin_consumer_group_member_removal::outcomes;

#[test]
fn outcomes_preserve_caller_order_and_errors() {
    let values = outcomes(
        vec![
            ("static-zulu".to_owned(), Ok(())),
            (
                "static-alpha".to_owned(),
                Err(KafkaError::new(ErrorKind::Broker, "member removal failed")),
            ),
        ],
        &identities(),
        &operation(),
    )
    .unwrap_or_else(|error| panic!("normalize static-member removal: {error}"));
    assert_eq!(values[0].group_instance_id, "static-zulu");
    assert_eq!(values[0].error_code, None);
    assert_eq!(values[1].group_instance_id, "static-alpha");
    assert_eq!(values[1].error_code.as_deref(), Some("broker"));
}

#[test]
fn missing_extra_or_reordered_outcomes_are_invalid() {
    for entries in [
        vec![("static-zulu".to_owned(), Ok(()))],
        vec![
            ("static-zulu".to_owned(), Ok(())),
            ("static-alpha".to_owned(), Ok(())),
            ("static-extra".to_owned(), Ok(())),
        ],
        vec![
            ("static-alpha".to_owned(), Ok(())),
            ("static-zulu".to_owned(), Ok(())),
        ],
    ] {
        let result = outcomes(entries, &identities(), &operation());
        assert!(matches!(result, Err(AdapterError::AdminResult(_))));
    }
}

fn identities() -> Vec<String> {
    vec!["static-zulu".to_owned(), "static-alpha".to_owned()]
}

fn operation() -> OperationId {
    OperationId::new("admin-remove-static-members")
        .unwrap_or_else(|error| panic!("operation id: {error}"))
}
