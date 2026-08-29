//! Topic-configuration normalization tests reject missing or ambiguous public results.

use testlab_schema::OperationId;

use super::protocol_admin_config::described_value;

fn operation() -> OperationId {
    OperationId::new("config-op").unwrap_or_else(|error| panic!("operation ID: {error}"))
}

#[test]
fn selected_topic_configuration_preserves_nullable_value() {
    let value = described_value(
        vec![(
            "orders".to_owned(),
            Ok(vec![(
                "cleanup.policy".to_owned(),
                Some("compact".to_owned()),
            )]),
        )],
        &operation(),
        "orders",
        "cleanup.policy",
    )
    .unwrap_or_else(|error| panic!("valid result: {error}"));
    assert_eq!(value.as_deref(), Some("compact"));
}

#[test]
fn selected_topic_configuration_rejects_wrong_key() {
    let Err(error) = described_value(
        vec![(
            "orders".to_owned(),
            Ok(vec![("retention.ms".to_owned(), Some("1000".to_owned()))]),
        )],
        &operation(),
        "orders",
        "cleanup.policy",
    ) else {
        panic!("wrong key must fail");
    };
    assert!(
        error
            .to_string()
            .contains("unexpected selected configuration")
    );
}

#[test]
fn selected_topic_configuration_rejects_extra_entries() {
    let Err(error) = described_value(
        vec![(
            "orders".to_owned(),
            Ok(vec![
                ("cleanup.policy".to_owned(), Some("delete".to_owned())),
                ("retention.ms".to_owned(), Some("1000".to_owned())),
            ]),
        )],
        &operation(),
        "orders",
        "cleanup.policy",
    ) else {
        panic!("extra entries must fail");
    };
    assert!(
        error
            .to_string()
            .contains("unexpected selected configuration")
    );
}
