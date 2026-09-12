//! Topic-configuration normalization tests reject missing or ambiguous public results.

use testlab_schema::{AdminConfigEntryMetadata, OperationId};

use super::protocol_admin_config_entry::{SelectedConfig, described_value};

fn operation() -> OperationId {
    OperationId::new("config-op").unwrap_or_else(|error| panic!("operation ID: {error}"))
}

#[test]
fn selected_topic_configuration_preserves_nullable_value() {
    let value = described_value(
        vec![(
            "orders".to_owned(),
            Ok(vec![selected("cleanup.policy", "compact")]),
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
            Ok(vec![selected("retention.ms", "1000")]),
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
                selected("cleanup.policy", "delete"),
                selected("retention.ms", "1000"),
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

fn selected(name: &str, value: &str) -> SelectedConfig {
    SelectedConfig {
        name: name.to_owned(),
        value: Some(value.to_owned()),
        metadata: AdminConfigEntryMetadata {
            read_only: false,
            source: 5,
            sensitive: false,
            synonyms: Vec::new(),
            config_type: None,
            documentation: None,
        },
    }
}
