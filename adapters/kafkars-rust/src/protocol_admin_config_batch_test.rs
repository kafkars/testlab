//! Plural topic-configuration normalization rejects lossy or reordered results.

use testlab_schema::{AdminConfigEntryMetadata, OperationId, TopicConfigSelection};

use super::protocol_admin_config_entry::{SelectedConfig, described_outcomes};

#[test]
fn exact_selected_values_remain_in_caller_order() {
    let outcomes = described_outcomes(
        vec![
            result("topic-z", "cleanup.policy", "delete"),
            result("topic-a", "retention.ms", "604800000"),
        ],
        &selections(),
        &operation(),
    )
    .unwrap_or_else(|error| panic!("valid plural config result: {error}"));
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0].topic, "topic-z");
    assert_eq!(outcomes[1].config_name, "retention.ms");
    assert_eq!(outcomes[1].value.as_deref(), Some("604800000"));
    let metadata = outcomes[1]
        .metadata
        .as_ref()
        .unwrap_or_else(|| panic!("selected configuration metadata"));
    assert_eq!(metadata.source, 5);
    assert_eq!(metadata.config_type, Some(7));
    assert_eq!(
        metadata.documentation.as_deref(),
        Some("configuration documentation")
    );
}

#[test]
fn reordered_topics_and_unselected_keys_are_rejected() {
    let reordered = vec![
        result("topic-a", "retention.ms", "604800000"),
        result("topic-z", "cleanup.policy", "delete"),
    ];
    let wrong_key = vec![
        result("topic-z", "retention.ms", "1000"),
        result("topic-a", "retention.ms", "604800000"),
    ];
    for entries in [reordered, wrong_key] {
        assert!(described_outcomes(entries, &selections(), &operation()).is_err());
    }
}

fn result(
    topic: &str,
    config_name: &str,
    value: &str,
) -> (
    String,
    Result<Vec<SelectedConfig>, crate::kafkars_api::KafkaError>,
) {
    (topic.to_owned(), Ok(vec![selected(config_name, value)]))
}

fn selected(config_name: &str, value: &str) -> SelectedConfig {
    SelectedConfig {
        name: config_name.to_owned(),
        value: Some(value.to_owned()),
        metadata: AdminConfigEntryMetadata {
            read_only: false,
            source: 5,
            sensitive: false,
            synonyms: Vec::new(),
            config_type: Some(7),
            documentation: Some("configuration documentation".to_owned()),
        },
    }
}

fn selections() -> Vec<TopicConfigSelection> {
    vec![
        TopicConfigSelection {
            topic: "topic-z".to_owned(),
            config_name: "cleanup.policy".to_owned(),
        },
        TopicConfigSelection {
            topic: "topic-a".to_owned(),
            config_name: "retention.ms".to_owned(),
        },
    ]
}

fn operation() -> OperationId {
    OperationId::new("describe-topic-configs").unwrap_or_else(|error| panic!("operation: {error}"))
}
