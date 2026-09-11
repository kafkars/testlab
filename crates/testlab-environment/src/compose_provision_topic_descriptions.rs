//! Plural topic descriptions and deletions provision only successful resources.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::ScenarioAction;

pub(super) fn record(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    action: &ScenarioAction,
) {
    match action {
        ScenarioAction::DescribeTopics(action) => {
            for topic in &action.topics {
                let Some(partitions) = topic.expected_partitions.as_deref() else {
                    continue;
                };
                let count = partitions.last().copied().unwrap_or(0).saturating_add(1);
                crate::compose_provision_targets::require_topic(
                    topics,
                    subject_created,
                    &topic.topic,
                    count,
                );
            }
        }
        ScenarioAction::DeleteTopics(action) => {
            for topic in &action.topics {
                if topic.expected_error_code.is_none() {
                    crate::compose_provision_targets::require_topic(
                        topics,
                        subject_created,
                        &topic.topic,
                        1,
                    );
                }
            }
        }
        _ => {}
    }
}
