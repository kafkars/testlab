//! Record-deletion provisioning derives independent per-partition seed targets.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::ScenarioAction;

use crate::compose_provision_targets::{SeedTarget, require_topic};

pub(super) fn seed_targets(action: &ScenarioAction) -> Vec<SeedTarget> {
    match action {
        ScenarioAction::DeleteRecords(action) => vec![SeedTarget {
            topic: action.topic.clone(),
            partition: action.partition,
            record_count: action.expected_high_watermark,
        }],
        ScenarioAction::DeleteRecordsBatch(action) => action
            .targets
            .iter()
            .map(|target| SeedTarget {
                topic: target.topic.clone(),
                partition: target.partition,
                record_count: target.expected_high_watermark,
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn record_topics(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    action: &ScenarioAction,
) -> bool {
    let targets = seed_targets(action);
    if targets.is_empty() {
        return false;
    }
    for target in targets {
        require_topic(
            topics,
            subject_created,
            &target.topic,
            target.partition.saturating_add(1),
        );
    }
    true
}
