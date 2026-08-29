//! Provisioning targets derive only broker state that the packaged client does not create.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{RecordSpec, Scenario, ScenarioAction};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct SeedTarget {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) record_count: i64,
}

pub(super) fn topics(scenario: &Scenario) -> BTreeMap<String, i32> {
    let mut subject_created = BTreeSet::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateTopic(action) => {
                subject_created.insert(action.topic.clone());
            }
            ScenarioAction::CreateTopicsBatch(action) => {
                subject_created.extend(action.topics.iter().map(|item| item.topic.clone()));
            }
            _ => {}
        }
    }
    let mut topics = BTreeMap::new();
    for step in &scenario.steps {
        record_targets(&mut topics, &subject_created, &step.action);
        admin_targets(&mut topics, &subject_created, &step.action);
    }
    topics
}

pub(super) fn share_groups(scenario: &Scenario) -> BTreeSet<String> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::CreateShareConsumer { group_id, .. } => Some(group_id.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn seed_targets(scenario: &Scenario) -> BTreeSet<SeedTarget> {
    scenario
        .steps
        .iter()
        .filter_map(|step| match &step.action {
            ScenarioAction::DeleteRecords(action) => Some(SeedTarget {
                topic: action.topic.clone(),
                partition: action.partition,
                record_count: action.expected_high_watermark,
            }),
            _ => None,
        })
        .collect()
}

fn record_targets(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    action: &ScenarioAction,
) {
    match action {
        ScenarioAction::Send { record, .. } => record_topic(topics, subject_created, record),
        ScenarioAction::SendBatch { operations, .. }
        | ScenarioAction::ExecuteTransaction { operations, .. } => {
            for operation in operations {
                record_topic(topics, subject_created, &operation.record);
            }
        }
        ScenarioAction::FenceTransaction { operation, .. } => {
            record_topic(topics, subject_created, &operation.record);
        }
        ScenarioAction::StartConcurrentActors(action) => {
            for actor in &action.actors {
                if let testlab_schema::ConcurrentActor::ProducerSend { record, .. } = actor {
                    record_topic(topics, subject_created, record);
                }
            }
        }
        _ => {}
    }
}

fn admin_targets(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    action: &ScenarioAction,
) {
    if plural_group_admin_targets(topics, subject_created, action) {
        return;
    }
    match action {
        ScenarioAction::CreatePartitions(action) if action.validate_only => {
            if let Some(expected_current_count) = action.expected_current_count {
                require_topic(
                    topics,
                    subject_created,
                    &action.topic,
                    expected_current_count,
                );
            }
        }
        ScenarioAction::CreatePartitions(action) if action.expected_error_code.is_none() => {
            require_topic(
                topics,
                subject_created,
                &action.topic,
                action.total_count.saturating_sub(1).max(1),
            );
        }
        ScenarioAction::DeleteTopic(action) if action.expected_error_code.is_none() => {
            require_topic(topics, subject_created, &action.topic, 1);
        }
        ScenarioAction::DescribeTopic(action) if action.expected_error_code.is_none() => {
            let partitions = action
                .expected_partitions
                .as_deref()
                .and_then(|values| values.last())
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
            require_topic(topics, subject_created, &action.topic, partitions);
        }
        ScenarioAction::ListTopics(action) => {
            for topic in &action.required_topics {
                require_topic(topics, subject_created, topic, 1);
            }
        }
        ScenarioAction::ListOffsets(action) => {
            let partitions = if action.expected_error_code.is_some() {
                action.partition
            } else {
                action.partition.saturating_add(1)
            };
            require_topic(topics, subject_created, &action.topic, partitions);
        }
        ScenarioAction::DeleteRecords(action) => require_topic(
            topics,
            subject_created,
            &action.topic,
            action.partition.saturating_add(1),
        ),
        ScenarioAction::DescribeTopicConfig(action) => {
            require_topic(topics, subject_created, &action.topic, 1);
        }
        ScenarioAction::AlterTopicConfig(action) => {
            require_topic(topics, subject_created, &action.topic, 1);
        }
        ScenarioAction::ListConsumerGroupOffsets(action) => require_topic(
            topics,
            subject_created,
            &action.topic,
            action.partition.saturating_add(1),
        ),
        ScenarioAction::AlterConsumerGroupOffset(action) => require_topic(
            topics,
            subject_created,
            &action.topic,
            action.partition.saturating_add(1),
        ),
        ScenarioAction::DeleteConsumerGroupOffset(action) => require_topic(
            topics,
            subject_created,
            &action.topic,
            action.partition.saturating_add(1),
        ),
        _ => {}
    }
}

fn plural_group_admin_targets(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    action: &ScenarioAction,
) -> bool {
    match action {
        ScenarioAction::ListConsumerGroupOffsetsBatch(action) => require_offset_topics(
            topics,
            subject_created,
            action
                .partitions
                .iter()
                .map(|item| (item.topic.as_str(), item.partition)),
        ),
        ScenarioAction::ListConsumerGroupsOffsets(action) => require_offset_topics(
            topics,
            subject_created,
            action.groups.iter().flat_map(|group| {
                group
                    .partitions
                    .iter()
                    .map(|item| (item.topic.as_str(), item.partition))
            }),
        ),
        ScenarioAction::AlterConsumerGroupOffsets(action) => require_offset_topics(
            topics,
            subject_created,
            action
                .offsets
                .iter()
                .map(|item| (item.topic.as_str(), item.partition)),
        ),
        ScenarioAction::DeleteConsumerGroupOffsets(action) => require_offset_topics(
            topics,
            subject_created,
            action
                .partitions
                .iter()
                .map(|item| (item.topic.as_str(), item.partition)),
        ),
        _ => return false,
    }
    true
}

fn record_topic(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    record: &RecordSpec,
) {
    require_topic(
        topics,
        subject_created,
        &record.topic,
        record.partition.saturating_add(1),
    );
}

fn require_topic(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    topic: &str,
    partitions: i32,
) {
    if subject_created.contains(topic) {
        return;
    }
    topics
        .entry(topic.to_owned())
        .and_modify(|current| *current = (*current).max(partitions))
        .or_insert(partitions);
}

fn require_offset_topics<'a>(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    offsets: impl IntoIterator<Item = (&'a str, i32)>,
) {
    for (topic, partition) in offsets {
        require_topic(topics, subject_created, topic, partition.saturating_add(1));
    }
}
