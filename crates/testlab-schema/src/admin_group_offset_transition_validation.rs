//! Group-offset mutations require exact previously listed modeled baselines.

use std::collections::BTreeMap;

use crate::{OperationId, Scenario, ScenarioAction};

type OffsetKey = (String, String, i32);

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut offsets = BTreeMap::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::ListConsumerGroupOffsets(action) => {
                offsets.insert(singleton_key(action), action.expected_offset);
            }
            ScenarioAction::ListConsumerGroupOffsetsBatch(action) => {
                record_expectations(&mut offsets, &action.group_id, &action.partitions);
            }
            ScenarioAction::ListConsumerGroupsOffsets(action) => {
                for group in &action.groups {
                    record_expectations(&mut offsets, &group.group_id, &group.partitions);
                }
            }
            ScenarioAction::AlterConsumerGroupOffset(action) => {
                alter(
                    &mut offsets,
                    &action.operation_id,
                    singleton_key(action),
                    action.offset,
                    problems,
                );
            }
            ScenarioAction::AlterConsumerGroupOffsets(action) => {
                for offset in &action.offsets {
                    alter(
                        &mut offsets,
                        &action.operation_id,
                        key(&action.group_id, &offset.topic, offset.partition),
                        offset.offset,
                        problems,
                    );
                }
            }
            ScenarioAction::DeleteConsumerGroupOffset(action) => {
                delete(
                    &mut offsets,
                    &action.operation_id,
                    &singleton_key(action),
                    problems,
                );
            }
            ScenarioAction::DeleteConsumerGroupOffsets(action) => {
                for selection in &action.partitions {
                    delete(
                        &mut offsets,
                        &action.operation_id,
                        &key(&action.group_id, &selection.topic, selection.partition),
                        problems,
                    );
                }
            }
            _ => {}
        }
    }
}

fn record_expectations(
    offsets: &mut BTreeMap<OffsetKey, i64>,
    group_id: &str,
    expectations: &[crate::ConsumerGroupOffsetExpectation],
) {
    for expectation in expectations {
        offsets.insert(
            key(group_id, &expectation.topic, expectation.partition),
            expectation.expected_offset,
        );
    }
}

fn alter(
    offsets: &mut BTreeMap<OffsetKey, i64>,
    operation_id: &OperationId,
    key: OffsetKey,
    requested: i64,
    problems: &mut Vec<String>,
) {
    let valid = match offsets.get(&key) {
        Some(actual) if *actual != requested => true,
        Some(_) => {
            problems.push(format!(
                "admin operation {operation_id} requires a prior different committed offset for {}:{}:{}",
                key.0, key.1, key.2
            ));
            false
        }
        None => {
            problems.push(format!(
                "admin operation {operation_id} requires a prior committed-offset listing for {}:{}:{}",
                key.0, key.1, key.2
            ));
            false
        }
    };
    if valid {
        offsets.insert(key, requested);
    }
}

fn delete(
    offsets: &mut BTreeMap<OffsetKey, i64>,
    operation_id: &OperationId,
    key: &OffsetKey,
    problems: &mut Vec<String>,
) {
    if offsets.remove(key).is_none() {
        problems.push(format!(
            "admin operation {operation_id} requires a prior committed-offset listing for {}:{}:{}",
            key.0, key.1, key.2
        ));
    }
}

trait SingletonOffset {
    fn group_id(&self) -> &str;
    fn topic(&self) -> &str;
    fn partition(&self) -> i32;
}

impl SingletonOffset for crate::ListConsumerGroupOffsetsAction {
    fn group_id(&self) -> &str {
        &self.group_id
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn partition(&self) -> i32 {
        self.partition
    }
}

impl SingletonOffset for crate::AlterConsumerGroupOffsetAction {
    fn group_id(&self) -> &str {
        &self.group_id
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn partition(&self) -> i32 {
        self.partition
    }
}

impl SingletonOffset for crate::DeleteConsumerGroupOffsetAction {
    fn group_id(&self) -> &str {
        &self.group_id
    }

    fn topic(&self) -> &str {
        &self.topic
    }

    fn partition(&self) -> i32 {
        self.partition
    }
}

fn singleton_key(action: &impl SingletonOffset) -> OffsetKey {
    key(action.group_id(), action.topic(), action.partition())
}

fn key(group_id: &str, topic: &str, partition: i32) -> OffsetKey {
    (group_id.to_owned(), topic.to_owned(), partition)
}
