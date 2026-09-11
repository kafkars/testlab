//! Share-group offset mutations require an empty modeled group and a listed baseline.

use std::collections::BTreeMap;

use crate::{ConsumerId, Scenario, ScenarioAction};

type OffsetKey = (String, String, i32);

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut consumers = BTreeMap::<ConsumerId, (String, bool)>::new();
    let mut offsets = BTreeMap::<OffsetKey, i64>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateShareConsumer {
                consumer_id,
                group_id,
                ..
            } => {
                consumers.insert(consumer_id.clone(), (group_id.clone(), false));
            }
            ScenarioAction::CloseShareConsumer { consumer_id, .. } => {
                if let Some((_, closed)) = consumers.get_mut(consumer_id) {
                    *closed = true;
                }
            }
            ScenarioAction::ListShareGroupOffsets(action) => {
                offsets.insert(
                    key(&action.group_id, &action.topic, action.partition),
                    action.expected_start_offset,
                );
            }
            ScenarioAction::AlterShareGroupOffsets(action) => {
                validate_empty_group(action, &consumers, problems);
                let key = key(&action.group_id, &action.topic, action.partition);
                match offsets.get(&key) {
                    Some(previous) if *previous != action.start_offset => {
                        offsets.insert(key, action.start_offset);
                    }
                    Some(_) => problems.push(format!(
                        "admin operation {} requires a prior different Share-group start offset for {}:{}:{}",
                        action.operation_id, action.group_id, action.topic, action.partition
                    )),
                    None => problems.push(format!(
                        "admin operation {} requires a prior Share-group offset listing for {}:{}:{}",
                        action.operation_id, action.group_id, action.topic, action.partition
                    )),
                }
            }
            _ => {}
        }
    }
}

fn validate_empty_group(
    action: &crate::AlterShareGroupOffsetsAction,
    consumers: &BTreeMap<ConsumerId, (String, bool)>,
    problems: &mut Vec<String>,
) {
    let matching = consumers
        .values()
        .filter(|(group_id, _)| group_id == &action.group_id)
        .collect::<Vec<_>>();
    if matching.is_empty() {
        problems.push(format!(
            "admin operation {} requires a prior modeled Share-group member",
            action.operation_id
        ));
    } else if matching.iter().any(|(_, closed)| !closed) {
        problems.push(format!(
            "admin operation {} requires every modeled Share-group member to be closed",
            action.operation_id
        ));
    }
}

fn key(group_id: &str, topic: &str, partition: i32) -> OffsetKey {
    (group_id.to_owned(), topic.to_owned(), partition)
}
