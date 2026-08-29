//! Delete-records transitions require an ordered independently checkable baseline.

use std::collections::{BTreeMap, BTreeSet};

use crate::{AdminOffsetPosition, Scenario, ScenarioAction};

#[derive(Clone, Copy)]
enum Baseline {
    EarliestZero,
    Complete(i64),
}

pub(crate) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    validate_fresh_targets(scenario, problems);
    let mut baselines = BTreeMap::<(String, i32), Baseline>::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::ListOffsets(action) if action.expected_error_code.is_none() => {
                let Some(expected_offset) = action.expected_offset else {
                    continue;
                };
                let key = (action.topic.clone(), action.partition);
                match action.position {
                    AdminOffsetPosition::Earliest if expected_offset == 0 => {
                        baselines.entry(key).or_insert(Baseline::EarliestZero);
                    }
                    AdminOffsetPosition::Earliest => {
                        baselines.remove(&key);
                    }
                    AdminOffsetPosition::Latest => {
                        if let Some(baseline) = baselines.get_mut(&key) {
                            *baseline = Baseline::Complete(expected_offset);
                        }
                    }
                }
            }
            ScenarioAction::DeleteRecords(action) => {
                let key = (action.topic.clone(), action.partition);
                let matches = matches!(
                    baselines.remove(&key),
                    Some(Baseline::Complete(value)) if value == action.expected_high_watermark
                );
                if !matches {
                    problems.push(format!(
                        "admin operation {} requires same-target earliest offset 0 followed by latest offset {}",
                        action.operation_id, action.expected_high_watermark
                    ));
                }
            }
            _ => {}
        }
    }
}

fn validate_fresh_targets(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut targets = BTreeSet::new();
    for step in &scenario.steps {
        let ScenarioAction::DeleteRecords(action) = &step.action else {
            continue;
        };
        let target = (action.topic.as_str(), action.partition);
        if !targets.insert(target) {
            problems.push(format!(
                "admin operation {} repeats a delete-records target",
                action.operation_id
            ));
        }
        if scenario.steps.iter().any(|step| {
            matches!(&step.action, ScenarioAction::CreateTopic(value) if value.topic == action.topic)
        }) {
            problems.push(format!(
                "admin operation {} requires a harness-owned topic",
                action.operation_id
            ));
        }
        if scenario
            .steps
            .iter()
            .any(|step| writes_target(&step.action, target))
        {
            problems.push(format!(
                "admin operation {} requires a partition without scenario record writes",
                action.operation_id
            ));
        }
    }
}

fn writes_target(action: &ScenarioAction, target: (&str, i32)) -> bool {
    match action {
        ScenarioAction::Send { record, .. } => record_matches(record, target),
        ScenarioAction::SendBatch { operations, .. }
        | ScenarioAction::ExecuteTransaction { operations, .. } => operations
            .iter()
            .any(|operation| record_matches(&operation.record, target)),
        ScenarioAction::FenceTransaction { operation, .. } => {
            record_matches(&operation.record, target)
        }
        _ => false,
    }
}

fn record_matches(record: &crate::RecordSpec, target: (&str, i32)) -> bool {
    record.topic == target.0 && record.partition == target.1
}
