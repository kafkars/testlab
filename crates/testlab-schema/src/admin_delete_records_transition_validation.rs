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
                record_baseline(
                    &mut baselines,
                    &action.topic,
                    action.partition,
                    action.position,
                    expected_offset,
                );
            }
            ScenarioAction::ListOffsetsBatch(action) => {
                for query in &action.queries {
                    record_baseline(
                        &mut baselines,
                        &query.topic,
                        query.partition,
                        query.position,
                        query.expected_offset,
                    );
                }
            }
            ScenarioAction::DeleteRecords(action) => {
                require_baseline(
                    &mut baselines,
                    &action.operation_id,
                    &action.topic,
                    action.partition,
                    action.expected_high_watermark,
                    problems,
                );
            }
            ScenarioAction::DeleteRecordsBatch(action) => {
                for target in &action.targets {
                    require_baseline(
                        &mut baselines,
                        &action.operation_id,
                        &target.topic,
                        target.partition,
                        target.expected_high_watermark,
                        problems,
                    );
                }
            }
            _ => {}
        }
    }
}

fn record_baseline(
    baselines: &mut BTreeMap<(String, i32), Baseline>,
    topic: &str,
    partition: i32,
    position: AdminOffsetPosition,
    expected_offset: i64,
) {
    let key = (topic.to_owned(), partition);
    match position {
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

fn require_baseline(
    baselines: &mut BTreeMap<(String, i32), Baseline>,
    operation_id: &crate::OperationId,
    topic: &str,
    partition: i32,
    expected_high_watermark: i64,
    problems: &mut Vec<String>,
) {
    let matches = matches!(
        baselines.remove(&(topic.to_owned(), partition)),
        Some(Baseline::Complete(value)) if value == expected_high_watermark
    );
    if !matches {
        problems.push(format!(
            "admin operation {operation_id} requires same-target earliest offset 0 followed by latest offset {expected_high_watermark}"
        ));
    }
}

fn validate_fresh_targets(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut targets = BTreeSet::new();
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::DeleteRecords(action) => validate_fresh_target(
                scenario,
                &action.operation_id,
                &action.topic,
                action.partition,
                &mut targets,
                problems,
            ),
            ScenarioAction::DeleteRecordsBatch(action) => {
                for target in &action.targets {
                    validate_fresh_target(
                        scenario,
                        &action.operation_id,
                        &target.topic,
                        target.partition,
                        &mut targets,
                        problems,
                    );
                }
            }
            _ => {}
        }
    }
}

fn validate_fresh_target(
    scenario: &Scenario,
    operation_id: &crate::OperationId,
    topic: &str,
    partition: i32,
    targets: &mut BTreeSet<(String, i32)>,
    problems: &mut Vec<String>,
) {
    if !targets.insert((topic.to_owned(), partition)) {
        problems.push(format!(
            "admin operation {operation_id} repeats a delete-records target"
        ));
    }
    if scenario.steps.iter().any(|step| match &step.action {
        ScenarioAction::CreateTopic(value) => value.topic == topic,
        ScenarioAction::CreateTopicsBatch(value) => value
            .topics
            .iter()
            .any(|candidate| candidate.topic == topic),
        _ => false,
    }) {
        problems.push(format!(
            "admin operation {operation_id} requires a harness-owned topic"
        ));
    }
    if scenario
        .steps
        .iter()
        .any(|step| writes_target(&step.action, (topic, partition)))
    {
        problems.push(format!(
            "admin operation {operation_id} requires a partition without scenario record writes"
        ));
    }
}

fn writes_target(action: &ScenarioAction, target: (&str, i32)) -> bool {
    match action {
        ScenarioAction::Send { record, .. } => record_matches(record, target),
        ScenarioAction::SendBatch { operations, .. }
        | ScenarioAction::ExecuteTransaction { operations, .. }
        | ScenarioAction::ExecuteTransactionalTransform(
            crate::TransactionalTransformAction { operations, .. },
        ) => operations
            .iter()
            .any(|operation| record_matches(&operation.record, target)),
        ScenarioAction::FenceTransaction { operation, .. } => {
            record_matches(&operation.record, target)
        }
        ScenarioAction::StartConcurrentActors(action) => action.actors.iter().any(|actor| {
            matches!(actor, crate::ConcurrentActor::ProducerSend { record, .. } if record_matches(record, target))
        }),
        _ => false,
    }
}

fn record_matches(record: &crate::RecordSpec, target: (&str, i32)) -> bool {
    record.topic == target.0 && record.partition == target.1
}
