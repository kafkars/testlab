//! Fenced-broker expectations require one reversible paired observation window.

use crate::{DescribeClusterAction, Scenario, ScenarioAction};

pub(super) fn validate(scenario: &Scenario, problems: &mut Vec<String>) {
    for (index, step) in scenario.steps.iter().enumerate() {
        let ScenarioAction::DescribeCluster(action) = &step.action else {
            continue;
        };
        if !action.expected_fenced_broker_ids.is_empty() && !has_reversible_pair(scenario, index) {
            problems.push(format!(
                "admin operation {} with fenced-broker expectations must belong to an immediate baseline, stop, exclude, include, start, and restored description window",
                action.operation_id
            ));
        }
    }
}

fn has_reversible_pair(scenario: &Scenario, index: usize) -> bool {
    let Some(current) = description(scenario, index) else {
        return false;
    };
    let excluded_index = if current.include_fenced_brokers {
        let Some(index) = index.checked_sub(1) else {
            return false;
        };
        index
    } else {
        index
    };
    let Some(baseline_index) = excluded_index.checked_sub(2) else {
        return false;
    };
    let Some(stop_index) = excluded_index.checked_sub(1) else {
        return false;
    };
    let Some(included_index) = excluded_index.checked_add(1) else {
        return false;
    };
    let Some(start_index) = excluded_index.checked_add(2) else {
        return false;
    };
    let Some(restored_index) = excluded_index.checked_add(3) else {
        return false;
    };
    let (Some(baseline), Some(excluded), Some(included), Some(restored)) = (
        description(scenario, baseline_index),
        description(scenario, excluded_index),
        description(scenario, included_index),
        description(scenario, restored_index),
    ) else {
        return false;
    };
    let expected = &excluded.expected_fenced_broker_ids;
    let Some(ordinal) = expected
        .first()
        .copied()
        .filter(|_| expected.len() == 1)
        .and_then(|broker| u16::try_from(broker).ok())
    else {
        return false;
    };
    let stopped = matches!(
        scenario.steps.get(stop_index).map(|step| &step.action),
        Some(ScenarioAction::StopBroker { broker_ordinal, .. }) if *broker_ordinal == ordinal
    );
    let started = matches!(
        scenario.steps.get(start_index).map(|step| &step.action),
        Some(ScenarioAction::StartBroker { broker_ordinal, .. }) if *broker_ordinal == ordinal
    );
    !baseline.include_fenced_brokers
        && baseline.expected_fenced_broker_ids.is_empty()
        && !excluded.include_fenced_brokers
        && included.include_fenced_brokers
        && included.expected_fenced_broker_ids.as_slice() == expected.as_slice()
        && !restored.include_fenced_brokers
        && restored.expected_fenced_broker_ids.is_empty()
        && [excluded, included, restored]
            .iter()
            .all(|action| action.client_id == baseline.client_id)
        && [excluded, included, restored].iter().all(|action| {
            action.include_authorized_operations == baseline.include_authorized_operations
        })
        && stopped
        && started
}

fn description(scenario: &Scenario, index: usize) -> Option<&DescribeClusterAction> {
    match &scenario.steps.get(index)?.action {
        ScenarioAction::DescribeCluster(action) => Some(action),
        _ => None,
    }
}

#[cfg(test)]
#[path = "admin_cluster_fenced_validation_test.rs"]
mod tests;
