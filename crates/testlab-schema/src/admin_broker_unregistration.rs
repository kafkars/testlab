//! Broker-unregistration payloads keep reversible topology expectations off the wire.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, Scenario, ScenarioAction};

/// Scenario intent for one reversible public broker unregistration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UnregisterBrokerAction {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact nonnegative Kafka broker ID to unregister.
    pub broker_id: i32,
    /// Scenario-only broker IDs expected immediately after unregistration.
    pub expected_remaining_broker_ids: Vec<i32>,
    /// Earlier cluster-description operation proving the complete baseline.
    pub baseline_operation_id: OperationId,
    /// Later cluster-description operation proving broker re-registration.
    pub restored_operation_id: OperationId,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Wire payload for one public broker unregistration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UnregisterBrokerCommand {
    /// Existing client whose admin handle is used.
    pub client_id: ClientId,
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact nonnegative Kafka broker ID to unregister.
    pub broker_id: i32,
    /// Complete public operation bound.
    pub timeout_ms: u64,
}

/// Public completion for one successful broker unregistration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminBrokerUnregistration {
    /// Stable admin operation identity.
    pub operation_id: OperationId,
    /// Exact broker ID passed to the public API.
    pub broker_id: i32,
    /// Nonnegative throttle reported by Kafka.
    pub throttle_time_ms: u64,
}

pub(crate) fn validate_action(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::UnregisterBroker(action) = action else {
        return false;
    };
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    if action.broker_id < 0 {
        problems.push(format!(
            "admin operation {} broker_id must be nonnegative",
            action.operation_id
        ));
    }
    if action.expected_remaining_broker_ids.is_empty()
        || action.expected_remaining_broker_ids.len() > 100
    {
        problems.push(format!(
            "admin operation {} expected_remaining_broker_ids must contain between 1 and 100 brokers",
            action.operation_id
        ));
    }
    if action
        .expected_remaining_broker_ids
        .iter()
        .any(|broker| *broker < 0)
        || action
            .expected_remaining_broker_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        problems.push(format!(
            "admin operation {} expected_remaining_broker_ids must be strictly increasing and nonnegative",
            action.operation_id
        ));
    }
    if action
        .expected_remaining_broker_ids
        .contains(&action.broker_id)
    {
        problems.push(format!(
            "admin operation {} expected remaining brokers include unregistered broker {}",
            action.operation_id, action.broker_id
        ));
    }
    if action.baseline_operation_id == action.operation_id
        || action.restored_operation_id == action.operation_id
        || action.baseline_operation_id == action.restored_operation_id
    {
        problems.push(format!(
            "admin operation {} requires three distinct operation identities",
            action.operation_id
        ));
    }
    true
}

pub(crate) fn validate_transitions(scenario: &Scenario, problems: &mut Vec<String>) {
    for (index, step) in scenario.steps.iter().enumerate() {
        let ScenarioAction::UnregisterBroker(action) = &step.action else {
            continue;
        };
        if !has_reversible_window(scenario, index, action) {
            problems.push(format!(
                "admin operation {} must be immediately bracketed by baseline describe, matching broker stop/start, and restored describe steps",
                action.operation_id
            ));
        }
    }
}

fn has_reversible_window(
    scenario: &Scenario,
    index: usize,
    action: &UnregisterBrokerAction,
) -> bool {
    let Ok(ordinal) = u16::try_from(action.broker_id) else {
        return false;
    };
    let Some(baseline_index) = index.checked_sub(2) else {
        return false;
    };
    let Some(stop_index) = index.checked_sub(1) else {
        return false;
    };
    let Some(start_index) = index.checked_add(1) else {
        return false;
    };
    let Some(restored_index) = index.checked_add(2) else {
        return false;
    };
    matches!(
        scenario.steps.get(baseline_index).map(|step| &step.action),
        Some(ScenarioAction::DescribeCluster(describe))
            if describe.client_id == action.client_id
                && describe.operation_id == action.baseline_operation_id
    ) && matches!(
        scenario.steps.get(stop_index).map(|step| &step.action),
        Some(ScenarioAction::StopBroker { broker_ordinal, .. }) if *broker_ordinal == ordinal
    ) && matches!(
        scenario.steps.get(start_index).map(|step| &step.action),
        Some(ScenarioAction::StartBroker { broker_ordinal, .. }) if *broker_ordinal == ordinal
    ) && matches!(
        scenario.steps.get(restored_index).map(|step| &step.action),
        Some(ScenarioAction::DescribeCluster(describe))
            if describe.client_id == action.client_id
                && describe.operation_id == action.restored_operation_id
    )
}

#[cfg(test)]
#[path = "admin_broker_unregistration_test.rs"]
mod tests;
