//! Leader-election validation bounds exact scopes, targets, and transition claims.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_PARTITIONS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::ElectLeaders(action) = action else {
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
    match &action.targets {
        Some(targets) => {
            validate_targets(&action.operation_id, targets, "selected", problems);
            if !action.required_partitions.is_empty() {
                problems.push(format!(
                    "admin operation {} selected election cannot declare required_partitions",
                    action.operation_id
                ));
            }
        }
        None => validate_targets(
            &action.operation_id,
            &action.required_partitions,
            "required cluster-wide",
            problems,
        ),
    }
    let required = action
        .targets
        .as_deref()
        .unwrap_or(&action.required_partitions);
    if action.require_leader_change
        && (action.election_type != crate::AdminLeaderElectionType::Preferred
            || required.len() != 1)
    {
        problems.push(format!(
            "admin operation {} can require a leader change only for one required preferred-election partition",
            action.operation_id
        ));
    }
    true
}

fn validate_targets(
    operation_id: &OperationId,
    targets: &[crate::LeaderElectionSelection],
    label: &str,
    problems: &mut Vec<String>,
) {
    if !(1..=MAX_PARTITIONS).contains(&targets.len()) {
        problems.push(format!(
            "admin operation {operation_id} must declare 1 to {MAX_PARTITIONS} {label} partitions"
        ));
    }
    let mut unique = BTreeSet::new();
    for target in targets {
        if target.topic.is_empty()
            || target.topic.len() > 249
            || target.topic.chars().any(char::is_whitespace)
            || target.partition < 0
        {
            problems.push(format!(
                "admin operation {operation_id} has an invalid leader-election target"
            ));
        }
        if !unique.insert((&target.topic, target.partition)) {
            problems.push(format!(
                "admin operation {operation_id} repeats a leader-election target"
            ));
        }
    }
}
