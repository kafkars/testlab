//! Replica log-directory alteration validation bounds exact caller intent.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::admin_action_validation::{validate_identity, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_ASSIGNMENTS: usize = 32;
const MAX_PATH_BYTES: usize = 4_096;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::AlterReplicaLogDirs(action) = action else {
        return false;
    };
    validate_identity(
        &action.client_id,
        &action.operation_id,
        clients,
        operation_ids,
        problems,
    );
    if !(1..=MAX_ASSIGNMENTS).contains(&action.assignments.len()) {
        problems.push(format!(
            "admin operation {} must alter 1 to {MAX_ASSIGNMENTS} replicas",
            action.operation_id
        ));
    }
    let mut replicas = BTreeSet::new();
    let mut selection = None;
    for assignment in &action.assignments {
        validate_assignment(&action.operation_id, assignment, problems);
        if !replicas.insert((
            assignment.topic.as_str(),
            assignment.partition,
            assignment.broker_id,
        )) {
            problems.push(format!(
                "admin operation {} repeats a replica assignment",
                action.operation_id
            ));
        }
        let selected = (assignment.topic.as_str(), assignment.partition);
        if selection
            .replace(selected)
            .is_some_and(|prior| prior != selected)
        {
            problems.push(format!(
                "admin operation {} assignments must select one topic-partition",
                action.operation_id
            ));
        }
    }
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    true
}

fn validate_assignment(
    operation_id: &OperationId,
    assignment: &crate::ReplicaLogDirAssignmentSpec,
    problems: &mut Vec<String>,
) {
    if assignment.topic.is_empty()
        || assignment.topic.len() > 249
        || assignment.topic.chars().any(char::is_whitespace)
        || assignment.partition < 0
        || assignment.broker_id < 0
    {
        problems.push(format!(
            "admin operation {operation_id} has an invalid replica identity"
        ));
    }
    if assignment.target_path.is_empty()
        || assignment.target_path.len() > MAX_PATH_BYTES
        || !Path::new(&assignment.target_path).is_absolute()
        || assignment.target_path.chars().any(char::is_control)
    {
        problems.push(format!(
            "admin operation {operation_id} has an invalid absolute target_path"
        ));
    }
}
