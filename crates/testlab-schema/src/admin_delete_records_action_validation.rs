//! Delete-records validation owns bounded partition and watermark intent.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_identity, validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

const MAX_TARGETS: usize = 32;

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    match action {
        ScenarioAction::DeleteRecords(action) => {
            validate_resource(
                &action.client_id,
                &action.operation_id,
                &action.topic,
                "topic",
                249,
                clients,
                operation_ids,
                problems,
            );
            validate_single(action, problems);
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        ScenarioAction::DeleteRecordsBatch(action) => {
            validate_identity(
                &action.client_id,
                &action.operation_id,
                clients,
                operation_ids,
                problems,
            );
            if !(2..=MAX_TARGETS).contains(&action.targets.len()) {
                problems.push(format!(
                    "admin operation {} targets must contain 2 to {MAX_TARGETS} entries",
                    action.operation_id
                ));
            }
            let mut targets = BTreeSet::new();
            for target in &action.targets {
                validate_batch_target(&action.operation_id, target, problems);
                if !targets.insert((target.topic.as_str(), target.partition)) {
                    problems.push(format!(
                        "admin operation {} delete-records targets must be unique",
                        action.operation_id
                    ));
                }
            }
            validate_timeout(&action.operation_id, action.timeout_ms, problems);
        }
        _ => return false,
    }
    true
}

fn validate_single(action: &crate::DeleteRecordsAction, problems: &mut Vec<String>) {
    if action.partition < 0 {
        problems.push(format!(
            "admin operation {} partition must be nonnegative",
            action.operation_id
        ));
    }
    if action.before_offset <= 0 {
        problems.push(format!(
            "admin operation {} before_offset must be positive",
            action.operation_id
        ));
    }
    if action.expected_high_watermark <= action.before_offset {
        problems.push(format!(
            "admin operation {} expected_high_watermark must exceed before_offset",
            action.operation_id
        ));
    }
}

fn validate_batch_target(
    operation_id: &OperationId,
    target: &crate::DeleteRecordsBatchExpectation,
    problems: &mut Vec<String>,
) {
    if target.topic.is_empty() || target.topic.len() > 249 {
        problems.push(format!("admin operation {operation_id} has invalid topic"));
    }
    if target.partition < 0 {
        problems.push(format!(
            "admin operation {operation_id} partition must be nonnegative"
        ));
    }
    if target.expected_high_watermark <= 0 {
        problems.push(format!(
            "admin operation {operation_id} expected_high_watermark must be positive"
        ));
    }
    if let crate::DeleteRecordsBoundary::Offset { offset } = target.boundary {
        if offset <= 0 {
            problems.push(format!(
                "admin operation {operation_id} deletion offset must be positive"
            ));
        }
        if target.expected_high_watermark <= offset {
            problems.push(format!(
                "admin operation {operation_id} expected_high_watermark must exceed deletion offset"
            ));
        }
    }
}
