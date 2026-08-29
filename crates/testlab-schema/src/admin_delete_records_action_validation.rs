//! Delete-records validation owns bounded partition and watermark intent.

use std::collections::{BTreeMap, BTreeSet};

use crate::admin_action_validation::{validate_resource, validate_timeout};
use crate::{ClientId, OperationId, ScenarioAction};

pub(crate) fn validate(
    action: &ScenarioAction,
    clients: &BTreeMap<ClientId, bool>,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) -> bool {
    let ScenarioAction::DeleteRecords(action) = action else {
        return false;
    };
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
    validate_timeout(&action.operation_id, action.timeout_ms, problems);
    true
}
