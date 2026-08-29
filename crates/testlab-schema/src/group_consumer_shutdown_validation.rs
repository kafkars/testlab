//! Group-consumer shutdown validation owns handle closure and bounded request counts.

use std::collections::BTreeSet;

use crate::consumer_action_validation::ConsumerStates;
use crate::{GroupConsumerShutdownAction, OperationId};

pub(crate) fn validate(
    action: &GroupConsumerShutdownAction,
    consumers: &mut ConsumerStates,
    operation_ids: &mut BTreeSet<OperationId>,
    problems: &mut Vec<String>,
) {
    let Some(state) =
        crate::consumer_action_validation::open(&action.consumer_id, consumers, problems)
    else {
        return;
    };
    if state.group.is_none() {
        problems.push(format!(
            "consumer {} is not a hosted group consumer",
            action.consumer_id
        ));
    }
    if !operation_ids.insert(action.operation_id.clone()) {
        problems.push(format!("duplicate operation id {}", action.operation_id));
    }
    if !(1..=8).contains(&action.request_count) {
        problems.push(format!(
            "group shutdown {} request_count must be between 1 and 8",
            action.operation_id
        ));
    }
    if !(100..=60_000).contains(&action.timeout_ms) {
        problems.push(format!(
            "group shutdown {} timeout_ms must be between 100 and 60000",
            action.operation_id
        ));
    }
    state.closed = true;
}
