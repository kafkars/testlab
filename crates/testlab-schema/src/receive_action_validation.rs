//! Receive validation owns consumer readiness and cross-operation identities.

use std::collections::BTreeSet;

use crate::consumer_action_validation::ConsumerStates;
use crate::{ConsumerId, OperationId};

pub(crate) fn validate(
    consumer_id: &ConsumerId,
    receive_id: &OperationId,
    expected_operation_id: &OperationId,
    timeout_ms: u64,
    consumers: &mut ConsumerStates,
    identities: &mut (&mut BTreeSet<OperationId>, &BTreeSet<OperationId>),
    problems: &mut Vec<String>,
) {
    crate::consumer_action_validation::receive(
        consumer_id,
        receive_id,
        timeout_ms,
        consumers,
        problems,
    );
    if !identities.0.insert(receive_id.clone()) {
        problems.push(format!("duplicate operation id {receive_id}"));
    }
    if !identities.1.contains(expected_operation_id) {
        problems.push(format!(
            "receive {receive_id} expects missing prior send {expected_operation_id}"
        ));
    }
}
