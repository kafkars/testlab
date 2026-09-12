//! Group-receive failure validation keeps expected errors tied to configured behavior.

use super::ConsumerStates;
use crate::{GroupOffsetReset, ScenarioAction};

pub(super) fn validate(
    action: &ScenarioAction,
    consumers: &ConsumerStates,
    problems: &mut Vec<String>,
) {
    let ScenarioAction::GroupReceive {
        consumer_id,
        receive_id,
        expected_error_code: Some(code),
        ..
    } = action
    else {
        return;
    };
    if code == crate::GROUP_AUTHORIZATION_ERROR_CODE {
        return;
    }
    if code != crate::GROUP_MISSING_OFFSET_ERROR_CODE {
        problems.push(format!(
            "group receive {receive_id} has unsupported expected error code {code}"
        ));
        return;
    }
    let fail_closed = consumers
        .get(consumer_id)
        .and_then(|consumer| consumer.group.as_ref())
        .and_then(|group| group.offset_reset)
        == Some(GroupOffsetReset::Error);
    if !fail_closed {
        problems.push(format!(
            "group receive {receive_id} expects a missing-offset error without offset_reset=error"
        ));
    }
}
