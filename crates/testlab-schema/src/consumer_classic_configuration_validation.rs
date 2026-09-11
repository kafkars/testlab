//! Classic group timing remains explicit and representable at scenario boundaries.

use crate::{ConsumerId, GroupConsumerConfiguration, GroupProtocol};

pub(super) fn validate(
    consumer_id: &ConsumerId,
    protocol: GroupProtocol,
    configuration: &Option<GroupConsumerConfiguration>,
    problems: &mut Vec<String>,
) {
    let Some(timeout_ms) = configuration
        .as_ref()
        .and_then(|value| value.classic_session_timeout_ms)
    else {
        return;
    };
    if protocol != GroupProtocol::Classic {
        problems.push(format!(
            "consumer {consumer_id} sets classic_session_timeout_ms for a non-classic group"
        ));
    }
    if !(1..=i32::MAX as u64).contains(&timeout_ms) {
        problems.push(format!(
            "consumer {consumer_id} classic_session_timeout_ms must be between 1 and {}",
            i32::MAX
        ));
    }
}
