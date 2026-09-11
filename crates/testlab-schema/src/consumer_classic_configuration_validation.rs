//! Classic group timing remains explicit and representable at scenario boundaries.

use crate::{ConsumerId, GroupConsumerConfiguration, GroupProtocol};

pub(super) fn validate(
    consumer_id: &ConsumerId,
    protocol: GroupProtocol,
    configuration: &Option<GroupConsumerConfiguration>,
    problems: &mut Vec<String>,
) {
    let Some(configuration) = configuration.as_ref() else {
        return;
    };
    if protocol != GroupProtocol::Classic {
        if configuration.classic_assignor.is_some() {
            problems.push(format!(
                "consumer {consumer_id} sets classic_assignor for a non-classic group"
            ));
        }
        if configuration.classic_session_timeout_ms.is_some() {
            problems.push(format!(
                "consumer {consumer_id} sets classic_session_timeout_ms for a non-classic group"
            ));
        }
    }
    if configuration
        .classic_session_timeout_ms
        .is_some_and(|timeout_ms| !(1..=i32::MAX as u64).contains(&timeout_ms))
    {
        problems.push(format!(
            "consumer {consumer_id} classic_session_timeout_ms must be between 1 and {}",
            i32::MAX
        ));
    }
}
