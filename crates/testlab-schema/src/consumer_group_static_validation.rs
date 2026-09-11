//! Static group identities remain unique across live members of one group.

use super::{ConsumerGroupInput, ConsumerStates};
use crate::ConsumerId;

pub(super) fn validate(
    group: &ConsumerGroupInput<'_>,
    consumers: &ConsumerStates,
    consumer_id: &ConsumerId,
    problems: &mut Vec<String>,
) {
    let Some(group_instance_id) = group.group_instance_id else {
        return;
    };
    super::validate_name(
        consumer_id,
        "group instance",
        group_instance_id,
        255,
        problems,
    );
    if consumers.values().any(|state| {
        !state.closed
            && state.group.as_ref().is_some_and(|existing| {
                existing.group_id == group.group_id
                    && existing.group_instance_id.as_deref() == Some(group_instance_id)
            })
    }) {
        problems.push(format!(
            "consumer {consumer_id} repeats live static member {group_instance_id} in group {}",
            group.group_id
        ));
    }
}
