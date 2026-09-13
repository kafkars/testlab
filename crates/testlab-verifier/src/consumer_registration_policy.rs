//! Hosted registration policy verification stays separate from returned-handle identity.

use testlab_schema::{AdapterEvent, Violation};

use super::ExpectedRegistration;
use crate::support::violation;

pub(super) fn verify(
    expected: &ExpectedRegistration,
    command_sequence: u64,
    events: &[(u64, &AdapterEvent)],
    violations: &mut Vec<Violation>,
) {
    let (contract_id, exact) = match expected {
        ExpectedRegistration::Group {
            selected_protocol,
            selected_configuration,
            ..
        } => (
            "CONS-035",
            events.len() == 1
                && events.first().is_some_and(|(sequence, event)| {
                    *sequence > command_sequence
                        && matches!(
                            event,
                            AdapterEvent::GroupConsumerCreated(observation)
                                if observation.selected_protocol == *selected_protocol
                                    && observation.selected_configuration.as_ref()
                                        == selected_configuration.as_ref()
                        )
                }),
        ),
        ExpectedRegistration::Share {
            rack,
            membership_timeout_ms,
            close_timeout_ms,
            configuration,
            ..
        } => (
            "SHARE-016",
            events.len() == 1
                && events.first().is_some_and(|(sequence, event)| {
                    *sequence > command_sequence
                        && matches!(
                            event,
                            AdapterEvent::ShareConsumerCreated(observation)
                                if observation.selected_builder.rack == *rack
                                    && observation.selected_builder.fetch.as_ref()
                                        == configuration.as_ref()
                                    && observation.selected_builder.close_timeout_ms
                                        == *close_timeout_ms
                                    && bounded_membership_timeout(
                                        observation.selected_builder.membership_start_timeout_ns,
                                        *membership_timeout_ms,
                                    )
                        )
                }),
        ),
    };
    if exact {
        return;
    }
    violations.push(violation(
        contract_id,
        format!(
            "hosted consumer {} expected one later exact selected builder-policy observation",
            expected.consumer_id()
        ),
        None,
        std::iter::once(command_sequence)
            .chain(events.iter().map(|(sequence, _)| *sequence))
            .map(|sequence| format!("history:{sequence}"))
            .collect(),
    ));
}

fn bounded_membership_timeout(selected_ns: u64, requested_ms: u64) -> bool {
    selected_ns > 0 && u128::from(selected_ns) <= u128::from(requested_ms) * 1_000_000
}
