//! Share verification binds retained batches, delivery counts, dispositions, and close certainty.

use testlab_schema::{Scenario, ScenarioAction, TerminalStatus, Violation};

use crate::index::HistoryIndex;
use crate::support::{references, violation};

pub(crate) fn verify_share(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        match &step.action {
            ScenarioAction::CreateShareConsumer { consumer_id, .. } => check_count(
                "SHARE-001",
                "share consumer creation",
                index
                    .share_consumers_created
                    .get(consumer_id)
                    .map(Vec::as_slice),
                violations,
            ),
            ScenarioAction::ShareReceive {
                consumer_id,
                receive_id,
                expected_operation_ids,
                minimum_delivery_count,
                expected_acquisition_count,
                ..
            } => crate::share_receive::verify(
                scenario,
                index,
                crate::share_receive::Expectation {
                    consumer_id,
                    receive_id,
                    operation_ids: expected_operation_ids,
                    minimum_delivery_count: *minimum_delivery_count,
                    acquisition_count: *expected_acquisition_count,
                },
                violations,
            ),
            ScenarioAction::ShareAcknowledge {
                receive_id,
                acknowledgement_id,
                dispositions,
                ..
            } => {
                let values = index.share_acknowledgements.get(acknowledgement_id);
                let matches = values.is_some_and(|values| {
                    values.len() == 1
                        && values[0].receive_id == *receive_id
                        && values[0].dispositions == *dispositions
                        && values[0].success
                        && values[0].delivery.is_none()
                        && values[0].code.is_none()
                });
                if !matches {
                    violations.push(violation(
                        if dispositions.len() > 1 {
                            "SHARE-008"
                        } else {
                            "SHARE-003"
                        },
                        format!(
                            "share acknowledgement {acknowledgement_id} did not settle exactly once for batch {receive_id} with {dispositions:?}"
                        ),
                        Some(acknowledgement_id.clone()),
                        values.into_iter().flatten().map(|value| {
                            format!("history:{}", value.history_sequence)
                        }).collect(),
                    ));
                }
            }
            ScenarioAction::DropShareBatch { receive_id, .. } => check_count(
                "SHARE-003",
                "share batch drop",
                index
                    .share_batches_dropped
                    .get(receive_id)
                    .map(Vec::as_slice),
                violations,
            ),
            ScenarioAction::CloseShareConsumer {
                consumer_id,
                expect_success,
            } => verify_close(index, consumer_id, *expect_success, violations),
            _ => {}
        }
    }
}

fn verify_close(
    index: &HistoryIndex,
    consumer_id: &testlab_schema::ConsumerId,
    expect_success: bool,
    violations: &mut Vec<Violation>,
) {
    let closes = index.share_consumers_closed.get(consumer_id);
    let exact = closes.is_some_and(|values| values.len() == 1);
    if !exact {
        violations.push(violation(
            "SHARE-004",
            format!(
                "share consumer {consumer_id} expected one close terminal, observed {}",
                closes.map_or(0, Vec::len)
            ),
            None,
            closes
                .into_iter()
                .flatten()
                .map(|value| format!("history:{}", value.history_sequence))
                .collect(),
        ));
        return;
    }
    let close = &closes.map_or(&[][..], Vec::as_slice)[0];
    let coherent = if expect_success {
        close.success && close.delivery.is_none() && close.code.is_none()
    } else {
        !close.success
            && matches!(
                close.delivery,
                Some(TerminalStatus::DefinitelyNotSent | TerminalStatus::PossiblySent)
            )
            && close.code.is_some()
    };
    if !coherent {
        violations.push(violation(
            "SHARE-004",
            format!(
                "share consumer {consumer_id} close success={} delivery={:?} code={:?} contradicted expect_success={expect_success}",
                close.success, close.delivery, close.code
            ),
            None,
            vec![format!("history:{}", close.history_sequence)],
        ));
    }
}

fn check_count(
    contract: &str,
    label: &str,
    values: Option<&[u64]>,
    violations: &mut Vec<Violation>,
) {
    if values.map_or(0, <[u64]>::len) != 1 {
        violations.push(violation(
            contract,
            format!(
                "expected one {label}, observed {}",
                values.map_or(0, <[u64]>::len)
            ),
            None,
            references(values),
        ));
    }
}
