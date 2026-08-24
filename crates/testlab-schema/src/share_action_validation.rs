//! Share-action validation owns retained batch identity and exact terminal consumption.

use std::collections::{BTreeMap, BTreeSet};

use crate::scenario_action_validation::ActionStates;
use crate::{ConsumerId, OperationId, ScenarioAction};

#[derive(Clone, Debug)]
pub(crate) struct ShareBatchState {
    consumer_id: ConsumerId,
    settled: bool,
}

pub(crate) type ShareBatchStates = BTreeMap<OperationId, ShareBatchState>;

pub(crate) fn validate(
    action: &ScenarioAction,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::CreateShareConsumer {
            client_id,
            consumer_id,
            group_id,
            topic,
            membership_timeout_ms,
            close_timeout_ms,
        } => {
            crate::consumer_action_validation::create_group(
                client_id,
                consumer_id,
                group_id,
                topic,
                &state.clients,
                &mut state.consumers,
                problems,
            );
            validate_timeout("membership", *membership_timeout_ms, problems);
            validate_timeout("close", *close_timeout_ms, problems);
        }
        ScenarioAction::ShareReceive {
            consumer_id,
            receive_id,
            expected_operation_id,
            minimum_delivery_count,
            timeout_ms,
        } => receive(
            consumer_id,
            receive_id,
            expected_operation_id,
            *minimum_delivery_count,
            *timeout_ms,
            state,
            problems,
        ),
        ScenarioAction::ShareAcknowledge {
            consumer_id,
            receive_id,
            acknowledgement_id,
            timeout_ms,
            ..
        } => {
            settle_batch(consumer_id, receive_id, &mut state.share_batches, problems);
            unique(acknowledgement_id, &mut state.operation_ids, problems);
            validate_timeout("acknowledgement", *timeout_ms, problems);
        }
        ScenarioAction::DropShareBatch {
            consumer_id,
            receive_id,
        } => settle_batch(consumer_id, receive_id, &mut state.share_batches, problems),
        ScenarioAction::CloseShareConsumer { consumer_id, .. } => {
            crate::consumer_action_validation::close(consumer_id, &mut state.consumers, problems);
            for batch in state.share_batches.values_mut() {
                if &batch.consumer_id == consumer_id {
                    batch.settled = true;
                }
            }
        }
        _ => {}
    }
}

fn receive(
    consumer_id: &ConsumerId,
    receive_id: &OperationId,
    expected: &OperationId,
    minimum_delivery_count: i16,
    timeout_ms: u64,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    crate::receive_action_validation::validate(
        consumer_id,
        receive_id,
        expected,
        timeout_ms,
        &mut state.consumers,
        &mut (&mut state.operation_ids, &mut state.sends),
        problems,
    );
    if minimum_delivery_count < 1 {
        problems.push(format!(
            "share receive {receive_id} minimum delivery count must be positive"
        ));
    }
    if state
        .share_batches
        .insert(
            receive_id.clone(),
            ShareBatchState {
                consumer_id: consumer_id.clone(),
                settled: false,
            },
        )
        .is_some()
    {
        problems.push(format!("duplicate share receive {receive_id}"));
    }
}

fn settle_batch(
    consumer_id: &ConsumerId,
    receive_id: &OperationId,
    batches: &mut ShareBatchStates,
    problems: &mut Vec<String>,
) {
    match batches.get_mut(receive_id) {
        Some(batch) if &batch.consumer_id != consumer_id => problems.push(format!(
            "share batch {receive_id} belongs to {}, not {consumer_id}",
            batch.consumer_id
        )),
        Some(batch) if batch.settled => {
            problems.push(format!("share batch {receive_id} settled more than once"));
        }
        Some(batch) => batch.settled = true,
        None => problems.push(format!("missing share batch {receive_id} was used")),
    }
}

fn validate_timeout(label: &str, timeout_ms: u64, problems: &mut Vec<String>) {
    if !(100..=60_000).contains(&timeout_ms) {
        problems.push(format!(
            "share {label} timeout_ms must be between 100 and 60000"
        ));
    }
}

fn unique(id: &OperationId, identities: &mut BTreeSet<OperationId>, problems: &mut Vec<String>) {
    if !identities.insert(id.clone()) {
        problems.push(format!("duplicate operation id {id}"));
    }
}

pub(crate) fn unsettled(batches: &ShareBatchStates) -> Vec<OperationId> {
    batches
        .iter()
        .filter_map(|(id, batch)| (!batch.settled).then_some(id.clone()))
        .collect()
}
