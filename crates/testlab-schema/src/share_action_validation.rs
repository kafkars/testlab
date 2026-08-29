//! Share-action validation owns retained batch identity and exact terminal consumption.

use std::collections::{BTreeMap, BTreeSet};

use crate::consumer_action_validation::ConsumerGroupInput;
use crate::scenario_action_validation::ActionStates;
use crate::{ConsumerId, OperationId, ScenarioAction};

#[derive(Clone, Debug)]
pub(crate) struct ShareBatchState {
    consumer_id: ConsumerId,
    record_count: usize,
    settled: bool,
}

pub(crate) type ShareBatchStates = BTreeMap<OperationId, ShareBatchState>;

#[derive(Clone, Copy)]
struct ShareReceiveInput<'a> {
    consumer_id: &'a ConsumerId,
    receive_id: &'a OperationId,
    expected: &'a [OperationId],
    minimum_delivery_count: i16,
    expected_acquisition_count: Option<usize>,
    timeout_ms: u64,
}

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
            configuration,
        } => {
            crate::consumer_action_validation::create_group(
                client_id,
                consumer_id,
                ConsumerGroupInput {
                    group_id,
                    topic,
                    protocol: None,
                },
                &state.clients,
                &mut state.consumers,
                problems,
            );
            validate_timeout("membership", *membership_timeout_ms, problems);
            validate_timeout("close", *close_timeout_ms, problems);
            if let Some(configuration) = configuration {
                validate_configuration(consumer_id, *configuration, problems);
            }
        }
        ScenarioAction::ShareReceive {
            consumer_id,
            receive_id,
            expected_operation_ids,
            minimum_delivery_count,
            expected_acquisition_count,
            timeout_ms,
        } => receive(
            ShareReceiveInput {
                consumer_id,
                receive_id,
                expected: expected_operation_ids,
                minimum_delivery_count: *minimum_delivery_count,
                expected_acquisition_count: *expected_acquisition_count,
                timeout_ms: *timeout_ms,
            },
            state,
            problems,
        ),
        ScenarioAction::ShareAcknowledge {
            consumer_id,
            receive_id,
            acknowledgement_id,
            dispositions,
            timeout_ms,
        } => {
            validate_dispositions(receive_id, dispositions, &state.share_batches, problems);
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

fn receive(input: ShareReceiveInput<'_>, state: &mut ActionStates, problems: &mut Vec<String>) {
    let ShareReceiveInput {
        consumer_id,
        receive_id,
        expected,
        minimum_delivery_count,
        expected_acquisition_count,
        timeout_ms,
    } = input;
    crate::consumer_action_validation::receive(
        consumer_id,
        receive_id,
        timeout_ms,
        &mut state.consumers,
        problems,
    );
    unique(receive_id, &mut state.operation_ids, problems);
    if expected.is_empty() || expected.len() > 31 {
        problems.push(format!(
            "share receive {receive_id} must expect between 1 and 31 records"
        ));
    }
    let mut distinct = BTreeSet::new();
    for operation_id in expected {
        if !state.sends.contains(operation_id) {
            problems.push(format!(
                "share receive {receive_id} expects missing prior send {operation_id}"
            ));
        }
        if !distinct.insert(operation_id) {
            problems.push(format!(
                "share receive {receive_id} repeats expected send {operation_id}"
            ));
        }
    }
    if minimum_delivery_count < 1 {
        problems.push(format!(
            "share receive {receive_id} minimum delivery count must be positive"
        ));
    }
    if expected_acquisition_count.is_some_and(|count| count == 0 || count > expected.len()) {
        problems.push(format!(
            "share receive {receive_id} acquisition count must be between 1 and its {} expected records",
            expected.len()
        ));
    }
    if state
        .share_batches
        .insert(
            receive_id.clone(),
            ShareBatchState {
                consumer_id: consumer_id.clone(),
                record_count: expected.len(),
                settled: false,
            },
        )
        .is_some()
    {
        problems.push(format!("duplicate share receive {receive_id}"));
    }
}

fn validate_configuration(
    consumer_id: &ConsumerId,
    configuration: crate::ShareConsumerFetchConfiguration,
    problems: &mut Vec<String>,
) {
    for (name, value) in [
        ("max_records", configuration.max_records),
        ("batch_size", configuration.batch_size),
    ] {
        if !(1..=31).contains(&value) {
            problems.push(format!(
                "share consumer {consumer_id} {name} must be between 1 and 31"
            ));
        }
    }
}

fn validate_dispositions(
    receive_id: &OperationId,
    dispositions: &[crate::ShareDisposition],
    batches: &ShareBatchStates,
    problems: &mut Vec<String>,
) {
    if batches
        .get(receive_id)
        .is_some_and(|batch| batch.record_count != dispositions.len())
    {
        problems.push(format!(
            "share acknowledgement for {receive_id} must provide one disposition per expected record"
        ));
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
