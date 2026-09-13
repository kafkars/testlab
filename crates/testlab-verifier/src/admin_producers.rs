//! Active-producer verification requires exact public and Kafka CLI equality.

use testlab_schema::{
    AdminProducersDescription, BrokerProducersState, ProducerStateSnapshot, ScenarioAction,
    Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_features::Indexed;
use crate::support::violation;

pub(crate) fn verify_producers_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    let command_window = index.admin_command_window(action);
    let ScenarioAction::DescribeProducers(action) = action else {
        return false;
    };
    let public = index
        .admin_features
        .producers_described
        .get(&action.operation_id);
    let independent = index
        .admin_features
        .producers_observed
        .get(&action.operation_id);
    if exact_match(
        public,
        independent,
        command_window,
        action.expected_producer_count,
    ) {
        return true;
    }
    violations.push(violation(
        contract(action),
        format!(
            "admin operation {} expected {} canonical active producer(s) exactly matching one immediate Kafka CLI snapshot",
            action.operation_id, action.expected_producer_count
        ),
        Some(action.operation_id.clone()),
        evidence(public, independent),
    ));
    true
}

pub(crate) const fn contract(action: &testlab_schema::DescribeProducersAction) -> &'static str {
    if action.broker_id.is_some() {
        "ADMIN-093"
    } else {
        "ADMIN-051"
    }
}

fn exact_match(
    public: Option<&Vec<Indexed<AdminProducersDescription>>>,
    independent: Option<&Vec<Indexed<BrokerProducersState>>>,
    window: Option<AdminCommandWindow>,
    expected_count: usize,
) -> bool {
    let (Some([public]), Some([independent])) =
        (public.map(Vec::as_slice), independent.map(Vec::as_slice))
    else {
        return false;
    };
    public.value.topic == independent.value.topic
        && public.value.partition == independent.value.partition
        && public.value.producers.len() == expected_count
        && canonical(&public.value.producers)
        && public.value.producers == independent.value.producers
        && public_after_command(window, public.history_sequence)
        && immediate_after_public(
            window,
            public.history_sequence,
            independent.history_sequence,
        )
}

pub(crate) fn canonical(producers: &[ProducerStateSnapshot]) -> bool {
    producers.iter().all(|producer| {
        producer.producer_id >= 0
            && producer.producer_epoch >= 0
            && producer.last_sequence >= -1
            && producer.last_timestamp >= -1
            && producer.coordinator_epoch >= 0
            && producer
                .current_transaction_start_offset
                .is_none_or(|offset| offset >= 0)
    }) && producers
        .windows(2)
        .all(|pair| pair[0].producer_id < pair[1].producer_id)
}

fn evidence(
    public: Option<&Vec<Indexed<AdminProducersDescription>>>,
    independent: Option<&Vec<Indexed<BrokerProducersState>>>,
) -> Vec<String> {
    public
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.value.observation)),
        )
        .collect()
}
