//! Consumer-group evidence formatting stays separate from deterministic matching.

use crate::index::{
    IndexedAdminGroupCompletion, IndexedConsumerGroupDescription, IndexedConsumerGroupObservation,
    IndexedConsumerGroupOffset, IndexedConsumerGroupOffsetObservation, IndexedConsumerGroupsList,
};

pub(crate) fn offset_evidence(
    public: Option<&Vec<IndexedConsumerGroupOffset>>,
    independent: Option<&Vec<IndexedConsumerGroupOffsetObservation>>,
) -> Vec<String> {
    history_and_state(
        public
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence),
        independent
            .into_iter()
            .flatten()
            .map(|value| value.observation),
    )
}

pub(crate) fn group_list_evidence(
    public: Option<&Vec<IndexedConsumerGroupsList>>,
    independent: Option<&Vec<IndexedConsumerGroupObservation>>,
) -> Vec<String> {
    history_and_state(
        public
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence),
        independent
            .into_iter()
            .flatten()
            .map(|value| value.observation),
    )
}

pub(crate) fn group_description_evidence(
    public: Option<&Vec<IndexedConsumerGroupDescription>>,
    independent: Option<&Vec<IndexedConsumerGroupObservation>>,
) -> Vec<String> {
    history_and_state(
        public
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence),
        independent
            .into_iter()
            .flatten()
            .map(|value| value.observation),
    )
}

pub(crate) fn group_delete_evidence(
    public: Option<&Vec<IndexedAdminGroupCompletion>>,
    independent: Option<&Vec<IndexedConsumerGroupObservation>>,
) -> Vec<String> {
    history_and_state(
        public
            .into_iter()
            .flatten()
            .map(|value| value.history_sequence),
        independent
            .into_iter()
            .flatten()
            .map(|value| value.observation),
    )
}

fn history_and_state(
    history: impl Iterator<Item = u64>,
    state: impl Iterator<Item = u64>,
) -> Vec<String> {
    history
        .map(|value| format!("history:{value}"))
        .chain(state.map(|value| format!("broker-state-observation:{value}")))
        .collect()
}
