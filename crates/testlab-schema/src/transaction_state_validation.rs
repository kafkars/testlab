//! Transaction state queries keep lifecycle reporting separate from action routing.

use crate::transaction_action_validation::TransactionStates;
use crate::{ClientId, ProducerId};

pub(crate) fn open_for_client(
    transactions: &TransactionStates,
    client_id: &ClientId,
) -> Vec<String> {
    transactions
        .iter()
        .filter(|(_, owner)| &owner.client_id == client_id && !owner.closed)
        .map(|(producer, _)| producer.to_string())
        .collect()
}

pub(crate) fn unclosed(transactions: TransactionStates) -> Vec<ProducerId> {
    transactions
        .into_iter()
        .filter_map(|(producer, owner)| (!owner.closed).then_some(producer))
        .collect()
}
