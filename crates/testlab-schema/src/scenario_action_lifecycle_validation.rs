//! Client shutdown validation rejects every still-open owned public handle.

use crate::ClientId;
use crate::scenario_action_validation::ActionStates;

pub(crate) fn shutdown_client(
    client_id: &ClientId,
    state: &mut ActionStates,
    problems: &mut Vec<String>,
) {
    let open = state
        .producers
        .iter()
        .filter(|(_, (owner, closed))| owner == client_id && !closed)
        .map(|(producer, _)| producer.to_string())
        .collect::<Vec<_>>();
    if !open.is_empty() {
        problems.push(format!(
            "client {client_id} shut down with open producers {}",
            open.join(", ")
        ));
    }
    let open_consumers =
        crate::consumer_action_validation::open_for_client(&state.consumers, client_id);
    if !open_consumers.is_empty() {
        problems.push(format!(
            "client {client_id} shut down with open consumers {}",
            open_consumers.join(", ")
        ));
    }
    let open_transactions =
        crate::transaction_action_validation::open_for_client(&state.transactions, client_id);
    if !open_transactions.is_empty() {
        problems.push(format!(
            "client {client_id} shut down with open transactional producers {}",
            open_transactions.join(", ")
        ));
    }
    match state.clients.get_mut(client_id) {
        Some(shutdown) if !*shutdown => *shutdown = true,
        Some(_) => problems.push(format!("client {client_id} shut down more than once")),
        None => problems.push(format!("missing client {client_id} was shut down")),
    }
}
