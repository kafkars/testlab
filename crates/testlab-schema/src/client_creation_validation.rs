//! Cluster-identity guards remain bounded scenario intent with explicit failure state.

use crate::scenario_action_state::ClientStates;
use crate::{ClientId, ScenarioAction};

const MAX_EXPECTED_CLUSTER_ID_BYTES: usize = 1_024;

pub(crate) fn validate_action(
    action: &ScenarioAction,
    clients: &mut ClientStates,
    problems: &mut Vec<String>,
) {
    match action {
        ScenarioAction::CreateClient(action) => validate_identity(
            &action.client_id,
            action.expected_cluster_id.as_deref(),
            action.expected_error_code.as_deref(),
            clients,
            problems,
        ),
        ScenarioAction::CreateConfiguredClient(action) => {
            crate::producer_configuration_validation::validate(&action.configuration, problems);
            create_live(&action.client_id, clients, problems);
        }
        ScenarioAction::CreateAssignedConsumerClient(action) => {
            let owner = format!("assigned-consumer client {}", action.client_id);
            crate::consumer_configuration::validate(
                &owner,
                action.configuration.fetch,
                action.configuration.limits,
                problems,
            );
            create_live(&action.client_id, clients, problems);
        }
        _ => unreachable!("non-client-creation action reached client validation"),
    }
}

fn validate_identity(
    client_id: &ClientId,
    expected_cluster_id: Option<&str>,
    expected_error_code: Option<&str>,
    clients: &mut ClientStates,
    problems: &mut Vec<String>,
) {
    match expected_cluster_id {
        Some("") => problems.push("expected_cluster_id must not be empty".to_owned()),
        Some(cluster_id) if cluster_id.len() > MAX_EXPECTED_CLUSTER_ID_BYTES => {
            problems.push("expected_cluster_id must not exceed 1024 bytes".to_owned());
        }
        _ => {}
    }
    if expected_error_code.is_some() && expected_cluster_id.is_none() {
        problems.push("create_client expected_error_code requires expected_cluster_id".to_owned());
    }
    if expected_error_code.is_some_and(|code| code != "identity") {
        problems.push("create_client expected_error_code must be identity".to_owned());
    }
    if expected_error_code.is_none() {
        create_live(client_id, clients, problems);
    }
}

pub(crate) fn create_live(
    client_id: &ClientId,
    clients: &mut ClientStates,
    problems: &mut Vec<String>,
) {
    if clients.insert(client_id.clone(), false).is_some() {
        problems.push(format!("duplicate client id {client_id}"));
    }
}

#[cfg(test)]
#[path = "client_creation_test.rs"]
mod test;
