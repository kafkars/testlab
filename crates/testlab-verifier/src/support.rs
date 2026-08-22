//! Shared verifier helpers create stable violations and evidence references.

use testlab_schema::{BrokerObservation, ContractId, OperationId, Violation};

use crate::index::IndexedTerminal;

pub(crate) fn violation(
    contract: &str,
    message: String,
    operation_id: Option<OperationId>,
    evidence: Vec<String>,
) -> Violation {
    let contract_id =
        ContractId::new(contract).unwrap_or_else(|error| panic!("known contract id: {error}"));
    Violation {
        contract_id,
        message,
        operation_id,
        evidence,
    }
}

pub(crate) fn references(values: Option<&[u64]>) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .map(|sequence| format!("history:{sequence}"))
        .collect()
}

pub(crate) fn terminal_references(values: Option<&[IndexedTerminal]>) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .map(|terminal| format!("history:{}", terminal.history_sequence))
        .collect()
}

pub(crate) fn observation_references(values: Option<&[&BrokerObservation]>) -> Vec<String> {
    values
        .into_iter()
        .flatten()
        .map(|observation| format!("broker-observation:{}", observation.observation))
        .collect()
}
