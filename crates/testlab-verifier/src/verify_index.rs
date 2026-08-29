//! Verification indexing groups scenario assertions and broker observations by operation.

use std::collections::BTreeMap;

use testlab_schema::{BrokerObservation, OperationAssertion, OperationId, Scenario};

pub(crate) fn assertions(scenario: &Scenario) -> BTreeMap<OperationId, &OperationAssertion> {
    scenario
        .assertions
        .iter()
        .map(|assertion| (assertion.operation_id.clone(), assertion))
        .collect()
}

pub(crate) fn observations_by_operation(
    observations: &[BrokerObservation],
) -> BTreeMap<OperationId, Vec<&BrokerObservation>> {
    let mut by_operation: BTreeMap<OperationId, Vec<&BrokerObservation>> = BTreeMap::new();
    for observation in observations {
        by_operation
            .entry(observation.operation_id.clone())
            .or_default()
            .push(observation);
    }
    by_operation
}
