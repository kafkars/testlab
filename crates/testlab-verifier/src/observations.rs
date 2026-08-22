//! Broker-observation verification rejects records outside declared sends.

use std::collections::BTreeMap;

use testlab_schema::{BrokerObservation, OperationId, RecordSpec, Violation};

use crate::support::{observation_references, violation};

pub(crate) fn verify_unknown(
    sends: &BTreeMap<OperationId, &RecordSpec>,
    observed: &BTreeMap<OperationId, Vec<&BrokerObservation>>,
    violations: &mut Vec<Violation>,
) {
    for operation in observed
        .keys()
        .filter(|operation| !sends.contains_key(*operation))
    {
        violations.push(violation(
            "PROTO-002",
            "broker observed an operation absent from the scenario".to_owned(),
            Some(operation.clone()),
            observation_references(observed.get(operation).map(Vec::as_slice)),
        ));
    }
}
