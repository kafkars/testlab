//! Lifecycle verification proves every requested public boundary settles once.

use testlab_schema::{Scenario, ScenarioAction, Violation};

use crate::index::HistoryIndex;
use crate::support::{references, violation};

pub(crate) fn verify_lifecycle(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        match &step.action {
            ScenarioAction::CreateClient { client_id } => check(
                "LIFE-001",
                "client creation",
                references(index.clients_created.get(client_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::CreateProducer { producer_id, .. } => check(
                "LIFE-002",
                "producer creation",
                references(index.producers_created.get(producer_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::Flush { producer_id } => check(
                "LIFE-003",
                "producer flush",
                references(index.flushes.get(producer_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::CloseProducer { producer_id } => check(
                "LIFE-004",
                "producer close",
                references(index.producers_closed.get(producer_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::ShutdownClient { client_id } => check(
                "LIFE-005",
                "client shutdown",
                references(index.clients_shutdown.get(client_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::SetBrokerBehavior { .. } | ScenarioAction::Send { .. } => {}
        }
    }
    let evidence = index
        .finished
        .iter()
        .map(|sequence| format!("history:{sequence}"))
        .collect();
    check("LIFE-006", "adapter finish", evidence, violations);
}

fn check(contract: &str, operation: &str, evidence: Vec<String>, violations: &mut Vec<Violation>) {
    if evidence.len() != 1 {
        violations.push(violation(
            contract,
            format!(
                "expected exactly one {operation} event, observed {}",
                evidence.len()
            ),
            None,
            evidence,
        ));
    }
}
