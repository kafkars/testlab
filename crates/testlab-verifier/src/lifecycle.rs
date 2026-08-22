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
        if !index.action_issued(&step.action) {
            continue;
        }
        match &step.action {
            ScenarioAction::CreateClient { client_id } => check(
                "LIFE-001",
                "client creation",
                references(index.clients_created.get(client_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::AwaitClientReady { client_id } => check(
                "LIFE-007",
                "client readiness",
                references(index.clients_ready.get(client_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::CreateProducer { producer_id, .. } => check(
                "LIFE-002",
                "producer creation",
                references(index.producers_created.get(producer_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::CreateAssignedConsumer { consumer_id, .. } => check(
                "LIFE-008",
                "assigned consumer creation",
                references(index.consumers_created.get(consumer_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::AssignBeginning { consumer_id, .. } => check(
                "LIFE-009",
                "direct assignment",
                references(index.assignments.get(consumer_id).map(Vec::as_slice)),
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
            ScenarioAction::CloseAssignedConsumer { consumer_id } => check(
                "LIFE-010",
                "assigned consumer close",
                references(index.consumers_closed.get(consumer_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::CreateGroupConsumer { consumer_id, .. } => check(
                "LIFE-011",
                "group consumer creation",
                references(
                    index
                        .group_consumers_created
                        .get(consumer_id)
                        .map(Vec::as_slice),
                ),
                violations,
            ),
            ScenarioAction::CloseGroupConsumer { consumer_id } => check(
                "LIFE-012",
                "group consumer close",
                references(
                    index
                        .group_consumers_closed
                        .get(consumer_id)
                        .map(Vec::as_slice),
                ),
                violations,
            ),
            ScenarioAction::ShutdownClient { client_id } => check(
                "LIFE-005",
                "client shutdown",
                references(index.clients_shutdown.get(client_id).map(Vec::as_slice)),
                violations,
            ),
            ScenarioAction::SetBrokerBehavior { .. }
            | ScenarioAction::Send { .. }
            | ScenarioAction::SendBatch { .. }
            | ScenarioAction::Receive { .. }
            | ScenarioAction::GroupReceive { .. }
            | ScenarioAction::CreateTopic { .. } => {}
        }
    }
    if index.command_failures.is_empty() && index.finish_issued() {
        let evidence = index
            .finished
            .iter()
            .map(|sequence| format!("history:{sequence}"))
            .collect();
        check("LIFE-006", "adapter finish", evidence, violations);
    }
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
