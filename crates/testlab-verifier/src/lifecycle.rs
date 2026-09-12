//! Lifecycle verification proves every requested public boundary settles once.
use crate::index::HistoryIndex;
use crate::support::{references, violation};
use testlab_schema::{Scenario, ScenarioAction, Violation};
pub(crate) fn verify_lifecycle(
    scenario: &Scenario,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    if !index.commands.is_empty() {
        crate::lifecycle_commands::verify(scenario, index, violations);
        return;
    }
    for step in &scenario.steps {
        if !index.action_issued(&step.action) {
            continue;
        }
        if verify_transaction_lifecycle(&step.action, index, violations) {
            continue;
        }
        if verify_group_lifecycle(&step.action, index, violations) {
            continue;
        }
        if crate::lifecycle_nonterminal::has_no_lifecycle_terminal(&step.action) {
            continue;
        }
        if let Some(client_id) =
            crate::lifecycle_commands::successful_client_creation_id(&step.action)
        {
            check(
                "LIFE-001",
                "client creation",
                references(index.clients_created.get(client_id).map(Vec::as_slice)),
                violations,
            );
            continue;
        }
        match &step.action {
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
            ScenarioAction::AssignBeginningBatch(action) => check(
                "LIFE-009",
                "direct assignment",
                references(
                    index
                        .assignments
                        .get(&action.consumer_id)
                        .map(Vec::as_slice),
                ),
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
            ScenarioAction::ShutdownClient { client_id } => check(
                "LIFE-005",
                "client shutdown",
                references(index.clients_shutdown.get(client_id).map(Vec::as_slice)),
                violations,
            ),
            _ => unreachable!("non-lifecycle action was filtered before lifecycle matching"),
        }
    }
    verify_finish(index, violations);
}
fn verify_group_lifecycle(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    match action {
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
        _ => return false,
    }
    true
}
fn verify_finish(index: &HistoryIndex, violations: &mut Vec<Violation>) {
    if index.command_failures.is_empty() && index.finish_issued() {
        let evidence = index
            .finished
            .iter()
            .map(|sequence| format!("history:{sequence}"))
            .collect();
        check("LIFE-006", "adapter finish", evidence, violations);
    }
}
fn verify_transaction_lifecycle(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    match action {
        ScenarioAction::CreateTransactionalProducer {
            expected_error_code: Some(_),
            ..
        } => {}
        ScenarioAction::CreateTransactionalProducer { producer_id, .. } => check(
            "LIFE-013",
            "transactional producer creation",
            references(
                index
                    .transactional_producers_created
                    .get(producer_id)
                    .map(Vec::as_slice),
            ),
            violations,
        ),
        ScenarioAction::CloseTransactionalProducer(action) => check(
            "LIFE-014",
            "transactional producer close",
            references(
                index
                    .transactional_producers_closed
                    .get(&action.producer_id)
                    .map(Vec::as_slice),
            ),
            violations,
        ),
        ScenarioAction::FenceTransaction {
            replacement_producer_id,
            ..
        } => check(
            "LIFE-013",
            "replacement transactional producer creation",
            references(
                index
                    .transactional_producers_created
                    .get(replacement_producer_id)
                    .map(Vec::as_slice),
            ),
            violations,
        ),
        _ => return false,
    }
    true
}
fn check(contract: &str, operation: &str, evidence: Vec<String>, violations: &mut Vec<Violation>) {
    if evidence.len() != 1 {
        let message = format!(
            "expected exactly one {operation} event, observed {}",
            evidence.len()
        );
        violations.push(violation(contract, message, None, evidence));
    }
}
