//! Transaction staging verification binds batch scenarios to one exact public method.

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction, TransactionSendMethod, Violation};

use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) fn verify(scenario: &Scenario, index: &HistoryIndex, violations: &mut Vec<Violation>) {
    for step in &scenario.steps {
        let ScenarioAction::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            method: TransactionSendMethod::SendBatch,
            disposition,
            timeout_ms,
        } = &step.action
        else {
            continue;
        };
        let expected = AdapterCommand::ExecuteTransaction {
            producer_id: producer_id.clone(),
            transaction_id: transaction_id.clone(),
            operations: operations.clone(),
            method: TransactionSendMethod::SendBatch,
            disposition: *disposition,
            timeout_ms: *timeout_ms,
        };
        let commands = index
            .commands
            .iter()
            .filter(|(_, _, command)| {
                matches!(
                    command,
                    AdapterCommand::ExecuteTransaction {
                        transaction_id: actual,
                        ..
                    } if actual == transaction_id
                )
            })
            .collect::<Vec<_>>();
        let exact = commands.len() == 1 && matches!(&commands[0].2, actual if actual == &expected);
        if exact {
            continue;
        }
        violations.push(violation(
            "TXN-009",
            format!(
                "transaction {transaction_id} selected send_batch but observed {} matching command(s) without one exact batch request",
                commands.len()
            ),
            Some(transaction_id.clone()),
            commands
                .iter()
                .map(|(sequence, ..)| format!("history:{sequence}"))
                .collect(),
        ));
    }
}
