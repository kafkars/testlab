//! Transaction Admin actions translate without leaking fixture expectations.

use testlab_schema::{
    AdapterCommand, DescribeTransactionsCommand, FenceProducersCommand, ListTransactionsCommand,
    ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::ListTransactions(action) => (
            AdapterCommand::ListTransactions(ListTransactionsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::TransactionsListed(action.operation_id.clone()),
        ),
        ScenarioAction::DescribeTransactions(action) => {
            let transactional_ids = action
                .transactions
                .iter()
                .map(|transaction| transaction.transactional_id.clone())
                .collect::<Vec<_>>();
            (
                AdapterCommand::DescribeTransactions(DescribeTransactionsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    transactional_ids: transactional_ids.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                ExpectedEvent::TransactionsDescribed(
                    action.operation_id.clone(),
                    transactional_ids,
                ),
            )
        }
        ScenarioAction::FenceProducers(action) => {
            let transactional_ids = action
                .producers
                .iter()
                .map(|transaction| transaction.transactional_id.clone())
                .collect::<Vec<_>>();
            (
                AdapterCommand::FenceProducers(FenceProducersCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    transactional_ids: transactional_ids.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                ExpectedEvent::ProducersFenced(action.operation_id.clone(), transactional_ids),
            )
        }
        _ => return None,
    })
}
