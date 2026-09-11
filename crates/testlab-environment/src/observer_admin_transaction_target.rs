//! Transaction Admin targets preserve exact action and command identities.

use testlab_schema::{
    AdapterCommand, DescribeTransactionsCommand, FenceProducersCommand, ListTransactionsCommand,
    OperationId, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, TargetMatch};
use crate::observer_error::ObserverError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TransactionTarget {
    List(OperationId),
    Descriptions {
        operation_id: OperationId,
        transactional_ids: Vec<String>,
    },
}

impl TransactionTarget {
    pub(super) const fn operation_id(&self) -> &OperationId {
        match self {
            Self::List(operation_id) | Self::Descriptions { operation_id, .. } => operation_id,
        }
    }

    pub(super) fn observation_count(&self) -> usize {
        match self {
            Self::List(_) => 1,
            Self::Descriptions {
                transactional_ids, ..
            } => transactional_ids.len(),
        }
    }
}

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    Ok(Some(match action {
        ScenarioAction::ListTransactions(action) => (
            AdapterCommand::ListTransactions(ListTransactionsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::Transactions(TransactionTarget::List(action.operation_id.clone())),
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
                AdminTarget::Transactions(TransactionTarget::Descriptions {
                    operation_id: action.operation_id.clone(),
                    transactional_ids,
                }),
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
                AdminTarget::Transactions(TransactionTarget::Descriptions {
                    operation_id: action.operation_id.clone(),
                    transactional_ids,
                }),
            )
        }
        _ => return Ok(None),
    }))
}
