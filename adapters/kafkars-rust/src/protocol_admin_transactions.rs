//! Transaction discovery preserves canonical public listing and description facts.

use std::io::Write;

use testlab_schema::{
    AdapterCommand, AdapterEvent, AdapterEventEnvelope, AdminTransactionsDescription,
    AdminTransactionsListing, CommandId, DescribeTransactionsCommand, ListTransactionsCommand,
    TransactionDescriptionSnapshot, TransactionListingSnapshot, TransactionTopicSnapshot,
};

use crate::AdapterError;
use crate::admission_retry::retry_until_with_remaining;
use crate::protocol::emit;
use crate::protocol_admin_read::{deadline_after, retry_safe};
use crate::state::AdapterState;

pub(crate) fn dispatch<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: AdapterCommand,
) -> Result<(), AdapterError> {
    match command {
        AdapterCommand::ListTransactions(command) => list(state, writer, command_id, command),
        AdapterCommand::DescribeTransactions(command) => {
            describe(state, writer, command_id, command)
        }
        _ => Err(invalid(
            "non-transaction-discovery command reached dispatcher",
        )),
    }
}

fn list<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: ListTransactionsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .list_transactions()
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let (_, transactions, unknown_filters, broker_errors) = result.into_parts();
    if !unknown_filters.is_empty() || !broker_errors.is_empty() {
        return Err(invalid(
            "unfiltered listing returned unknown filters or broker errors",
        ));
    }
    let transactions = transactions
        .into_iter()
        .map(|transaction| TransactionListingSnapshot {
            transactional_id: transaction.transactional_id().to_owned(),
            producer_id: transaction.producer_id(),
            transaction_state: transaction.transaction_state().to_owned(),
        })
        .collect::<Vec<_>>();
    validate_listings(&transactions)?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TransactionsListed(AdminTransactionsListing {
                operation_id: command.operation_id,
                transactions,
            }),
        ),
    )
}

fn describe<W: Write>(
    state: &AdapterState,
    writer: &mut W,
    command_id: CommandId,
    command: DescribeTransactionsCommand,
) -> Result<(), AdapterError> {
    let deadline = deadline_after(command.timeout_ms);
    let client = state.client(&command.client_id)?;
    let result = retry_until_with_remaining(
        deadline,
        |remaining| {
            client
                .admin()
                .describe_transactions(command.transactional_ids.clone())
                .deadline_after(remaining)
                .submit()
                .wait()
        },
        retry_safe,
    )
    .map_err(AdapterError::Client)?;
    let entries = result.into_transactions().into_entries();
    if entries.len() != command.transactional_ids.len() {
        return Err(invalid("description returned the wrong outcome count"));
    }
    let transactions = entries
        .into_iter()
        .zip(&command.transactional_ids)
        .map(|((transactional_id, result), expected)| {
            if &transactional_id != expected {
                return Err(invalid("description changed caller transaction order"));
            }
            let description = result.map_err(AdapterError::Client)?;
            let snapshot = TransactionDescriptionSnapshot {
                transactional_id,
                transaction_state: description.transaction_state().to_owned(),
                transaction_timeout_ms: description.transaction_timeout_ms(),
                transaction_start_time_ms: description.transaction_start_time_ms(),
                producer_id: description.producer_id(),
                producer_epoch: description.producer_epoch(),
                topics: description
                    .topics()
                    .iter()
                    .map(|topic| TransactionTopicSnapshot {
                        topic: topic.topic().to_owned(),
                        partitions: topic.partitions().to_vec(),
                    })
                    .collect(),
            };
            validate_description(&snapshot)?;
            Ok(snapshot)
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;
    emit(
        writer,
        &AdapterEventEnvelope::new(
            command_id,
            AdapterEvent::TransactionsDescribed(AdminTransactionsDescription {
                operation_id: command.operation_id,
                transactions,
            }),
        ),
    )
}

fn validate_listings(transactions: &[TransactionListingSnapshot]) -> Result<(), AdapterError> {
    if transactions.iter().any(|transaction| {
        invalid_id(&transaction.transactional_id)
            || transaction.producer_id < 0
            || invalid_state(&transaction.transaction_state)
    }) || transactions
        .windows(2)
        .any(|pair| pair[0].transactional_id.as_bytes() >= pair[1].transactional_id.as_bytes())
    {
        return Err(invalid("listing returned malformed or noncanonical rows"));
    }
    Ok(())
}

fn validate_description(value: &TransactionDescriptionSnapshot) -> Result<(), AdapterError> {
    if invalid_id(&value.transactional_id)
        || invalid_state(&value.transaction_state)
        || value.transaction_timeout_ms <= 0
        || value
            .transaction_start_time_ms
            .is_some_and(|timestamp| timestamp < 0)
        || value.producer_id < 0
        || value.producer_epoch < 0
        || !canonical_topics(&value.topics)
    {
        return Err(invalid(
            "description returned malformed or noncanonical state",
        ));
    }
    Ok(())
}

fn canonical_topics(topics: &[TransactionTopicSnapshot]) -> bool {
    topics.iter().all(|topic| {
        !topic.topic.is_empty()
            && !topic.partitions.is_empty()
            && topic.partitions.iter().all(|partition| *partition >= 0)
            && topic.partitions.windows(2).all(|pair| pair[0] < pair[1])
    }) && topics
        .windows(2)
        .all(|pair| pair[0].topic.as_bytes() < pair[1].topic.as_bytes())
}

fn invalid_id(value: &str) -> bool {
    value.is_empty() || value.chars().any(char::is_whitespace)
}

fn invalid_state(value: &str) -> bool {
    value.is_empty() || value.chars().any(char::is_whitespace)
}

fn invalid(detail: &str) -> AdapterError {
    AdapterError::AdminResult(format!("transaction discovery {detail}"))
}
