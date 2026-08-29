//! Removed ACL verification requires public success and broker-visible recovery.

use testlab_schema::{
    BrokerAclOperation, BrokerAclResource, Scenario, ScenarioAction, TerminalStatus,
    TransactionDisposition,
};

use crate::broker_policy::PolicyWindow;
use crate::broker_policy_acl::{consumer_group, exact_command};
use crate::index::HistoryIndex;

pub(crate) fn verify(
    scenario: &Scenario,
    resource: &BrokerAclResource,
    operation: BrokerAclOperation,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
    observations: &[testlab_schema::BrokerObservation],
) -> Option<u64> {
    match (resource, operation) {
        (BrokerAclResource::Topic { name }, BrokerAclOperation::Write) => scenario
            .steps
            .iter()
            .find_map(|step| producer(&step.action, name, window, index, observations)),
        (BrokerAclResource::Topic { name }, BrokerAclOperation::Create) => scenario
            .steps
            .iter()
            .find_map(|step| admin(&step.action, name, window, index)),
        (BrokerAclResource::Group { name }, BrokerAclOperation::Read) => scenario
            .steps
            .iter()
            .find_map(|step| group(&step.action, name, window, scenario, index)),
        (BrokerAclResource::TransactionalId { name }, BrokerAclOperation::Write) => {
            transaction(scenario, name, window, index, observations)
        }
        _ => None,
    }
}

fn producer(
    action: &ScenarioAction,
    topic: &str,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
    observations: &[testlab_schema::BrokerObservation],
) -> Option<u64> {
    let ScenarioAction::Send {
        operation_id,
        record,
        ..
    } = action
    else {
        return None;
    };
    let (command, _) = exact_command(action, index)?;
    let [terminal] = index.terminals.get(operation_id)?.as_slice() else {
        return None;
    };
    (record.topic == topic
        && command > window.absent.observation_sequence
        && terminal.history_sequence > window.absent.observation_sequence
        && terminal.status == TerminalStatus::Acknowledged
        && observations
            .iter()
            .filter(|value| value.operation_id == *operation_id)
            .count()
            == 1)
        .then_some(terminal.history_sequence)
}

fn group(
    action: &ScenarioAction,
    group_id: &str,
    window: &PolicyWindow<'_>,
    scenario: &Scenario,
    index: &HistoryIndex,
) -> Option<u64> {
    let ScenarioAction::GroupReceive {
        consumer_id,
        receive_id,
        expected_error_code: None,
        ..
    } = action
    else {
        return None;
    };
    let (command, _) = exact_command(action, index)?;
    let [receive] = index.receives.get(receive_id)?.as_slice() else {
        return None;
    };
    (consumer_group(scenario, consumer_id)? == group_id
        && command > window.absent.observation_sequence
        && receive.history_sequence > window.absent.observation_sequence
        && receive.committed == Some(true)
        && !receive.records.is_empty())
    .then_some(receive.history_sequence)
}

fn admin(
    action: &ScenarioAction,
    topic: &str,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
) -> Option<u64> {
    let ScenarioAction::CreateTopic(create) = action else {
        return None;
    };
    if create.topic != topic || create.expected_error_code.is_some() {
        return None;
    }
    let (command, _) = exact_command(action, index)?;
    let [completion] = index.topics_created.get(&create.operation_id)?.as_slice() else {
        return None;
    };
    let [observed] = index.topics_observed.get(&create.operation_id)?.as_slice() else {
        return None;
    };
    (command > window.absent.observation_sequence
        && completion.history_sequence > window.absent.observation_sequence
        && observed.exists)
        .then_some(completion.history_sequence)
}

fn transaction(
    scenario: &Scenario,
    transactional_id: &str,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
    observations: &[testlab_schema::BrokerObservation],
) -> Option<u64> {
    scenario.steps.iter().find_map(|step| {
        let ScenarioAction::CreateTransactionalProducer {
            producer_id,
            transactional_id: actual,
            expected_error_code: None,
            ..
        } = &step.action
        else {
            return None;
        };
        let (command, _) = exact_command(&step.action, index)?;
        let [created] = index
            .transactional_producers_created
            .get(producer_id)?
            .as_slice()
        else {
            return None;
        };
        if actual != transactional_id || command <= window.absent.observation_sequence {
            return None;
        }
        committed_transaction(scenario, producer_id, *created, index, observations)
    })
}

fn committed_transaction(
    scenario: &Scenario,
    producer_id: &testlab_schema::ProducerId,
    created: u64,
    index: &HistoryIndex,
    observations: &[testlab_schema::BrokerObservation],
) -> Option<u64> {
    scenario.steps.iter().find_map(|candidate| {
        let ScenarioAction::ExecuteTransaction {
            producer_id: owner,
            transaction_id,
            operations,
            disposition: TransactionDisposition::Commit,
            ..
        } = &candidate.action
        else {
            return None;
        };
        let [completion] = index.transactions_completed.get(transaction_id)?.as_slice() else {
            return None;
        };
        (owner == producer_id
            && completion.history_sequence > created
            && operations.iter().all(|operation| {
                observations
                    .iter()
                    .filter(|value| value.operation_id == operation.operation_id)
                    .count()
                    == 1
            }))
        .then_some(completion.history_sequence)
    })
}
