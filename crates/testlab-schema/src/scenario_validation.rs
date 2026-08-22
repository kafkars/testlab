//! Scenario validation owns identity, lifecycle, and assertion coherence.

use std::collections::BTreeSet;

use crate::{
    Capability, GroupProtocol, OperationId, SCENARIO_SCHEMA_VERSION, Scenario, ScenarioAction,
    ScenarioError, TerminalStatus, VisibilityExpectation,
};

pub(crate) fn validate(scenario: &Scenario) -> Result<(), ScenarioError> {
    let mut problems = Vec::new();
    validate_header(scenario, &mut problems);
    validate_steps(scenario, &mut problems);
    if problems.is_empty() {
        Ok(())
    } else {
        Err(ScenarioError { problems })
    }
}

fn validate_header(scenario: &Scenario, problems: &mut Vec<String>) {
    if scenario.schema_version != SCENARIO_SCHEMA_VERSION {
        problems.push(format!(
            "unsupported schema version {}, expected {SCENARIO_SCHEMA_VERSION}",
            scenario.schema_version
        ));
    }
    if scenario.title.trim().is_empty() {
        problems.push("title must not be empty".to_owned());
    }
    if scenario.description.trim().is_empty() {
        problems.push("description must not be empty".to_owned());
    }
    if !(100..=600_000).contains(&scenario.timeout_ms) {
        problems.push("timeout_ms must be between 100 and 600000".to_owned());
    }
    if scenario.steps.is_empty() {
        problems.push("scenario must contain at least one step".to_owned());
    }
}

fn validate_steps(scenario: &Scenario, problems: &mut Vec<String>) {
    let mut step_ids = BTreeSet::new();
    let mut state = crate::scenario_action_validation::ActionStates::default();
    let mut usage = BTreeSet::new();
    for step in &scenario.steps {
        if !step_ids.insert(step.id.clone()) {
            problems.push(format!("duplicate step id {}", step.id));
        }
        record_usage(&step.action, &mut usage);
        crate::scenario_action_validation::validate_action(&step.action, &mut state, problems);
    }
    validate_required_capabilities(scenario, &usage, &state, problems);
    validate_assertions(scenario, &state.sends, &state.transaction_sends, problems);
    for (producer, (_, closed)) in state.producers {
        if !closed {
            problems.push(format!("producer {producer} was not closed"));
        }
    }
    for consumer in crate::consumer_action_validation::unclosed(state.consumers) {
        problems.push(format!("consumer {consumer} was not closed"));
    }
    for producer in crate::transaction_action_validation::unclosed(state.transactions) {
        problems.push(format!("transactional producer {producer} was not closed"));
    }
    for (client, shutdown) in state.clients {
        if !shutdown {
            problems.push(format!("client {client} was not shut down"));
        }
    }
}

fn record_usage(action: &ScenarioAction, usage: &mut BTreeSet<Capability>) {
    let capability = match action {
        ScenarioAction::SetBrokerBehavior { .. } => Some(Capability::ModelBroker),
        ScenarioAction::AwaitClientReady { .. } => Some(Capability::ClientReadiness),
        ScenarioAction::SendBatch { .. } => Some(Capability::ProducerBatch),
        ScenarioAction::CreateAssignedConsumer { .. }
        | ScenarioAction::AssignBeginning { .. }
        | ScenarioAction::Receive { .. }
        | ScenarioAction::CloseAssignedConsumer { .. } => Some(Capability::AssignedConsumer),
        ScenarioAction::CreateGroupConsumer { protocol, .. } => Some(match protocol {
            GroupProtocol::Classic => Capability::ConsumerGroups,
            GroupProtocol::Consumer => Capability::ConsumerProtocolGroups,
        }),
        ScenarioAction::CreateTopic { .. } => Some(Capability::Admin),
        ScenarioAction::CreateTransactionalProducer { .. }
        | ScenarioAction::ExecuteTransaction { .. }
        | ScenarioAction::CloseTransactionalProducer { .. } => Some(Capability::Transactions),
        _ => None,
    };
    usage.extend(capability);
}

fn validate_required_capabilities(
    scenario: &Scenario,
    usage: &BTreeSet<Capability>,
    state: &crate::scenario_action_validation::ActionStates,
    problems: &mut Vec<String>,
) {
    require(
        !state.sends.is_empty(),
        Capability::Producer,
        "send steps require the producer capability",
        scenario,
        problems,
    );
    require(
        usage.contains(&Capability::ProducerBatch),
        Capability::ProducerBatch,
        "batch-send steps require the producer_batch capability",
        scenario,
        problems,
    );
    require(
        usage.contains(&Capability::AssignedConsumer),
        Capability::AssignedConsumer,
        "assigned-consumer steps require the assigned_consumer capability",
        scenario,
        problems,
    );
    require(
        usage.contains(&Capability::ConsumerGroups),
        Capability::ConsumerGroups,
        "classic group-consumer steps require the consumer_groups capability",
        scenario,
        problems,
    );
    require(
        usage.contains(&Capability::ConsumerProtocolGroups),
        Capability::ConsumerProtocolGroups,
        "KIP-848 group-consumer steps require the consumer_protocol_groups capability",
        scenario,
        problems,
    );
    require(
        usage.contains(&Capability::Admin),
        Capability::Admin,
        "admin steps require the admin capability",
        scenario,
        problems,
    );
    require(
        usage.contains(&Capability::Transactions),
        Capability::Transactions,
        "transaction steps require the transactions capability",
        scenario,
        problems,
    );
    require(
        usage.contains(&Capability::ModelBroker),
        Capability::ModelBroker,
        "broker-control steps require the model_broker capability",
        scenario,
        problems,
    );
    require(
        usage.contains(&Capability::ClientReadiness),
        Capability::ClientReadiness,
        "client readiness steps require the client_readiness capability",
        scenario,
        problems,
    );
    let handles = !state.clients.is_empty()
        || !state.producers.is_empty()
        || !state.consumers.is_empty()
        || !state.transactions.is_empty();
    require(
        handles,
        Capability::Lifecycle,
        "handle steps require the lifecycle capability",
        scenario,
        problems,
    );
}

fn require(
    used: bool,
    capability: Capability,
    message: &str,
    scenario: &Scenario,
    problems: &mut Vec<String>,
) {
    if used && !scenario.requires.contains(&capability) {
        problems.push(message.to_owned());
    }
}

fn validate_assertions(
    scenario: &Scenario,
    operations: &BTreeSet<OperationId>,
    transaction_sends: &crate::transaction_action_validation::TransactionSends,
    problems: &mut Vec<String>,
) {
    let mut asserted = BTreeSet::new();
    for assertion in &scenario.assertions {
        if !operations.contains(&assertion.operation_id) {
            problems.push(format!(
                "assertion references missing operation {}",
                assertion.operation_id
            ));
        }
        if !asserted.insert(assertion.operation_id.clone()) {
            problems.push(format!(
                "duplicate assertion for operation {}",
                assertion.operation_id
            ));
        }
        validate_assertion_semantics(
            assertion,
            transaction_sends.get(&assertion.operation_id).copied(),
            problems,
        );
    }
    for operation in operations {
        if !asserted.contains(operation) {
            problems.push(format!("operation {operation} has no assertion"));
        }
    }
}

fn validate_assertion_semantics(
    assertion: &crate::OperationAssertion,
    transaction: Option<crate::TransactionDisposition>,
    problems: &mut Vec<String>,
) {
    match (assertion.accepted, assertion.terminal) {
        (true, None) => problems.push(format!(
            "accepted operation {} requires a terminal expectation",
            assertion.operation_id
        )),
        (false, Some(_)) => problems.push(format!(
            "rejected operation {} must not declare a terminal expectation",
            assertion.operation_id
        )),
        _ => {}
    }
    if !assertion.accepted && assertion.visibility != VisibilityExpectation::Absent {
        problems.push(format!(
            "rejected operation {} must expect absent visibility",
            assertion.operation_id
        ));
    }
    if let Some(disposition) = transaction {
        validate_transaction_assertion(assertion, disposition, problems);
    } else if assertion.terminal == Some(TerminalStatus::TransactionStaged) {
        problems.push(format!(
            "non-transactional operation {} cannot expect transaction_staged",
            assertion.operation_id
        ));
    }
    if assertion.terminal == Some(TerminalStatus::Acknowledged)
        && assertion.visibility != VisibilityExpectation::ExactlyOnce
    {
        problems.push(format!(
            "acknowledged operation {} must expect exactly-once visibility",
            assertion.operation_id
        ));
    }
    if assertion.terminal == Some(TerminalStatus::DefinitelyNotSent)
        && assertion.visibility != VisibilityExpectation::Absent
    {
        problems.push(format!(
            "definitely-not-sent operation {} must expect absent visibility",
            assertion.operation_id
        ));
    }
}

fn validate_transaction_assertion(
    assertion: &crate::OperationAssertion,
    disposition: crate::TransactionDisposition,
    problems: &mut Vec<String>,
) {
    if !assertion.accepted || assertion.terminal != Some(TerminalStatus::TransactionStaged) {
        problems.push(format!(
            "transactional operation {} must expect accepted transaction_staged delivery",
            assertion.operation_id
        ));
    }
    let expected = match disposition {
        crate::TransactionDisposition::Commit => VisibilityExpectation::ExactlyOnce,
        crate::TransactionDisposition::Abort => VisibilityExpectation::Absent,
    };
    if assertion.visibility != expected {
        problems.push(format!(
            "transactional operation {} with {disposition:?} must expect {expected:?} visibility",
            assertion.operation_id
        ));
    }
}
