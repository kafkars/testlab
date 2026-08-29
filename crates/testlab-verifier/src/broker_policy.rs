//! Broker-policy verification joins external policy truth to bounded public behavior.

use testlab_schema::{
    BrokerPolicy, BrokerPolicyState, BrokerQuotaDirection, Scenario, ScenarioAction,
    TerminalStatus, Violation,
};

use crate::broker_policy_control::{PolicyFact, facts, references, valid};
use crate::index::HistoryIndex;
use crate::support::violation;

pub(crate) struct PolicyWindow<'a> {
    pub(crate) present: PolicyFact<'a>,
    pub(crate) absent: PolicyFact<'a>,
}

pub(crate) fn verify(
    scenario: &Scenario,
    index: &HistoryIndex,
    observations: &[testlab_schema::BrokerObservation],
    violations: &mut Vec<Violation>,
) {
    for step in &scenario.steps {
        let ScenarioAction::AlterBrokerPolicy(action) = &step.action else {
            continue;
        };
        if action.state != BrokerPolicyState::Present {
            continue;
        }
        let present = facts(index, &action.policy, BrokerPolicyState::Present);
        let absent = facts(index, &action.policy, BrokerPolicyState::Absent);
        let window = match (present.as_slice(), absent.as_slice()) {
            ([present], [absent])
                if valid(present, &action.policy, BrokerPolicyState::Present)
                    && valid(absent, &action.policy, BrokerPolicyState::Absent)
                    && present.observation_sequence < absent.alter_sequence =>
            {
                Some(PolicyWindow {
                    present: PolicyFact {
                        alter_sequence: present.alter_sequence,
                        alter: present.alter,
                        query_sequence: present.query_sequence,
                        query: present.query,
                        observation_sequence: present.observation_sequence,
                        observation: present.observation,
                    },
                    absent: PolicyFact {
                        alter_sequence: absent.alter_sequence,
                        alter: absent.alter,
                        query_sequence: absent.query_sequence,
                        query: absent.query,
                        observation_sequence: absent.observation_sequence,
                        observation: absent.observation,
                    },
                })
            }
            _ => None,
        };
        let Some(window) = window else {
            violations.push(violation(
                "POLICY-001",
                format!(
                    "broker policy {:?} expected one exact observed apply/remove chain",
                    action.policy
                ),
                None,
                present.iter().chain(&absent).flat_map(references).collect(),
            ));
            continue;
        };
        match &action.policy {
            BrokerPolicy::Acl { .. } => crate::broker_policy_acl::verify(
                scenario,
                &action.policy,
                &window,
                index,
                observations,
                violations,
            ),
            BrokerPolicy::Quota {
                direction,
                minimum_active_ms,
                ..
            } => verify_quota(
                scenario,
                *direction,
                *minimum_active_ms,
                &window,
                index,
                violations,
            ),
        }
    }
}

fn verify_quota(
    scenario: &Scenario,
    direction: BrokerQuotaDirection,
    minimum_active_ms: u64,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) {
    let active_ms = window
        .absent
        .alter
        .started_unix_ms
        .saturating_sub(window.present.observation.completed_unix_ms);
    let progress = match direction {
        BrokerQuotaDirection::Producer => producer_progress(scenario, window, index),
        BrokerQuotaDirection::Consumer => consumer_progress(scenario, window, index),
    };
    if active_ms >= minimum_active_ms && progress.is_some() {
        return;
    }
    let mut evidence = references(&window.present);
    evidence.extend(references(&window.absent));
    evidence.extend(progress.map(|sequence| format!("history:{sequence}")));
    violations.push(violation(
        "POLICY-004",
        format!(
            "{direction:?} quota expected at least {minimum_active_ms} ms active with public progress; observed {active_ms} ms"
        ),
        None,
        evidence,
    ));
}

fn producer_progress(
    scenario: &Scenario,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
) -> Option<u64> {
    scenario.steps.iter().find_map(|step| {
        let ScenarioAction::Send { operation_id, .. } = &step.action else {
            return None;
        };
        let command = command_sequence(index, operation_id)?;
        let terminal = index
            .terminals
            .get(operation_id)?
            .iter()
            .find(|value| value.status == TerminalStatus::Acknowledged)?;
        (active(window, command) && active(window, terminal.history_sequence))
            .then_some(terminal.history_sequence)
    })
}

fn consumer_progress(
    scenario: &Scenario,
    window: &PolicyWindow<'_>,
    index: &HistoryIndex,
) -> Option<u64> {
    scenario.steps.iter().find_map(|step| {
        let (ScenarioAction::Receive { receive_id, .. }
        | ScenarioAction::GroupReceive {
            receive_id,
            expected_error_code: None,
            ..
        }) = &step.action
        else {
            return None;
        };
        let command = command_sequence(index, receive_id)?;
        let receive = index.receives.get(receive_id)?.iter().find(|value| {
            !value.records.is_empty() && value.committed.is_none_or(|committed| committed)
        })?;
        (active(window, command) && active(window, receive.history_sequence))
            .then_some(receive.history_sequence)
    })
}

fn command_sequence(
    index: &HistoryIndex,
    operation_id: &testlab_schema::OperationId,
) -> Option<u64> {
    index
        .commands
        .iter()
        .find_map(|(sequence, _, command)| match command {
            testlab_schema::AdapterCommand::Send {
                operation_id: actual,
                ..
            }
            | testlab_schema::AdapterCommand::Receive {
                receive_id: actual, ..
            }
            | testlab_schema::AdapterCommand::GroupReceive {
                receive_id: actual, ..
            } if actual == operation_id => Some(*sequence),
            _ => None,
        })
}

pub(crate) fn active(window: &PolicyWindow<'_>, sequence: u64) -> bool {
    window.present.observation_sequence < sequence && sequence < window.absent.alter_sequence
}
