//! Compose broker-policy controls retain raw terminals and normalized query facts.

use std::time::{Duration, Instant};

use testlab_schema::{
    BrokerPolicy, BrokerPolicyAction, BrokerPolicyState, EnvironmentOperation,
    EnvironmentOperationKind, EnvironmentOperationStatus,
};

use crate::broker_policy_command;
use crate::compose::DockerComposeEnvironment;
use crate::compose_support::{elapsed_unix_ms, remaining};
use crate::compose_types::ComposePhase;

impl DockerComposeEnvironment {
    /// Alters one exact client policy and independently queries the resulting state.
    pub fn alter_broker_policy(
        &mut self,
        action: &BrokerPolicyAction,
        timeout: Duration,
    ) -> ComposePhase {
        let mut phase = ComposePhase::default();
        if !self.valid_policy_transition(&action.policy, action.state) {
            phase.fail(
                "environment_broker_policy_transition_invalid",
                format!(
                    "invalid {:?} transition for {:?}",
                    action.state, action.policy
                ),
            );
            return phase;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_broker_policy_deadline_invalid",
                "broker policy deadline overflowed",
            );
            return phase;
        };
        let Some(service) = self.broker_services.first().cloned() else {
            phase.fail(
                "environment_broker_policy_target_invalid",
                "broker policy control requires a declared broker service",
            );
            return phase;
        };
        let alter_sequence = self.next_operation;
        let alter = broker_policy_command::alter(
            &self.prefix,
            &service,
            self.client_port,
            &action.policy,
            action.state,
            alter_sequence,
        );
        if !self.required(&mut phase, alter, deadline) {
            return phase;
        }
        let query_sequence = self.next_operation;
        let query = broker_policy_command::query(
            &self.prefix,
            &service,
            self.client_port,
            &action.policy,
            query_sequence,
        );
        let output = match self.execute(query, remaining(deadline)) {
            Ok(output) => output,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return phase;
            }
        };
        let observed = crate::broker_policy_observation::parse(&action.policy, &output.stdout);
        let query_succeeded = phase.retain(output);
        if !query_succeeded {
            phase.fail(
                "environment_broker_policy_query_failed",
                "broker policy query terminal failed",
            );
            return phase;
        }
        let observed = match observed {
            Ok(observed) => observed,
            Err(error) => {
                phase.fail("environment_broker_policy_query_invalid", error);
                return phase;
            }
        };
        if observed != (action.state == BrokerPolicyState::Present) {
            phase.fail(
                "environment_broker_policy_state_mismatch",
                format!("broker policy query did not confirm {:?}", action.state),
            );
            return phase;
        }
        self.record_policy_observation(&mut phase, &action.policy, action.state);
        if phase.succeeded() {
            match action.state {
                BrokerPolicyState::Present => {
                    self.active_broker_policies.insert(action.policy.clone());
                }
                BrokerPolicyState::Absent => {
                    self.active_broker_policies.remove(&action.policy);
                }
            }
        }
        phase
    }

    fn valid_policy_transition(&self, policy: &BrokerPolicy, state: BrokerPolicyState) -> bool {
        self.active_broker_policies.contains(policy) == (state == BrokerPolicyState::Absent)
    }

    fn record_policy_observation(
        &mut self,
        phase: &mut ComposePhase,
        policy: &BrokerPolicy,
        state: BrokerPolicyState,
    ) {
        let Ok(id) = self.operation_id() else {
            phase.fail(
                "environment_operation_overflow",
                "broker policy observation id overflowed",
            );
            return;
        };
        let now = elapsed_unix_ms(self.started_unix_ms, self.started.elapsed());
        phase.operations.push(EnvironmentOperation {
            id,
            kind: EnvironmentOperationKind::BrokerPolicyObserve,
            program: "testlab-kafka-policy-observer/1".to_owned(),
            args: policy.evidence_args(state),
            started_unix_ms: now,
            completed_unix_ms: now,
            status: EnvironmentOperationStatus::Succeeded,
            exit_code: None,
            stdout_artifact: None,
            stderr_artifact: None,
            diagnostic: None,
        });
    }
}
