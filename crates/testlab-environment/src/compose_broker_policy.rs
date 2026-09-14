//! Compose broker-policy controls retain raw terminals and normalized query facts.

use std::{
    thread,
    time::{Duration, Instant},
};

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
        if action.state == BrokerPolicyState::Absent {
            let minimum = match &action.policy {
                BrokerPolicy::Quota {
                    minimum_active_ms, ..
                } => Duration::from_millis(*minimum_active_ms),
                BrokerPolicy::Acl { .. } => Duration::ZERO,
            };
            if minimum >= remaining(deadline) {
                phase.fail(
                    "environment_broker_policy_deadline_elapsed",
                    "broker policy minimum active window exceeded the action deadline",
                );
                return phase;
            }
            thread::sleep(minimum);
        }
        if action.state == BrokerPolicyState::Present
            && !self.alter_broker_policy_support(
                &mut phase,
                &action.policy,
                action.state,
                &service,
                deadline,
            )
        {
            return phase;
        }
        if !self.apply_and_confirm_broker_policy(
            &mut phase,
            &action.policy,
            action.state,
            &service,
            deadline,
        ) {
            return phase;
        }
        if action.state == BrokerPolicyState::Absent
            && !self.alter_broker_policy_support(
                &mut phase,
                &action.policy,
                action.state,
                &service,
                deadline,
            )
        {
            return phase;
        }
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

    fn apply_and_confirm_broker_policy(
        &mut self,
        phase: &mut ComposePhase,
        policy: &BrokerPolicy,
        state: BrokerPolicyState,
        service: &str,
        deadline: Instant,
    ) -> bool {
        let alter = broker_policy_command::alter(
            &self.prefix,
            service,
            self.client_port,
            policy,
            state,
            self.next_operation,
        );
        if !self.required(phase, alter, deadline) {
            return false;
        }
        let query = broker_policy_command::query(
            &self.prefix,
            service,
            self.client_port,
            policy,
            self.next_operation,
        );
        let output = match self.execute(query, remaining(deadline)) {
            Ok(output) => output,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return false;
            }
        };
        let observed = crate::broker_policy_observation::parse(policy, &output.stdout);
        if !phase.retain(output) {
            phase.fail(
                "environment_broker_policy_query_failed",
                "broker policy query terminal failed",
            );
            return false;
        }
        let observed = match observed {
            Ok(observed) => observed,
            Err(error) => {
                phase.fail("environment_broker_policy_query_invalid", error);
                return false;
            }
        };
        if observed != (state == BrokerPolicyState::Present) {
            phase.fail(
                "environment_broker_policy_state_mismatch",
                format!("broker policy query did not confirm {state:?}"),
            );
            return false;
        }
        self.record_policy_observation(phase, policy, state);
        true
    }

    fn alter_broker_policy_support(
        &mut self,
        phase: &mut ComposePhase,
        policy: &BrokerPolicy,
        state: BrokerPolicyState,
        service: &str,
        deadline: Instant,
    ) -> bool {
        for index in 0..broker_policy_command::supporting_access_count(policy) {
            let Some(command) = broker_policy_command::supporting_access(
                &self.prefix,
                service,
                self.client_port,
                policy,
                state,
                index,
                self.next_operation,
            ) else {
                phase.fail(
                    "environment_broker_policy_support_invalid",
                    "broker policy support command was missing",
                );
                return false;
            };
            if !self.required(phase, command, deadline) {
                return false;
            }
        }
        true
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
