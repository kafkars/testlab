//! Transaction discovery retains pinned CLI artifacts and canonical broker state.

use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::TerminalOutput;
use crate::compose::DockerComposeEnvironment;
use crate::compose_command::compose_owned;
use crate::compose_support::remaining;
use crate::compose_types::{ComposeFailure, ComposeObservation};
use crate::observer_admin_target::{AdminTarget, ordinal};
use crate::observer_admin_transaction_target::TransactionTarget;

impl DockerComposeEnvironment {
    pub(super) fn observe_transactions_with_cli(
        &mut self,
        target: &AdminTarget,
        timeout: Duration,
    ) -> ComposeObservation {
        let mut observed = ComposeObservation::default();
        let AdminTarget::Transactions(target) = target else {
            observed.phase.fail(
                "environment_observation_failed",
                "non-transaction target reached transaction CLI observer",
            );
            return observed;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            observed.phase.fail(
                "environment_observation_failed",
                "transaction observation deadline overflow",
            );
            return observed;
        };
        let first = match self.begin_admin_observation(&AdminTarget::Transactions(target.clone())) {
            Ok(first) => first,
            Err(error) => {
                observed
                    .phase
                    .fail("environment_observation_failed", error.to_string());
                return observed;
            }
        };
        match target {
            TransactionTarget::List(operation_id) => {
                let output =
                    match self.execute_transaction_cli(vec!["list".to_owned()], "list", deadline) {
                        Ok(output) => output,
                        Err(error) => {
                            observed.phase.fail(error.code, error.diagnostic);
                            return observed;
                        }
                    };
                let stdout = output.stdout.clone();
                if !observed.phase.retain(output) {
                    observed.phase.fail(
                        "environment_observation_failed",
                        "Kafka CLI transaction listing failed",
                    );
                    return observed;
                }
                match crate::transaction_cli_observation::normalize_list(
                    first,
                    operation_id,
                    &stdout,
                ) {
                    Ok(state) => observed.state_observations.push(state),
                    Err(error) => observed
                        .phase
                        .fail("environment_observation_failed", error.to_string()),
                }
            }
            TransactionTarget::Descriptions {
                operation_id,
                transactional_ids,
            } => {
                for (index, transactional_id) in transactional_ids.iter().enumerate() {
                    let observation = match ordinal(first, index) {
                        Ok(observation) => observation,
                        Err(error) => {
                            observed
                                .phase
                                .fail("environment_observation_failed", error.to_string());
                            return observed;
                        }
                    };
                    let output = match self.execute_transaction_cli(
                        vec![
                            "describe".to_owned(),
                            "--transactional-id".to_owned(),
                            transactional_id.clone(),
                        ],
                        &format!("description-{index}"),
                        deadline,
                    ) {
                        Ok(output) => output,
                        Err(error) => {
                            observed.phase.fail(error.code, error.diagnostic);
                            return observed;
                        }
                    };
                    let stdout = output.stdout.clone();
                    if !observed.phase.retain(output) {
                        observed.phase.fail(
                            "environment_observation_failed",
                            "Kafka CLI transaction description failed",
                        );
                        return observed;
                    }
                    match crate::transaction_cli_observation::normalize_description(
                        observation,
                        operation_id,
                        transactional_id,
                        &stdout,
                    ) {
                        Ok(state) => observed.state_observations.push(state),
                        Err(error) => {
                            observed
                                .phase
                                .fail("environment_observation_failed", error.to_string());
                            return observed;
                        }
                    }
                    if remaining(deadline).is_zero() && index + 1 < transactional_ids.len() {
                        observed.phase.fail(
                            "environment_observation_failed",
                            "transaction description observation deadline elapsed",
                        );
                        return observed;
                    }
                }
            }
        }
        observed
    }

    fn execute_transaction_cli(
        &mut self,
        selection: Vec<String>,
        artifact: &str,
        deadline: Instant,
    ) -> Result<TerminalOutput, ComposeFailure> {
        let service = self.broker_services.first().cloned().ok_or_else(|| {
            ComposeFailure::new(
                "environment_observation_failed",
                "no broker service for transaction query",
            )
        })?;
        let operation = self.next_operation;
        let mut args = vec![
            "exec".to_owned(),
            "--no-TTY".to_owned(),
            service,
            "/opt/kafka/bin/kafka-transactions.sh".to_owned(),
            "--bootstrap-server".to_owned(),
            format!("localhost:{}", self.client_port),
        ];
        args.extend(selection);
        self.execute(
            compose_owned(
                EnvironmentOperationKind::BrokerObserve,
                &self.prefix,
                args,
                format!("transaction-{artifact}-{operation:05}.txt"),
                format!("transaction-{artifact}-{operation:05}.stderr.txt"),
            ),
            remaining(deadline),
        )
    }
}
