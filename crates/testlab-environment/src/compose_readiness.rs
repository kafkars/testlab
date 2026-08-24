//! Broker readiness owns bounded retries before feature and security setup.

use std::thread;
use std::time::{Duration, Instant};

use testlab_schema::EnvironmentOperationKind;

use crate::compose::DockerComposeEnvironment;
use crate::compose_command::{self, CommandSpec};
use crate::compose_support::remaining;
use crate::compose_types::ComposePhase;

const READINESS_ATTEMPT_MAX: Duration = Duration::from_secs(5);
const READINESS_RETRY_DELAY: Duration = Duration::from_millis(250);
const STARTUP_RECOVERY_LIMIT: u8 = 1;

impl DockerComposeEnvironment {
    pub(super) fn wait_ready(
        &mut self,
        phase: &mut ComposePhase,
        service: &str,
        deadline: Instant,
    ) -> bool {
        let mut attempt = 1_u32;
        let mut recovery_attempts = 0_u8;
        loop {
            let timeout = remaining(deadline).min(READINESS_ATTEMPT_MAX);
            let spec = compose_command::readiness(&self.prefix, service, self.client_port, attempt);
            match self.execute(spec, timeout) {
                Ok(output) => {
                    if phase.retain(output) {
                        return true;
                    }
                }
                Err(error) => {
                    phase.fail(error.code, error.diagnostic);
                    return false;
                }
            }
            if remaining(deadline).is_zero() {
                phase.fail(
                    "environment_readiness_failed",
                    format!("broker service {service} did not become ready"),
                );
                return false;
            }
            if !self.recover_exited_startup(
                phase,
                service,
                attempt,
                &mut recovery_attempts,
                deadline,
            ) {
                return false;
            }
            thread::sleep(READINESS_RETRY_DELAY.min(remaining(deadline)));
            attempt = if let Some(value) = attempt.checked_add(1) {
                value
            } else {
                phase.fail(
                    "environment_operation_overflow",
                    "readiness attempt overflowed",
                );
                return false;
            };
        }
    }

    fn recover_exited_startup(
        &mut self,
        phase: &mut ComposePhase,
        service: &str,
        readiness_attempt: u32,
        recovery_attempts: &mut u8,
        deadline: Instant,
    ) -> bool {
        let state = match self.execute(
            exited_service(&self.prefix, service, readiness_attempt),
            remaining(deadline),
        ) {
            Ok(output) => output,
            Err(error) => {
                phase.fail(error.code, error.diagnostic);
                return false;
            }
        };
        let exited = exited_service_output(&state.stdout, service);
        if !phase.retain(state) {
            phase.fail(
                "environment_broker_state_failed",
                format!("could not inspect broker service {service}"),
            );
            return false;
        }
        let Ok(exited) = exited else {
            phase.fail(
                "environment_broker_state_invalid",
                format!("broker service {service} returned an invalid state"),
            );
            return false;
        };
        if !exited {
            return true;
        }
        if *recovery_attempts >= STARTUP_RECOVERY_LIMIT {
            phase.fail(
                "environment_broker_startup_recovery_exhausted",
                format!("broker service {service} exited after bounded startup recovery"),
            );
            return false;
        }
        *recovery_attempts += 1;
        self.required(
            phase,
            startup_logs(&self.prefix, service, *recovery_attempts),
            deadline,
        ) && self.required(
            phase,
            startup_recovery(&self.prefix, service, *recovery_attempts),
            deadline,
        )
    }
}

fn exited_service(prefix: &[String], service: &str, attempt: u32) -> CommandSpec {
    compose_command::compose_owned(
        EnvironmentOperationKind::ComposePs,
        prefix,
        vec![
            "ps".to_owned(),
            "--all".to_owned(),
            "--status".to_owned(),
            "exited".to_owned(),
            "--services".to_owned(),
            service.to_owned(),
        ],
        format!("startup-state-{service}-{attempt:03}.txt"),
        format!("startup-state-{service}-{attempt:03}.stderr.txt"),
    )
}

fn startup_logs(prefix: &[String], service: &str, attempt: u8) -> CommandSpec {
    compose_command::compose_owned(
        EnvironmentOperationKind::ComposeLogs,
        prefix,
        vec![
            "logs".to_owned(),
            "--no-color".to_owned(),
            "--timestamps".to_owned(),
            service.to_owned(),
        ],
        format!("startup-failure-{service}-{attempt:03}.log"),
        format!("startup-failure-{service}-{attempt:03}.stderr.txt"),
    )
}

fn startup_recovery(prefix: &[String], service: &str, attempt: u8) -> CommandSpec {
    compose_command::compose_owned(
        EnvironmentOperationKind::BrokerStart,
        prefix,
        vec!["start".to_owned(), service.to_owned()],
        format!("startup-recovery-{service}-{attempt:03}.txt"),
        format!("startup-recovery-{service}-{attempt:03}.stderr.txt"),
    )
}

fn exited_service_output(output: &[u8], service: &str) -> Result<bool, ()> {
    let output = std::str::from_utf8(output).map_err(|_| ())?.trim();
    if output.is_empty() {
        Ok(false)
    } else if output == service {
        Ok(true)
    } else {
        Err(())
    }
}
