//! Broker readiness owns bounded retries before feature and security setup.

use std::thread;
use std::time::{Duration, Instant};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command;
use crate::compose_support::remaining;
use crate::compose_types::ComposePhase;

const READINESS_ATTEMPT_MAX: Duration = Duration::from_secs(5);
const READINESS_RETRY_DELAY: Duration = Duration::from_millis(250);

impl DockerComposeEnvironment {
    pub(super) fn wait_ready(
        &mut self,
        phase: &mut ComposePhase,
        service: &str,
        deadline: Instant,
    ) -> bool {
        let mut attempt = 1_u32;
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
}
