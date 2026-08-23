//! Compose disruption operations restart declared brokers and prove readiness afterward.

use std::thread;
use std::time::{Duration, Instant};

use crate::compose::DockerComposeEnvironment;
use crate::compose_command;
use crate::compose_support::remaining;
use crate::compose_types::ComposePhase;

const READINESS_ATTEMPT_MAX: Duration = Duration::from_secs(5);
const READINESS_RETRY_DELAY: Duration = Duration::from_millis(250);

impl DockerComposeEnvironment {
    /// Restarts one broker by one-based ordinal and waits until its Kafka API responds.
    pub fn restart_broker(&mut self, broker_ordinal: u16, timeout: Duration) -> ComposePhase {
        let mut phase = ComposePhase::default();
        let Some(index) = broker_ordinal
            .checked_sub(1)
            .map(usize::from)
            .filter(|index| *index < self.broker_services.len())
        else {
            phase.fail(
                "environment_broker_target_invalid",
                format!("broker ordinal {broker_ordinal} is not declared"),
            );
            return phase;
        };
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            phase.fail(
                "environment_disruption_deadline_invalid",
                "broker restart deadline overflowed",
            );
            return phase;
        };
        let service = self.broker_services[index].clone();
        let operation = self.next_operation;
        if !self.required(
            &mut phase,
            compose_command::restart(&self.prefix, &service, operation),
            deadline,
        ) {
            return phase;
        }
        self.wait_restart_ready(&mut phase, &service, operation, deadline);
        phase
    }

    fn wait_restart_ready(
        &mut self,
        phase: &mut ComposePhase,
        service: &str,
        operation: u32,
        deadline: Instant,
    ) {
        let mut attempt = 1_u32;
        loop {
            let spec = compose_command::restart_readiness(
                &self.prefix,
                service,
                self.client_port,
                operation,
                attempt,
            );
            match self.execute(spec, remaining(deadline).min(READINESS_ATTEMPT_MAX)) {
                Ok(output) => {
                    if phase.retain(output) {
                        return;
                    }
                }
                Err(error) => {
                    phase.fail(error.code, error.diagnostic);
                    return;
                }
            }
            if remaining(deadline).is_zero() {
                phase.fail(
                    "environment_broker_restart_failed",
                    format!("broker service {service} did not recover after restart"),
                );
                return;
            }
            thread::sleep(READINESS_RETRY_DELAY.min(remaining(deadline)));
            let Some(next) = attempt.checked_add(1) else {
                phase.fail(
                    "environment_operation_overflow",
                    "restart readiness attempt overflowed",
                );
                return;
            };
            attempt = next;
        }
    }
}
