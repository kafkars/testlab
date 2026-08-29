//! Protocol-adversary execution seals worker evidence around one adapter session.

use std::time::Duration;

use testlab_environment::{AdversaryProcessRequest, ComposeArtifact, RunningAdversary};
use testlab_schema::{AdapterDescriptor, AdapterSecurity, EnvironmentOperationId};

use crate::recorder::HistoryRecorder;
use crate::run_error::RunFailure;
use crate::runner_environment::{EnvironmentExecutionRequest, record_phase};
use crate::session::{SessionEnvironment, SessionRequest, run_adapter_session};

pub(crate) fn execute(
    request: EnvironmentExecutionRequest<'_>,
    topic: &str,
    recorder: &mut HistoryRecorder,
    adapter: &mut Option<AdapterDescriptor>,
    artifacts: &mut Vec<ComposeArtifact>,
) -> Result<(), RunFailure> {
    let work_deadline = request.deadline.reserving(Duration::from_secs(5))?;
    let program = std::env::current_exe().map_err(|error| {
        RunFailure::harness(
            "environment_program_missing",
            format!("failed to locate testctl executable: {error}"),
        )
    })?;
    let operation_id = EnvironmentOperationId::new(format!("{}:environment:00000", request.run_id))
        .map_err(|error| {
            RunFailure::harness("environment_operation_id_invalid", error.to_string())
        })?;
    let start = RunningAdversary::start(
        AdversaryProcessRequest {
            program: &program,
            repository_root: request.repository_root,
            topic,
            operation_id,
            started_unix_ms: crate::time::unix_ms()?,
        },
        work_deadline.remaining()?.min(Duration::from_secs(5)),
    );
    let mut environment = match start {
        Ok(environment) => environment,
        Err(phase) => return record_phase(phase, recorder, artifacts),
    };
    let broker_endpoints = [environment.endpoint().to_owned()];
    let session_result = run_adapter_session(
        SessionRequest {
            repository_root: request.repository_root,
            scenario: request.scenario,
            subject: request.subject,
            run_id: request.run_id,
            deadline: work_deadline,
            broker_endpoints: &broker_endpoints,
            security: AdapterSecurity::Plaintext,
            adapter_environment: &[],
            environment: SessionEnvironment::Adversary(&mut environment),
        },
        recorder,
        adapter,
    );
    let phase = environment.finish(request.deadline.remaining().unwrap_or(Duration::ZERO));
    for observation in environment.take_observations() {
        recorder.adversary_observation(observation)?;
    }
    let finish_result = record_phase(phase, recorder, artifacts);
    session_result?;
    finish_result
}
