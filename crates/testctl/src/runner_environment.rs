//! Environment execution keeps model and real-cluster lifecycles explicit.

use std::path::Path;
use std::time::Duration;

use testlab_broker::RunningBroker;
use testlab_environment::{
    ComposeArtifact, ComposeFailure, ComposeObservation, ComposePhase, ComposeRequest,
    DockerComposeEnvironment,
};
use testlab_schema::{
    AdapterDescriptor, AdapterSecurity, BrokerObservation, BrokerStateObservation,
    EnvironmentDriver, EnvironmentManifest, RunId, Scenario, SubjectManifest,
};

use crate::recorder::HistoryRecorder;
use crate::run_error::RunFailure;
use crate::session::{SessionEnvironment, SessionRequest, run_adapter_session};
use crate::time::Deadline;

const CLEANUP_RESERVE: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug)]
pub(crate) struct EnvironmentExecutionRequest<'a> {
    pub(crate) repository_root: &'a Path,
    pub(crate) scenario: &'a Scenario,
    pub(crate) subject: &'a SubjectManifest,
    pub(crate) environment: &'a EnvironmentManifest,
    pub(crate) run_id: &'a RunId,
    pub(crate) deadline: Deadline,
}

pub(crate) fn execute_environment(
    request: EnvironmentExecutionRequest<'_>,
    recorder: &mut HistoryRecorder,
    adapter: &mut Option<AdapterDescriptor>,
    observations: &mut Vec<BrokerObservation>,
    state_observations: &mut Vec<BrokerStateObservation>,
    artifacts: &mut Vec<ComposeArtifact>,
) -> Result<(), RunFailure> {
    match &request.environment.driver {
        EnvironmentDriver::ModelBroker => execute_model(request, recorder, adapter, observations),
        EnvironmentDriver::KafkaProtocolAdversary { topic } => {
            crate::runner_adversary::execute(request, topic, recorder, adapter, artifacts)
        }
        EnvironmentDriver::DockerCompose { .. } => execute_compose(
            request,
            recorder,
            adapter,
            observations,
            state_observations,
            artifacts,
        ),
    }
}

fn execute_model(
    request: EnvironmentExecutionRequest<'_>,
    recorder: &mut HistoryRecorder,
    adapter: &mut Option<AdapterDescriptor>,
    observations: &mut Vec<BrokerObservation>,
) -> Result<(), RunFailure> {
    let broker = RunningBroker::start().map_err(|error| {
        RunFailure::harness(
            "environment_start_failed",
            format!("failed to start model broker: {error}"),
        )
    })?;
    let broker_endpoints = [broker.endpoint().to_owned()];
    let session_result = run_adapter_session(
        SessionRequest {
            repository_root: request.repository_root,
            scenario: request.scenario,
            subject: request.subject,
            run_id: request.run_id,
            deadline: request.deadline,
            broker_endpoints: &broker_endpoints,
            security: AdapterSecurity::Plaintext,
            adapter_environment: &[],
            environment: SessionEnvironment::Model(&broker),
        },
        recorder,
        adapter,
    );
    let observation_result = broker.observations().map_err(|error| {
        RunFailure::harness(
            "environment_snapshot_failed",
            format!("failed to read model broker observations: {error}"),
        )
    });
    let recording_result = record_observations(&observation_result, recorder, observations);
    let health_result = broker.failure().map_err(|error| {
        RunFailure::harness(
            "environment_health_failed",
            format!("failed to inspect model broker health: {error}"),
        )
    });
    let shutdown_result = broker.shutdown().map_err(|error| {
        RunFailure::harness(
            "environment_shutdown_failed",
            format!("failed to stop model broker: {error}"),
        )
    });
    session_result?;
    observation_result?;
    recording_result?;
    if let Some(diagnostic) = health_result? {
        return Err(RunFailure::harness("environment_failed", diagnostic));
    }
    shutdown_result
}

fn execute_compose(
    request: EnvironmentExecutionRequest<'_>,
    recorder: &mut HistoryRecorder,
    adapter: &mut Option<AdapterDescriptor>,
    observations: &mut Vec<BrokerObservation>,
    state_observations: &mut Vec<BrokerStateObservation>,
    artifacts: &mut Vec<ComposeArtifact>,
) -> Result<(), RunFailure> {
    let work_deadline = request.deadline.reserving(CLEANUP_RESERVE)?;
    let mut environment = DockerComposeEnvironment::new(ComposeRequest {
        repository_root: request.repository_root,
        environment: request.environment,
        run_id: request.run_id,
        started_unix_ms: crate::time::unix_ms()?,
    })
    .map_err(|error| compose_failure(&error))?;
    let setup = environment.start(work_deadline.remaining()?);
    let setup_result = record_phase(setup, recorder, artifacts);
    let provision = if setup_result.is_ok() {
        environment.provision(
            request.scenario,
            work_deadline.remaining().unwrap_or(Duration::ZERO),
        )
    } else {
        ComposePhase::default()
    };
    let provision_result = record_phase(provision, recorder, artifacts);
    let broker_endpoints = environment.adapter_endpoints();
    let adapter_security = environment.adapter_security();
    let adapter_environment = environment.adapter_environment();
    let session_result = if setup_result.is_ok() && provision_result.is_ok() {
        run_adapter_session(
            SessionRequest {
                repository_root: request.repository_root,
                scenario: request.scenario,
                subject: request.subject,
                run_id: request.run_id,
                deadline: work_deadline,
                broker_endpoints: &broker_endpoints,
                security: adapter_security,
                adapter_environment: &adapter_environment,
                environment: SessionEnvironment::Compose {
                    controller: &mut environment,
                    artifacts,
                },
            },
            recorder,
            adapter,
        )
    } else {
        Ok(())
    };
    let issued_operations = crate::issued_operations::from_history(recorder.entries());
    let observation = if setup_result.is_ok() && provision_result.is_ok() && adapter.is_some() {
        environment.observe(
            request.scenario,
            &issued_operations.record_operations,
            &[],
            work_deadline.remaining().unwrap_or(Duration::ZERO),
        )
    } else {
        ComposeObservation::default()
    };
    let observation_result = record_compose_observation(
        observation,
        recorder,
        observations,
        state_observations,
        artifacts,
    );
    collect_state_observations(recorder, state_observations);
    let proxy_timeout = request
        .deadline
        .remaining()
        .unwrap_or(Duration::ZERO)
        .min(Duration::from_secs(5));
    let proxy = environment.finish_network_proxy(proxy_timeout);
    let proxy_observation_result = proxy
        .observations
        .into_iter()
        .try_for_each(|observation| recorder.network_proxy_observation(observation));
    let proxy_result = record_phase(proxy.phase, recorder, artifacts);
    let cleanup_timeout = request.deadline.remaining().unwrap_or(Duration::ZERO);
    let cleanup = environment.finish(cleanup_timeout);
    let cleanup_result = record_phase(cleanup, recorder, artifacts);
    setup_result?;
    provision_result?;
    session_result?;
    observation_result?;
    proxy_observation_result?;
    proxy_result?;
    cleanup_result
}

fn collect_state_observations(
    recorder: &HistoryRecorder,
    state_observations: &mut Vec<BrokerStateObservation>,
) {
    state_observations.clear();
    state_observations.extend(recorder.entries().iter().filter_map(|entry| {
        let testlab_schema::HistoryPayload::BrokerStateObservation { observation } = &entry.payload
        else {
            return None;
        };
        Some(observation.clone())
    }));
}

fn record_compose_observation(
    snapshot: ComposeObservation,
    recorder: &mut HistoryRecorder,
    observations: &mut Vec<BrokerObservation>,
    state_observations: &mut Vec<BrokerStateObservation>,
    artifacts: &mut Vec<ComposeArtifact>,
) -> Result<(), RunFailure> {
    let ComposeObservation {
        phase,
        observations: captured,
        state_observations: captured_state,
    } = snapshot;
    let phase_result = record_phase(phase, recorder, artifacts);
    for observation in &captured {
        recorder.observation(observation.clone())?;
    }
    observations.clone_from(&captured);
    for observation in &captured_state {
        recorder.state_observation(observation.clone())?;
    }
    state_observations.clone_from(&captured_state);
    phase_result
}

pub(crate) fn record_phase(
    phase: ComposePhase,
    recorder: &mut HistoryRecorder,
    artifacts: &mut Vec<ComposeArtifact>,
) -> Result<(), RunFailure> {
    let ComposePhase {
        operations,
        artifacts: phase_artifacts,
        failure,
    } = phase;
    for operation in operations {
        recorder.environment_operation(operation)?;
    }
    artifacts.extend(phase_artifacts);
    match failure {
        Some(error) => Err(compose_failure(&error)),
        None => Ok(()),
    }
}

fn record_observations(
    snapshot: &Result<Vec<BrokerObservation>, RunFailure>,
    recorder: &mut HistoryRecorder,
    observations: &mut Vec<BrokerObservation>,
) -> Result<(), RunFailure> {
    if let Ok(snapshot) = snapshot {
        observations.clone_from(snapshot);
        for observation in snapshot {
            recorder.observation(observation.clone())?;
        }
    }
    Ok(())
}

fn compose_failure(error: &ComposeFailure) -> RunFailure {
    RunFailure::harness(error.code(), error.diagnostic())
}
