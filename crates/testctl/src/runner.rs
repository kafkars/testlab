//! Scenario execution keeps adapter claims, broker truth, and invalidity distinct.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use testlab_broker::RunningBroker;
use testlab_schema::{
    AdapterDescriptor, BrokerObservation, EnvironmentDriver, EnvironmentManifest, RunId, Scenario,
    SubjectManifest, Verdict,
};

use crate::catalog::Repository;
use crate::evidence::{SealRequest, SealedRun, seal};
use crate::recorder::HistoryRecorder;
use crate::run_error::{AppError, RunFailure};
use crate::session::{SessionRequest, run_adapter_session};
use crate::time::{Deadline, unix_ms};

static RUN_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) fn run_scenario(
    repository: &Repository,
    scenario_path: &Path,
    subject_path: &Path,
    environment_path: &Path,
    evidence_directory: &Path,
) -> Result<SealedRun, AppError> {
    repository.validate_all()?;
    let (scenario_path, scenario) = repository.load_scenario(scenario_path)?;
    let (subject_path, subject) = repository.load_subject(subject_path)?;
    let (environment_path, environment) = repository.load_environment(environment_path)?;
    run_loaded(
        repository,
        LoadedRun {
            scenario_path: &scenario_path,
            scenario: &scenario,
            subject_path: &subject_path,
            subject: &subject,
            environment_path: &environment_path,
            environment: &environment,
            evidence_directory,
        },
    )
}

pub(crate) fn run_pack(
    repository: &Repository,
    pack_path: &Path,
    subject_path: &Path,
    environment_path: &Path,
    evidence_directory: &Path,
) -> Result<Vec<SealedRun>, AppError> {
    repository.validate_all()?;
    let (_, pack) = repository.load_pack(pack_path)?;
    let (subject_path, subject) = repository.load_subject(subject_path)?;
    let (environment_path, environment) = repository.load_environment(environment_path)?;
    let mut runs = Vec::with_capacity(pack.scenarios.len());
    for scenario_member in &pack.scenarios {
        let (scenario_path, scenario) = repository.load_scenario(Path::new(scenario_member))?;
        runs.push(run_loaded(
            repository,
            LoadedRun {
                scenario_path: &scenario_path,
                scenario: &scenario,
                subject_path: &subject_path,
                subject: &subject,
                environment_path: &environment_path,
                environment: &environment,
                evidence_directory,
            },
        )?);
    }
    Ok(runs)
}

#[derive(Clone, Copy, Debug)]
struct LoadedRun<'a> {
    scenario_path: &'a Path,
    scenario: &'a Scenario,
    subject_path: &'a Path,
    subject: &'a SubjectManifest,
    environment_path: &'a Path,
    environment: &'a EnvironmentManifest,
    evidence_directory: &'a Path,
}

fn run_loaded(repository: &Repository, request: LoadedRun<'_>) -> Result<SealedRun, AppError> {
    let LoadedRun {
        scenario_path,
        scenario,
        subject_path,
        subject,
        environment_path,
        environment,
        evidence_directory,
    } = request;
    let started_unix_ms = preflight_time()?;
    let run_id = new_run_id(started_unix_ms)?;
    let deadline = Deadline::after_millis(scenario.timeout_ms)
        .map_err(|error| AppError::Catalog(error.to_string()))?;
    let mut recorder = HistoryRecorder::default();
    let mut adapter = None;
    let mut observations = Vec::new();
    let execution = execute(
        ExecutionRequest {
            repository_root: repository.root(),
            scenario,
            subject,
            environment,
            run_id: &run_id,
            deadline,
        },
        &mut recorder,
        &mut adapter,
        &mut observations,
    );
    let mut failure = execution.err();
    if let Some(error) = &failure {
        if let Err(record_error) = recorder.failure(error.harness_error()) {
            failure = Some(record_error);
        }
    }
    let mut verdict = match (&failure, &adapter) {
        (Some(error), _) => Verdict::invalid(vec![error.violation()]),
        (None, Some(adapter)) => {
            testlab_verifier::verify(scenario, adapter, recorder.entries(), &observations)
        }
        (None, None) => Verdict::invalid(vec![
            RunFailure::harness(
                "adapter_identity_missing",
                "adapter handshake completed without an identity",
            )
            .violation(),
        ]),
    };
    let completed_unix_ms = match unix_ms() {
        Ok(value) => value,
        Err(error) => {
            let _ = recorder.failure(error.harness_error());
            verdict = Verdict::invalid(vec![error.violation()]);
            started_unix_ms
        }
    };
    let history = recorder.into_entries();
    seal(&SealRequest {
        repository_root: repository.root(),
        evidence_directory,
        scenario_path,
        subject_path,
        environment_path,
        run_id: &run_id,
        scenario,
        subject,
        environment,
        adapter: adapter.as_ref(),
        history: &history,
        observations: &observations,
        verdict: &verdict,
        started_unix_ms,
        completed_unix_ms,
    })
}

#[derive(Clone, Copy, Debug)]
struct ExecutionRequest<'a> {
    repository_root: &'a Path,
    scenario: &'a Scenario,
    subject: &'a SubjectManifest,
    environment: &'a EnvironmentManifest,
    run_id: &'a RunId,
    deadline: Deadline,
}

fn execute(
    request: ExecutionRequest<'_>,
    recorder: &mut HistoryRecorder,
    adapter: &mut Option<AdapterDescriptor>,
    observations: &mut Vec<BrokerObservation>,
) -> Result<(), RunFailure> {
    let ExecutionRequest {
        repository_root,
        scenario,
        subject,
        environment,
        run_id,
        deadline,
    } = request;
    if !matches!(&environment.driver, EnvironmentDriver::ModelBroker) {
        return Err(RunFailure::harness(
            "environment_driver_unsupported",
            format!(
                "environment {} requires a driver that testctl does not execute yet",
                environment.id
            ),
        ));
    }
    let broker = RunningBroker::start().map_err(|error| {
        RunFailure::harness(
            "environment_start_failed",
            format!("failed to start model broker: {error}"),
        )
    })?;
    let session_result = run_adapter_session(
        SessionRequest {
            repository_root,
            scenario,
            subject,
            run_id,
            deadline,
            broker: &broker,
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
    let mut recording_result = Ok(());
    if let Ok(snapshot) = &observation_result {
        observations.clone_from(snapshot);
        for observation in snapshot {
            if recording_result.is_ok() {
                recording_result = recorder.observation(observation.clone());
            }
        }
    }
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

fn preflight_time() -> Result<u64, AppError> {
    unix_ms().map_err(|error| AppError::Catalog(error.to_string()))
}

fn new_run_id(started_unix_ms: u64) -> Result<RunId, AppError> {
    let sequence = RUN_SEQUENCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| AppError::Catalog("run identity counter overflowed".to_owned()))?;
    RunId::new(format!(
        "run-{started_unix_ms}-{}-{sequence}",
        std::process::id()
    ))
    .map_err(|error| AppError::Catalog(format!("generated invalid run id: {error}")))
}
