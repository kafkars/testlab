//! Scenario execution keeps adapter claims, broker truth, and invalidity distinct.

use std::path::Path;

use testlab_environment::ComposeArtifact;
use testlab_schema::{EnvironmentManifest, Scenario, SubjectManifest, Verdict};

use crate::catalog::Repository;
use crate::evidence::{SealRequest, SealedRun, seal};
use crate::identity::new_run_id;
use crate::recorder::HistoryRecorder;
use crate::run_error::{AppError, RunFailure};
use crate::runner_environment::{EnvironmentExecutionRequest, execute_environment};
use crate::time::{Deadline, unix_ms};

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
    let run_id = new_run_id("run", started_unix_ms)?;
    let deadline = Deadline::after_millis(scenario.timeout_ms)
        .map_err(|error| AppError::Catalog(error.to_string()))?;
    let mut recorder = HistoryRecorder::default();
    let mut adapter = None;
    let mut observations = Vec::new();
    let mut environment_artifacts = Vec::<ComposeArtifact>::new();
    let execution = execute_environment(
        EnvironmentExecutionRequest {
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
        &mut environment_artifacts,
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
        environment_artifacts: &environment_artifacts,
        verdict: &verdict,
        started_unix_ms,
        completed_unix_ms,
    })
}

fn preflight_time() -> Result<u64, AppError> {
    unix_ms().map_err(|error| AppError::Catalog(error.to_string()))
}
