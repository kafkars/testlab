//! Catalog loading validates every manifest before a run identity is created.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use testlab_schema::{
    ContractRegistry, EnvironmentDriver, EnvironmentManifest, QualificationManifest, Scenario,
    ScenarioPack, SubjectManifest,
};
use testlab_verifier::known_contract_ids;

use crate::catalog_io::{collect_toml, insert_unique, read_toml};
use crate::run_error::AppError;

#[derive(Clone, Debug)]
pub(crate) struct Repository {
    root: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CatalogSummary {
    pub(crate) scenarios: usize,
    pub(crate) packs: usize,
    pub(crate) subjects: usize,
    pub(crate) environments: usize,
    pub(crate) qualifications: usize,
    pub(crate) contracts: usize,
}

impl Repository {
    pub(crate) fn open(root: &Path) -> Result<Self, AppError> {
        let root = fs::canonicalize(root).map_err(|error| {
            AppError::io(
                format!("failed to resolve repository root {}", root.display()),
                error,
            )
        })?;
        if !root.is_dir() {
            return Err(AppError::Catalog(format!(
                "repository root is not a directory: {}",
                root.display()
            )));
        }
        Ok(Self { root })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn load_scenario(&self, path: &Path) -> Result<(PathBuf, Scenario), AppError> {
        let path = self.resolve_existing(path)?;
        let scenario: Scenario = read_toml(&path)?;
        scenario
            .validate()
            .map_err(|error| AppError::Catalog(format!("{}: {error}", path.display())))?;
        Ok((self.relative(&path)?, scenario))
    }

    pub(crate) fn load_pack(&self, path: &Path) -> Result<(PathBuf, ScenarioPack), AppError> {
        let path = self.resolve_existing(path)?;
        let pack: ScenarioPack = read_toml(&path)?;
        pack.validate()
            .map_err(|error| AppError::Catalog(format!("{}: {error}", path.display())))?;
        Ok((self.relative(&path)?, pack))
    }

    pub(crate) fn load_subject(&self, path: &Path) -> Result<(PathBuf, SubjectManifest), AppError> {
        let path = self.resolve_existing(path)?;
        let subject: SubjectManifest = read_toml(&path)?;
        subject
            .validate()
            .map_err(|error| AppError::Catalog(format!("{}: {error}", path.display())))?;
        Ok((self.relative(&path)?, subject))
    }

    pub(crate) fn load_environment(
        &self,
        path: &Path,
    ) -> Result<(PathBuf, EnvironmentManifest), AppError> {
        let path = self.resolve_existing(path)?;
        let environment: EnvironmentManifest = read_toml(&path)?;
        environment
            .validate()
            .map_err(|error| AppError::Catalog(format!("{}: {error}", path.display())))?;
        self.validate_environment_files(&environment)?;
        Ok((self.relative(&path)?, environment))
    }

    pub(crate) fn load_qualification(
        &self,
        path: &Path,
    ) -> Result<(PathBuf, QualificationManifest), AppError> {
        let path = self.resolve_existing(path)?;
        let qualification: QualificationManifest = read_toml(&path)?;
        qualification
            .validate()
            .map_err(|error| AppError::Catalog(format!("{}: {error}", path.display())))?;
        for cell in &qualification.cells {
            self.load_environment(Path::new(&cell.environment))?;
            self.load_pack(Path::new(&cell.pack))?;
        }
        Ok((self.relative(&path)?, qualification))
    }

    pub(crate) fn validate_all(&self) -> Result<CatalogSummary, AppError> {
        let scenario_paths = collect_toml(&self.root.join("scenarios"))?;
        let pack_paths = collect_toml(&self.root.join("packs"))?;
        let subject_paths = collect_toml(&self.root.join("subjects"))?;
        let environment_paths = collect_toml(&self.root.join("clusters"))?;
        let qualification_paths = collect_toml(&self.root.join("qualifications"))?;
        let mut scenarios = BTreeMap::new();
        for path in &scenario_paths {
            let (relative, scenario) = self.load_scenario(path)?;
            insert_unique(
                &mut scenarios,
                scenario.id.as_str(),
                &relative,
                "scenario id",
            )?;
        }
        let mut packs = BTreeMap::new();
        for path in &pack_paths {
            let (relative, pack) = self.load_pack(path)?;
            insert_unique(&mut packs, pack.id.as_str(), &relative, "pack id")?;
            for scenario in &pack.scenarios {
                self.load_scenario(Path::new(scenario))?;
            }
        }
        let mut subjects = BTreeMap::new();
        for path in &subject_paths {
            let (relative, subject) = self.load_subject(path)?;
            insert_unique(&mut subjects, subject.id.as_str(), &relative, "subject id")?;
        }
        let mut environments = BTreeMap::new();
        for path in &environment_paths {
            let (relative, environment) = self.load_environment(path)?;
            insert_unique(
                &mut environments,
                environment.id.as_str(),
                &relative,
                "environment id",
            )?;
        }
        let mut qualifications = BTreeMap::new();
        for path in &qualification_paths {
            let (relative, qualification) = self.load_qualification(path)?;
            insert_unique(
                &mut qualifications,
                qualification.id.as_str(),
                &relative,
                "qualification id",
            )?;
        }
        let contracts = self.validate_contracts()?;
        Ok(CatalogSummary {
            scenarios: scenarios.len(),
            packs: packs.len(),
            subjects: subjects.len(),
            environments: environments.len(),
            qualifications: qualifications.len(),
            contracts,
        })
    }

    fn validate_environment_files(
        &self,
        environment: &EnvironmentManifest,
    ) -> Result<(), AppError> {
        let EnvironmentDriver::DockerCompose { compose_files, .. } = &environment.driver else {
            return Ok(());
        };
        for compose_file in compose_files {
            let path = self.resolve_existing(Path::new(compose_file))?;
            if !path.is_file() {
                return Err(AppError::Catalog(format!(
                    "environment {} Compose path is not a file: {}",
                    environment.id,
                    path.display()
                )));
            }
        }
        Ok(())
    }

    fn validate_contracts(&self) -> Result<usize, AppError> {
        let path = self.resolve_existing(Path::new("contracts/conformance.toml"))?;
        let registry: ContractRegistry = read_toml(&path)?;
        registry
            .validate()
            .map_err(|error| AppError::Catalog(format!("{}: {error}", path.display())))?;
        let registered = registry
            .contracts
            .iter()
            .map(|contract| contract.id.as_str())
            .collect::<BTreeSet<_>>();
        let implemented = known_contract_ids()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if registered != implemented {
            return Err(AppError::Catalog(format!(
                "contract registry differs from verifier; registered={registered:?}, implemented={implemented:?}"
            )));
        }
        Ok(registered.len())
    }

    fn resolve_existing(&self, path: &Path) -> Result<PathBuf, AppError> {
        let candidate = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.root.join(path)
        };
        let resolved = fs::canonicalize(&candidate).map_err(|error| {
            AppError::io(format!("failed to resolve {}", candidate.display()), error)
        })?;
        if !resolved.starts_with(&self.root) {
            return Err(AppError::Catalog(format!(
                "path escapes repository root: {}",
                path.display()
            )));
        }
        Ok(resolved)
    }

    fn relative(&self, path: &Path) -> Result<PathBuf, AppError> {
        path.strip_prefix(&self.root)
            .map(Path::to_path_buf)
            .map_err(|error| {
                AppError::Catalog(format!(
                    "{} is outside {}: {error}",
                    path.display(),
                    self.root.display()
                ))
            })
    }
}
