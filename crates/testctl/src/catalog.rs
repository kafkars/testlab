//! Catalog loading validates every manifest before a run identity is created.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use testlab_schema::{ContractRegistry, Scenario, ScenarioPack, SubjectManifest};
use testlab_verifier::known_contract_ids;

use crate::run_error::AppError;

const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct Repository {
    root: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CatalogSummary {
    pub(crate) scenarios: usize,
    pub(crate) packs: usize,
    pub(crate) subjects: usize,
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

    pub(crate) fn validate_all(&self) -> Result<CatalogSummary, AppError> {
        let scenario_paths = collect_toml(&self.root.join("scenarios"))?;
        let pack_paths = collect_toml(&self.root.join("packs"))?;
        let subject_paths = collect_toml(&self.root.join("subjects"))?;
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
        let contracts = self.validate_contracts()?;
        Ok(CatalogSummary {
            scenarios: scenarios.len(),
            packs: packs.len(),
            subjects: subjects.len(),
            contracts,
        })
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

fn read_toml<T: DeserializeOwned>(path: &Path) -> Result<T, AppError> {
    let metadata = fs::metadata(path)
        .map_err(|error| AppError::io(format!("failed to inspect {}", path.display()), error))?;
    if metadata.len() > MAX_MANIFEST_BYTES {
        return Err(AppError::Catalog(format!(
            "manifest exceeds {MAX_MANIFEST_BYTES} bytes: {}",
            path.display()
        )));
    }
    let source = fs::read_to_string(path)
        .map_err(|error| AppError::io(format!("failed to read {}", path.display()), error))?;
    toml::from_str(&source).map_err(|source| AppError::Toml {
        path: path.to_path_buf(),
        source,
    })
}

fn collect_toml(root: &Path) -> Result<Vec<PathBuf>, AppError> {
    let mut paths = Vec::new();
    collect_toml_into(root, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_toml_into(root: &Path, paths: &mut Vec<PathBuf>) -> Result<(), AppError> {
    let entries = fs::read_dir(root)
        .map_err(|error| AppError::io(format!("failed to list {}", root.display()), error))?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            AppError::io(format!("failed to read entry in {}", root.display()), error)
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            AppError::io(format!("failed to inspect {}", path.display()), error)
        })?;
        if file_type.is_dir() {
            collect_toml_into(&path, paths)?;
        } else if file_type.is_file() && path.extension() == Some(std::ffi::OsStr::new("toml")) {
            paths.push(path);
        }
    }
    Ok(())
}

fn insert_unique(
    values: &mut BTreeMap<String, PathBuf>,
    id: &str,
    path: &Path,
    kind: &str,
) -> Result<(), AppError> {
    if let Some(existing) = values.insert(id.to_owned(), path.to_path_buf()) {
        return Err(AppError::Catalog(format!(
            "duplicate {kind} {id} in {} and {}",
            existing.display(),
            path.display()
        )));
    }
    Ok(())
}
