//! Catalog filesystem helpers enforce bounded, deterministic manifest discovery.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use crate::run_error::AppError;

const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;

pub(crate) fn read_toml<T: DeserializeOwned>(path: &Path) -> Result<T, AppError> {
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

pub(crate) fn collect_toml(root: &Path) -> Result<Vec<PathBuf>, AppError> {
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

pub(crate) fn insert_unique(
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
