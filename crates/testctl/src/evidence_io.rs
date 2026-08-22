//! Evidence filesystem helpers own durable writes, digests, and permissions.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::run_error::AppError;

pub(crate) fn write_json<T: serde::Serialize + ?Sized>(
    directory: &Path,
    name: &str,
    value: &T,
) -> Result<(), AppError> {
    let mut bytes =
        serde_json::to_vec_pretty(value).map_err(|error| AppError::json(name, error))?;
    bytes.push(b'\n');
    write_bytes(directory, name, &bytes, false)
}

pub(crate) fn write_json_lines<T: serde::Serialize>(
    directory: &Path,
    name: &str,
    values: &[T],
) -> Result<(), AppError> {
    let path = directory.join(name);
    let mut file = create_new(&path)?;
    for value in values {
        serde_json::to_writer(&mut file, value).map_err(|error| AppError::json(name, error))?;
        file.write_all(b"\n")
            .map_err(|error| AppError::io(format!("failed to write {}", path.display()), error))?;
    }
    file.sync_all()
        .map_err(|error| AppError::io(format!("failed to sync {}", path.display()), error))
}

pub(crate) fn write_bytes(
    directory: &Path,
    name: &str,
    bytes: &[u8],
    executable: bool,
) -> Result<(), AppError> {
    let path = directory.join(name);
    let mut file = create_new(&path)?;
    file.write_all(bytes)
        .map_err(|error| AppError::io(format!("failed to write {}", path.display()), error))?;
    file.sync_all()
        .map_err(|error| AppError::io(format!("failed to sync {}", path.display()), error))?;
    if executable {
        make_executable(&path)?;
    }
    Ok(())
}

pub(crate) fn digest_directory(directory: &Path) -> Result<BTreeMap<String, String>, AppError> {
    let mut paths = fs::read_dir(directory)
        .map_err(|error| AppError::io(format!("failed to list {}", directory.display()), error))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| AppError::io("failed to inspect evidence files", error))?;
    paths.sort();
    let mut digests = BTreeMap::new();
    for path in paths {
        if path.file_name() == Some(std::ffi::OsStr::new("digests.json")) {
            continue;
        }
        let name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| AppError::Evidence("non-UTF-8 evidence name".to_owned()))?;
        if digests
            .insert(name.to_owned(), digest_file(&path)?)
            .is_some()
        {
            return Err(AppError::Evidence(format!(
                "duplicate evidence artifact name {name}"
            )));
        }
    }
    Ok(digests)
}

pub(crate) fn digest_tree(directory: &Path) -> Result<BTreeMap<String, String>, AppError> {
    let mut files = Vec::new();
    collect_files(directory, &mut files)?;
    files.sort();
    let mut digests = BTreeMap::new();
    for path in files {
        let relative = path.strip_prefix(directory).map_err(|error| {
            AppError::Evidence(format!("failed to relativize {}: {error}", path.display()))
        })?;
        if relative == Path::new("digests.json") {
            continue;
        }
        let name = portable_path(relative)?;
        digests.insert(name, digest_file(&path)?);
    }
    Ok(digests)
}

pub(crate) fn sync_tree_directories(directory: &Path) -> Result<(), AppError> {
    let mut directories = fs::read_dir(directory)
        .map_err(|error| AppError::io(format!("failed to list {}", directory.display()), error))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    directories.sort();
    for child in directories {
        sync_tree_directories(&child)?;
    }
    sync_directory(directory)
}

fn collect_files(directory: &Path, files: &mut Vec<std::path::PathBuf>) -> Result<(), AppError> {
    let mut entries = fs::read_dir(directory)
        .map_err(|error| AppError::io(format!("failed to list {}", directory.display()), error))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| AppError::io("failed to inspect qualification evidence", error))?;
    entries.sort();
    for path in entries {
        if path.is_dir() {
            collect_files(&path, files)?;
        } else if path.is_file() {
            files.push(path);
        } else {
            return Err(AppError::Evidence(format!(
                "unsupported qualification evidence entry {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn portable_path(path: &Path) -> Result<String, AppError> {
    let mut components = Vec::new();
    for component in path.components() {
        let std::path::Component::Normal(value) = component else {
            return Err(AppError::Evidence(format!(
                "non-portable qualification evidence path {}",
                path.display()
            )));
        };
        components.push(value.to_str().ok_or_else(|| {
            AppError::Evidence(format!("non-UTF-8 evidence path {}", path.display()))
        })?);
    }
    Ok(components.join("/"))
}

fn create_new(path: &Path) -> Result<File, AppError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| AppError::io(format!("failed to create {}", path.display()), error))
}

fn digest_file(path: &Path) -> Result<String, AppError> {
    let mut file = File::open(path)
        .map_err(|error| AppError::io(format!("failed to open {}", path.display()), error))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let bytes = file
            .read(&mut buffer)
            .map_err(|error| AppError::io(format!("failed to hash {}", path.display()), error))?;
        if bytes == 0 {
            break;
        }
        digest.update(&buffer[..bytes]);
    }
    Ok(hex::encode(digest.finalize()))
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<(), AppError> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .map_err(|error| AppError::io("failed to inspect reproduction script", error))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .map_err(|error| AppError::io("failed to mark reproduction script executable", error))
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

#[cfg(unix)]
pub(crate) fn sync_directory(path: &Path) -> Result<(), AppError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| AppError::io(format!("failed to sync {}", path.display()), error))
}

#[cfg(not(unix))]
pub(crate) fn sync_directory(_path: &Path) -> Result<(), AppError> {
    Ok(())
}
