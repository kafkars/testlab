//! Candidate adapter builds opt into public surfaces newer than the baseline dependency.

use std::path::Path;
use std::process::Command;

pub(crate) fn build_command(manifest: &Path, target: &Path) -> Command {
    let mut command = Command::new("cargo");
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest)
        .arg("--locked")
        .arg("--target-dir")
        .arg(target)
        .env(
            "RUSTFLAGS",
            "--cfg kafkars_share_candidate --cfg kafkars_independent_handles_candidate",
        );
    command
}
