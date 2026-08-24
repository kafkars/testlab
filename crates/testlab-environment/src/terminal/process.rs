//! Bounded child-process waiting and whole-process-tree termination.

use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(5);

#[derive(Debug)]
pub(super) enum WaitResult {
    Exited(ExitStatus),
    Failed(String),
    TimedOut(Option<ExitStatus>, Option<String>),
}

pub(super) fn wait_for_child(
    child: &mut std::process::Child,
    timeout: Duration,
    started: Instant,
) -> WaitResult {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return WaitResult::Exited(status),
            Ok(None) if started.elapsed() < timeout => {
                thread::sleep(POLL_INTERVAL.min(timeout.saturating_sub(started.elapsed())));
            }
            Ok(None) => {
                let kill_error = terminate_child_tree(child).err();
                let status = child.wait().ok();
                return WaitResult::TimedOut(status, kill_error);
            }
            Err(error) => return WaitResult::Failed(error.to_string()),
        }
    }
}

#[cfg(unix)]
fn terminate_child_tree(child: &mut std::process::Child) -> Result<(), String> {
    let process_group = format!("-{}", child.id());
    let status = Command::new("kill")
        .args(["-KILL", "--", &process_group])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        child.kill().map_err(|error| {
            format!("process-group termination failed with {status}; child kill failed: {error}")
        })
    }
}

#[cfg(not(unix))]
fn terminate_child_tree(child: &mut std::process::Child) -> Result<(), String> {
    child.kill().map_err(|error| error.to_string())
}
