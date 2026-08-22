//! Testctl owns scenario execution, subject supervision, and sealed evidence.

mod app;
mod candidate;
mod candidate_manifest;
mod catalog;
mod catalog_io;
mod evidence;
mod evidence_io;
mod identity;
mod process;
mod process_io;
mod protocol_session;
mod qualification;
mod qualification_evidence;
mod recorder;
mod run_error;
mod runner;
mod runner_environment;
mod runner_protocol;
mod runner_protocol_family;
mod session;
mod session_command;
mod time;

pub use app::run_cli;
pub use run_error::AppError;

#[cfg(test)]
mod candidate_test;
#[cfg(test)]
mod catalog_test;
#[cfg(test)]
mod qualification_test;
#[cfg(test)]
mod recorder_test;
#[cfg(test)]
mod runner_docker_test;
#[cfg(test)]
mod runner_protocol_test;
#[cfg(test)]
mod runner_test;
