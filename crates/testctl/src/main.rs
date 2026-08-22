//! Command-line entry point for the kafkars testlab runner.

use std::process::ExitCode;

fn main() -> ExitCode {
    match testctl::run_cli() {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(error) => {
            eprintln!("testctl: {error}");
            ExitCode::from(2)
        }
    }
}
