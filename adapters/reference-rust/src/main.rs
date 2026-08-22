//! Reference adapter executable keeps process startup intentionally tiny.

use std::process::ExitCode;

fn main() -> ExitCode {
    match testlab_reference_adapter::run_stdio() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
