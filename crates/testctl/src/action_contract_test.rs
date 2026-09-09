//! Composite-action tests pin fail-closed native dependency installation.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

const ROOT_ACTION: &str = include_str!("../../../action.yml");
const SETUP_ACTION: &str = include_str!("../../../.github/actions/setup-rust/action.yml");
const INSTALLER: &str = include_str!("../../../scripts/install-native-build-dependencies");

static FIXTURE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn composite_actions_share_the_scoped_native_dependency_installer() {
    assert!(ROOT_ACTION.contains("$GITHUB_ACTION_PATH/scripts/install-native-build-dependencies"));
    assert!(
        SETUP_ACTION
            .contains("$GITHUB_ACTION_PATH/../../../scripts/install-native-build-dependencies")
    );
    assert!(!ROOT_ACTION.contains("sudo apt-get update"));
    assert!(!SETUP_ACTION.contains("sudo apt-get update"));
    assert!(INSTALLER.contains("Dir::Etc::sourceparts=-"));
    assert!(INSTALLER.contains("APT::Get::List-Cleanup=0"));
}

#[test]
fn installer_skips_apt_when_the_package_is_already_installed() {
    let fixture = fixture_directory();
    let _cleanup = Cleanup(fixture.clone());
    must(fs::create_dir_all(&fixture), "create installer fixture");
    let calls = fixture.join("sudo-calls");

    let output = run_installer(
        &fixture,
        &calls,
        "dpkg-query() { printf '%s' 'install ok installed'; }",
    );

    assert!(output.status.success(), "{}", output_text(&output));
    assert!(!calls.exists());
}

#[test]
fn installer_excludes_unrelated_apt_sources() {
    let fixture = fixture_directory();
    let _cleanup = Cleanup(fixture.clone());
    let source_parts = fixture.join("sources.list.d");
    must(
        fs::create_dir_all(&source_parts),
        "create apt source fixture",
    );
    let ubuntu_sources = source_parts.join("ubuntu.sources");
    must(
        fs::write(&ubuntu_sources, "Types: deb\n"),
        "write Ubuntu source",
    );
    must(
        fs::write(source_parts.join("google-chrome.list"), "unrelated\n"),
        "write unrelated source",
    );
    let calls = fixture.join("sudo-calls");

    let output = run_installer(&fixture, &calls, "dpkg-query() { return 1; }");

    assert!(output.status.success(), "{}", output_text(&output));
    let calls = must(fs::read_to_string(calls), "read sudo calls");
    let source_option = format!("<Dir::Etc::sourcelist={}>", ubuntu_sources.display());
    let calls = calls.lines().collect::<Vec<_>>();
    assert_eq!(calls.len(), 2);
    assert!(calls.iter().all(|call| call.contains(&source_option)));
    assert!(
        calls
            .iter()
            .all(|call| call.contains("<Dir::Etc::sourceparts=->"))
    );
    assert!(
        calls
            .iter()
            .all(|call| call.contains("<APT::Get::List-Cleanup=0>"))
    );
    assert!(calls[0].ends_with("<update>"));
    assert!(calls[1].contains("<install><--yes><--no-install-recommends><libcurl4-openssl-dev>"));
    assert!(calls.iter().all(|call| !call.contains("google-chrome")));
}

fn run_installer(apt_etc: &Path, calls: &Path, dpkg_query: &str) -> std::process::Output {
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/install-native-build-dependencies");
    let shell = format!(
        r#"
source "$1"
call_log="$3"
{dpkg_query}
apt-get() {{ :; }}
sudo() {{
  for argument in "$@"; do
    printf '<%s>' "$argument" >> "$call_log"
  done
  printf '\n' >> "$call_log"
}}
install_native_build_dependencies "$2"
"#
    );
    must(
        Command::new("bash")
            .args(["-c", &shell, "test", path_text(&script), path_text(apt_etc)])
            .arg(calls)
            .output(),
        "run native dependency installer",
    )
}

fn fixture_directory() -> PathBuf {
    let sequence = FIXTURE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "testlab-action-contract-test-{}-{}",
        std::process::id(),
        sequence
    ))
}

fn path_text(path: &Path) -> &str {
    path.to_str()
        .unwrap_or_else(|| panic!("non-UTF-8 test path: {}", path.display()))
}

fn output_text(output: &std::process::Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

struct Cleanup(PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn must<T, E: std::fmt::Display>(result: Result<T, E>, context: &str) -> T {
    result.unwrap_or_else(|error| panic!("{context}: {error}"))
}
