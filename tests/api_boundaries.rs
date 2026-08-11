use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

struct CompileFixture {
    root: PathBuf,
}

impl CompileFixture {
    fn new(name: &str, dependency_feature: &str, source: &str) -> Self {
        Self::new_with_features(name, &[dependency_feature], source)
    }

    fn new_with_features(name: &str, dependency_features: &[&str], source: &str) -> Self {
        let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "fccli-api-boundary-{name}-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(root.join("src")).expect("create compile fixture");

        let package_name = format!("fccli-api-boundary-{name}");
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let dependency_path = toml_string(manifest_dir);
        let features = dependency_features
            .iter()
            .map(|feature| format!("{feature:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        let manifest = format!(
            "[package]\nname = {package_name:?}\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n[dependencies]\nfccli = {{ path = {dependency_path}, default-features = false, features = [{features}] }}\n"
        );
        fs::write(root.join("Cargo.toml"), manifest).expect("write compile fixture manifest");
        fs::write(root.join("src/main.rs"), source).expect("write compile fixture source");

        Self { root }
    }

    fn check(&self) -> Output {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        Command::new(cargo)
            .arg("check")
            .arg("--quiet")
            .arg("--offline")
            .env("CARGO_TARGET_DIR", self.root.join("target"))
            .current_dir(&self.root)
            .output()
            .expect("run compile fixture")
    }
}

impl Drop for CompileFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn toml_string(path: &Path) -> String {
    format!("{:?}", path.to_string_lossy())
}

fn assert_constructor_is_compiled_out(
    fixture_name: &str,
    dependency_feature: &str,
    forbidden_constructor: &str,
    source: &str,
) {
    let fixture = CompileFixture::new(fixture_name, dependency_feature, source);
    let output = fixture.check();
    assert!(
        !output.status.success(),
        "the forbidden {forbidden_constructor} constructor compiled under {dependency_feature}"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let named_constructor = format!("`{forbidden_constructor}`");
    let matching_diagnostic = stderr.match_indices("error[E0599]").any(|(start, _)| {
        let diagnostic_tail = &stderr[start..];
        let end = [
            diagnostic_tail.find("\n\n"),
            diagnostic_tail.find("\nnote:"),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(diagnostic_tail.len());
        let diagnostic = &diagnostic_tail[..end];

        (diagnostic.contains("no function or associated item named")
            || diagnostic.contains("no associated function or constant named"))
            && diagnostic.contains(&named_constructor)
            && diagnostic.contains("for struct `BinanceProvider`")
    });
    assert!(
        matching_diagnostic,
        "fixture failed for a reason other than the missing {forbidden_constructor} constructor on BinanceProvider:\n{stderr}"
    );
}

fn assert_function_is_compiled_out(
    fixture_name: &str,
    dependency_feature: &str,
    forbidden_function: &str,
    source: &str,
) {
    let fixture = CompileFixture::new(fixture_name, dependency_feature, source);
    let output = fixture.check();
    assert!(
        !output.status.success(),
        "the forbidden {forbidden_function} function compiled under {dependency_feature}"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let named_function = format!("`{forbidden_function}`");
    let matching_diagnostic = stderr.match_indices("error[E0425]").any(|(start, _)| {
        let diagnostic_tail = &stderr[start..];
        let end = [
            diagnostic_tail.find("\n\n"),
            diagnostic_tail.find("\nnote:"),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(diagnostic_tail.len());
        let diagnostic = &diagnostic_tail[..end];

        diagnostic.contains("cannot find function")
            && diagnostic.contains(&named_function)
            && diagnostic.contains("in module `fccli::provider::binance`")
    });
    assert!(
        matching_diagnostic,
        "fixture failed for a reason other than the missing {forbidden_function} function in fccli::provider::binance:\n{stderr}"
    );
}
fn assert_combined_provider_boundaries_are_compiled_out(
    fixture_name: &str,
    dependency_feature: &str,
    forbidden_constructor: &str,
    forbidden_function: &str,
    source: &str,
) {
    let fixture = CompileFixture::new(fixture_name, dependency_feature, source);
    let output = fixture.check();
    assert!(
        !output.status.success(),
        "the forbidden provider boundaries compiled under {dependency_feature}"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let named_constructor = format!("`{forbidden_constructor}`");
    let constructor_missing = stderr.match_indices("error[E0599]").any(|(start, _)| {
        let diagnostic_tail = &stderr[start..];
        let end = [
            diagnostic_tail.find("\n\n"),
            diagnostic_tail.find("\nnote:"),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(diagnostic_tail.len());
        let diagnostic = &diagnostic_tail[..end];

        (diagnostic.contains("no function or associated item named")
            || diagnostic.contains("no associated function or constant named"))
            && diagnostic.contains(&named_constructor)
            && diagnostic.contains("for struct `BinanceProvider`")
    });
    let named_function = format!("`{forbidden_function}`");
    let websocket_missing = stderr.match_indices("error[E0425]").any(|(start, _)| {
        let diagnostic_tail = &stderr[start..];
        let end = [
            diagnostic_tail.find("\n\n"),
            diagnostic_tail.find("\nnote:"),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(diagnostic_tail.len());
        let diagnostic = &diagnostic_tail[..end];

        diagnostic.contains("cannot find function")
            && diagnostic.contains(&named_function)
            && diagnostic.contains("in module `fccli::provider::binance`")
    });
    assert!(
        constructor_missing && websocket_missing,
        "combined fixture did not independently prove the missing {forbidden_constructor} constructor and {forbidden_function} WebSocket function:\n{stderr}"
    );
}

#[cfg(feature = "test-transport")]
#[test]
fn test_transport_cannot_construct_the_production_rest_client() {
    assert_constructor_is_compiled_out(
        "test-cannot-use-production-rest",
        "test-transport",
        "new",
        r#"
use std::sync::Arc;

use fccli::{clock::SystemClock, provider::binance::BinanceProvider};

fn main() {
    let _ = BinanceProvider::new(Arc::new(SystemClock));
}
"#,
    );
}

#[cfg(feature = "test-transport")]
#[test]
fn test_transport_cannot_connect_the_production_websocket() {
    assert_function_is_compiled_out(
        "test-cannot-use-production-websocket",
        "test-transport",
        "connect_websocket",
        r#"
fn main() {
    let _ = fccli::provider::binance::connect_websocket();
}
"#,
    );
}

#[cfg(feature = "production-transport")]
#[test]
fn production_transport_cannot_use_the_loopback_test_constructor() {
    assert_constructor_is_compiled_out(
        "production-cannot-use-loopback-rest",
        "production-transport",
        "new_test",
        r#"
use std::sync::Arc;

use fccli::{clock::SystemClock, provider::binance::BinanceProvider};

fn main() {
    let _ = BinanceProvider::new_test("http://127.0.0.1:1", Arc::new(SystemClock));
}
"#,
    );
}

#[cfg(feature = "production-transport")]
#[test]
fn production_transport_cannot_connect_the_loopback_test_websocket() {
    assert_function_is_compiled_out(
        "production-cannot-use-loopback-websocket",
        "production-transport",
        "connect_test_websocket",
        r#"
fn main() {
    let _ = fccli::provider::binance::connect_test_websocket();
}
"#,
    );
}

#[test]
fn combined_production_constructors_are_unnameable() {
    #[cfg(feature = "test-transport")]
    assert_combined_provider_boundaries_are_compiled_out(
        "test-registry-cannot-use-production-provider",
        "test-transport",
        "new",
        "connect_websocket",
        r#"
use std::sync::Arc;

use fccli::{
    clock::SystemClock,
    provider::{ProviderRegistry, binance::BinanceProvider},
};

fn main() {
    let provider = Arc::new(
        BinanceProvider::new(Arc::new(SystemClock)).expect("production provider"),
    );
    let _registry = ProviderRegistry::new(provider);
    let _socket = fccli::provider::binance::connect_websocket();
}
"#,
    );

    #[cfg(feature = "production-transport")]
    assert_combined_provider_boundaries_are_compiled_out(
        "production-registry-cannot-use-test-provider",
        "production-transport",
        "new_test_live",
        "connect_test_websocket",
        r#"
use std::sync::Arc;

use fccli::{
    clock::SystemClock,
    provider::{ProviderRegistry, binance::BinanceProvider},
};

fn main() {
    let provider = Arc::new(
        BinanceProvider::new_test_live(todo!(), Arc::new(SystemClock))
            .expect("test provider"),
    );
    let _registry = ProviderRegistry::new(provider);
    let _socket = fccli::provider::binance::connect_test_websocket();
}
"#,
    );
}

#[test]
fn mutually_exclusive_transports_emit_only_the_dedicated_compile_error() {
    let fixture = CompileFixture::new_with_features(
        "mutually-exclusive-transports",
        &["production-transport", "test-transport"],
        "fn main() {}\n",
    );
    let output = fixture.check();
    assert!(
        !output.status.success(),
        "mutually exclusive transport features unexpectedly compiled"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let dedicated = "features `test-transport` and `production-transport` are mutually exclusive";
    assert!(
        stderr.contains(dedicated),
        "missing dedicated mutual-exclusion diagnostic:\n{stderr}"
    );
    assert!(
        !stderr.contains("error[E"),
        "mutual-exclusion failure emitted a secondary Rust type/name error:\n{stderr}"
    );
    assert_eq!(
        stderr.matches(&format!("error: {dedicated}")).count(),
        1,
        "dedicated mutual-exclusion error header was emitted more than once:\n{stderr}"
    );
}
