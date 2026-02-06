use rstest::rstest;
use std::path::PathBuf;
use std::process::Command;

fn run_sql(sql_path: &PathBuf) -> (String, String, bool) {
    let clickhouse_path = std::env::var("CLICKHOUSE_BIN").unwrap_or_else(|_| "clickhouse".into());
    let user_script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("bundle")
        .join("var")
        .join("lib")
        .join("clickhouse")
        .join("user_scripts");
    let user_defined_executable_functions_config = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("bundle")
        .join("etc")
        .join("clickhouse-server")
        .join("*_function.*ml");
    let user_defined_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tmp")
        .join("bundle")
        .join("var")
        .join("lib")
        .join("clickhouse")
        .join("user_defined");
    let mut cmd = Command::new(clickhouse_path);

    let cmd = cmd.args([
        "local",
        "--log-level",
        "debug",
        "--queries-file",
        &sql_path.to_string_lossy(),
        "--",
        "--user_scripts_path",
        &user_script_path.to_string_lossy(),
        "--user_defined_executable_functions_config",
        &user_defined_executable_functions_config.to_string_lossy(),
        "--user_defined_path",
        &user_defined_path.to_string_lossy(),
        "--send_logs_level",
        "trace",
    ]);

    let output = cmd
        .output()
        .expect("failed to run clickhouse local — is it installed and in PATH?");

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (stdout, stderr, output.status.success())
}

#[rstest]
fn sql_test(#[files("tests/sql/*.sql")] path: PathBuf) {
    let expected_path = path.clone().with_extension("tsv");
    let (stdout, stderr, success) = run_sql(&path);

    assert!(
        success,
        "clickhouse local failed for {}:\n{}",
        path.display(),
        stderr,
    );

    let expected = std::fs::read_to_string(&expected_path).unwrap_or_else(|_| {
        panic!(
            "missing expected output file: {}\nActual output:\n{stdout}\nRun with BLESS=1 cargo test to generate it.",
            expected_path.display(),
        )
    });

    if stdout != expected {
        panic!(
            "output mismatch for {}\n\n--- expected\n+++ actual\n{}",
            path.display(),
            diff(&expected, &stdout),
        );
    }
}

fn diff(expected: &str, actual: &str) -> String {
    let mut out = String::new();
    let exp: Vec<&str> = expected.lines().collect();
    let act: Vec<&str> = actual.lines().collect();

    for i in 0..exp.len().max(act.len()) {
        match (exp.get(i), act.get(i)) {
            (Some(e), Some(a)) if e != a => {
                out.push_str(&format!("@@ line {} @@\n-{e}\n+{a}\n", i + 1));
            }
            (Some(e), None) => {
                out.push_str(&format!("@@ line {} @@\n-{e}\n", i + 1));
            }
            (None, Some(a)) => {
                out.push_str(&format!("@@ line {} @@\n+{a}\n", i + 1));
            }
            _ => {}
        }
    }

    if expected.ends_with('\n') != actual.ends_with('\n') {
        out.push_str("(trailing newline difference)\n");
    }

    out
}
