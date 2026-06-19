use super::*;
use std::io::Write;

fn parse(yaml: &str) -> ConfigResult<Config> {
    parse_config_string(yaml, "")
}

#[test]
fn empty_config() {
    let conf = parse("").unwrap();
    assert!(conf.jobs.is_empty());
    assert!(conf.web.is_none());
}

#[test]
fn simple_config() {
    let conf = parse(
        r#"
defaults:
  shell: /bin/bash
jobs:
  - name: test-03
    command: |
      echo "starting..."
      sleep 10
    schedule:
      minute: "*"
    captureStderr: true
    executionTimeout: 1
    killTimeout: 0.5
"#,
    )
    .unwrap();
    assert_eq!(conf.jobs.len(), 1);
    let job = &conf.jobs[0];
    assert_eq!(job.name, "test-03");
    assert_eq!(job.shell, "/bin/bash");
    assert!(job.capture_stderr);
    assert!(!job.capture_stdout);
    assert_eq!(job.execution_timeout, Some(1.0));
    assert_eq!(job.kill_timeout, 0.5);
}

#[test]
fn defaults_inherited_and_overridden() {
    let conf = parse(
        r#"
defaults:
  shell: /bin/bash
  utc: false
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
  - name: b
    command: echo b
    schedule: "* * * * *"
    shell: /bin/sh
"#,
    )
    .unwrap();
    assert_eq!(conf.jobs[0].shell, "/bin/bash");
    assert_eq!(conf.jobs[1].shell, "/bin/sh");
    assert!(!conf.jobs[0].utc);
}

#[test]
fn env_file_and_override() {
    let dir = tempfile::tempdir().unwrap();
    let env_path = dir.path().join("vars.env");
    let mut f = std::fs::File::create(&env_path).unwrap();
    write!(
        f,
        "# comment\n\n  VAR_OVERRIDE=ENV_FILE\nVAR_ENV_FILE = ENV_FILE\nVAR_TEST_EQUAL_SIGN=ENV_FILE===\n"
    )
    .unwrap();

    let conf = parse(&format!(
        r#"
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    environment:
      - key: VAR_STD
        value: STD
      - key: VAR_OVERRIDE
        value: STD
    env_file: {}
"#,
        env_path.display()
    ))
    .unwrap();

    let env: std::collections::HashMap<_, _> = conf.jobs[0]
        .environment
        .iter()
        .map(|e| (e.key.clone(), e.value.clone()))
        .collect();
    assert_eq!(env["VAR_STD"], "STD");
    assert_eq!(env["VAR_ENV_FILE"], "ENV_FILE");
    assert_eq!(env["VAR_OVERRIDE"], "STD"); // config overrides file
    assert_eq!(env["VAR_TEST_EQUAL_SIGN"], "ENV_FILE===");
}

#[test]
fn env_file_invalid_line() {
    let dir = tempfile::tempdir().unwrap();
    let env_path = dir.path().join("bad.env");
    std::fs::write(&env_path, "THERE_IS_NO_VALUE\n").unwrap();
    let err = parse(&format!(
        r#"
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    env_file: {}
"#,
        env_path.display()
    ))
    .unwrap_err();
    assert!(err.to_string().contains("env_file"));
}

#[test]
fn defaults_environment_merge_by_key() {
    let conf = parse(
        r#"
defaults:
  environment:
    - key: SHARED
      value: from-default
    - key: ONLY_DEFAULT
      value: d
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    environment:
      - key: SHARED
        value: from-job
      - key: ONLY_JOB
        value: j
"#,
    )
    .unwrap();
    let env: std::collections::HashMap<_, _> = conf.jobs[0]
        .environment
        .iter()
        .map(|e| (e.key.clone(), e.value.clone()))
        .collect();
    assert_eq!(env.len(), 3);
    assert_eq!(env["SHARED"], "from-job");
    assert_eq!(env["ONLY_DEFAULT"], "d");
    assert_eq!(env["ONLY_JOB"], "j");
}

#[test]
fn sentry_fingerprint_override_replaces() {
    let conf = parse(
        r#"
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    onFailure:
      report:
        sentry:
          fingerprint:
            - my-group
            - "{{ name }}"
"#,
    )
    .unwrap();
    assert_eq!(
        conf.jobs[0].on_failure_report.sentry.fingerprint,
        vec!["my-group".to_string(), "{{ name }}".to_string()]
    );
}

#[test]
fn default_report_is_inherited() {
    let conf = parse(
        r#"
defaults:
  onFailure:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
        smtpPort: 10025
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
"#,
    )
    .unwrap();
    let mail = &conf.jobs[0].on_failure_report.mail;
    assert_eq!(mail.from.as_deref(), Some("example@foo.com"));
    assert_eq!(mail.smtp_port, 10025);
    // unspecified fields fall back to built-in defaults
    assert!(!mail.html);
    assert!(mail.validate_certs);
}

#[test]
fn report_defaults_independent_objects() {
    // onFailure / onSuccess fingerprints must be distinct (no aliasing).
    let conf = parse(
        r#"
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    onSuccess:
      report:
        sentry:
          fingerprint: [only-success]
"#,
    )
    .unwrap();
    let job = &conf.jobs[0];
    assert_eq!(
        job.on_success_report.sentry.fingerprint,
        vec!["only-success"]
    );
    // the failure fingerprint keeps the built-in default
    assert_eq!(job.on_failure_report.sentry.fingerprint.len(), 3);
}

#[test]
fn numeric_range_validation() {
    for (field, value, needle) in [
        ("saveLimit", "-1", "saveLimit"),
        ("maxLineLength", "0", "maxLineLength"),
        ("killTimeout", "-5", "killTimeout"),
        ("executionTimeout", "-1", "executionTimeout"),
    ] {
        let err = parse(&format!(
            "jobs:\n  - name: t\n    command: foo\n    schedule: \"* * * * *\"\n    {field}: {value}\n"
        ))
        .unwrap_err();
        assert!(err.to_string().contains(needle), "{field}");
    }
}

#[test]
fn unknown_field_rejected() {
    let err = parse(
        r#"
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    capturStderr: true
"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("capturstderr")
            || err.to_string().contains("unknown")
    );
}

#[test]
fn includes_merge_defaults_and_jobs() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("child.yaml"),
        "defaults:\n  shell: /bin/ksh\njobs:\n  - name: common\n    command: echo hi\n    schedule: \"* * * * *\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("parent.yaml"),
        "include:\n  - child.yaml\njobs:\n  - name: top\n    command: echo top\n    schedule: \"* * * * *\"\n",
    )
    .unwrap();
    let conf = parse_config(dir.path().join("parent.yaml").to_str().unwrap()).unwrap();
    assert_eq!(conf.jobs.len(), 2);
    assert_eq!(conf.jobs[0].name, "common");
    assert_eq!(conf.jobs[1].name, "top");
    // included file's defaults apply to the parent's inline jobs
    assert_eq!(conf.jobs[1].shell, "/bin/ksh");
}

#[test]
fn include_cycle_detected() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.yaml"), "include:\n  - a.yaml\n").unwrap();
    let err = parse_config(dir.path().join("a.yaml").to_str().unwrap()).unwrap_err();
    assert!(err.to_string().contains("cycle"));
}

#[test]
fn directory_aggregates_sorted_and_skips_underscore() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("20-b.yaml"),
        "jobs:\n  - name: b\n    command: foo\n    schedule: \"* * * * *\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("10-a.yaml"),
        "jobs:\n  - name: a\n    command: foo\n    schedule: \"* * * * *\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("_skip.yaml"),
        "jobs:\n  - name: skip\n    command: foo\n    schedule: \"* * * * *\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("30-web.yaml"),
        "web:\n  listen:\n    - http://127.0.0.1:8080\n",
    )
    .unwrap();
    let conf = parse_config(dir.path().to_str().unwrap()).unwrap();
    let names: Vec<_> = conf.jobs.iter().map(|j| j.name.as_str()).collect();
    assert_eq!(names, vec!["a", "b"]);
    assert!(conf.web.is_some());
}

#[test]
fn directory_multiple_web_errors() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("a.yaml"),
        "web:\n  listen:\n    - http://127.0.0.1:8080\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("b.yaml"),
        "web:\n  listen:\n    - http://127.0.0.1:8081\n",
    )
    .unwrap();
    let err = parse_config(dir.path().to_str().unwrap()).unwrap_err();
    assert!(err.to_string().contains("Multiple 'web'"));
}

#[test]
fn empty_directory_is_empty_config() {
    let dir = tempfile::tempdir().unwrap();
    let conf = parse_config(dir.path().to_str().unwrap()).unwrap();
    assert!(conf.jobs.is_empty());
    assert!(conf.web.is_none());
}
