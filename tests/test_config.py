import os
from types import SimpleNamespace

import pytest

from yacron2 import config
from yacron2.config import ConfigError
from yacron2.platform import IS_WINDOWS


def test_mergedicts():
    assert dict(config.mergedicts({"a": 1}, {"b": 2})) == {"a": 1, "b": 2}


def test_mergedicts_nested():
    assert dict(
        config.mergedicts(
            {"a": {"x": 1, "y": 2, "z": 3}}, {"a": {"y": 10}, "b": 2}
        )
    ) == {"a": {"x": 1, "y": 10, "z": 3}, "b": 2}


def test_mergedicts_right_none():
    assert dict(config.mergedicts({"a": {"x": 1}}, {"a": None, "b": 2})) == {
        "a": {"x": 1},
        "b": 2,
    }


def test_mergedicts_lists():
    assert dict(
        config.mergedicts({"env": [{"key": "FOO"}]}, {"env": [{"key": "BAR"}]})
    ) == {"env": [{"key": "FOO"}, {"key": "BAR"}]}


def test_simple_config1():
    conf = config.parse_config_string(
        """
defaults:
  shell: /bin/bash

jobs:
  - name: test-03
    command: |
      trap "echo '(ignoring SIGTERM)'" TERM
      echo "starting..."
      sleep 10
      echo "all done."
    schedule:
      minute: "*"
    captureStderr: true
    executionTimeout: 1
    killTimeout: 0.5
                       """,
        "",
    )
    assert conf.web_config is None
    assert len(conf.jobs) == 1
    job = conf.jobs[0]
    assert job.name == "test-03"
    assert job.command == (
        "trap \"echo '(ignoring SIGTERM)'\" TERM\n"
        'echo "starting..."\n'
        "sleep 10\n"
        'echo "all done."\n'
    )
    assert job.schedule_unparsed == {"minute": "*"}
    assert job.captureStderr is True
    assert job.captureStdout is False
    assert job.executionTimeout == 1
    assert job.killTimeout == 0.5


def test_config_default_report():
    conf = config.parse_config_string(
        """
defaults:
  onFailure:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
        smtpPort: 10025

jobs:
  - name: test-03
    command: foo
    schedule:
      minute: "*"
    captureStderr: true
                       """,
        "",
    )
    assert len(conf.jobs) == 1
    job = conf.jobs[0]
    assert job.onFailure == (
        {
            "report": {
                "mail": {
                    "from": "example@foo.com",
                    "smtpHost": "127.0.0.1",
                    "smtpPort": 10025,
                    "to": "example@bar.com",
                    "body": (
                        config.DEFAULT_CONFIG["onFailure"]["report"]["mail"][
                            "body"
                        ]
                    ),
                    "subject": (
                        config.DEFAULT_CONFIG["onFailure"]["report"]["mail"][
                            "subject"
                        ]
                    ),
                    "username": None,
                    "password": {
                        "fromEnvVar": None,
                        "fromFile": None,
                        "value": None,
                    },
                    "tls": False,
                    "starttls": False,
                    "validate_certs": True,
                    "html": False,
                },
                "sentry": (
                    config.DEFAULT_CONFIG["onFailure"]["report"]["sentry"]
                ),
                "shell": config.DEFAULT_CONFIG["onFailure"]["report"]["shell"],
                "webhook": (
                    config.DEFAULT_CONFIG["onFailure"]["report"]["webhook"]
                ),
            },
            "retry": {
                "backoffMultiplier": 2,
                "initialDelay": 1,
                "maximumDelay": 300,
                "maximumRetries": 0,
            },
        }
    )


def test_config_default_report_override():
    # even if the default says send email on error, it should be possible for
    # specific jobs to override the default and disable sending email.
    conf = config.parse_config_string(
        """
defaults:
  onFailure:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com
        smtpHost: 127.0.0.1
        smtpPort: 10025

jobs:
  - name: test-03
    command: foo
    schedule:
      minute: "*"
    captureStderr: true
    onFailure:
      report:
        mail:
          to:
          from:
                       """,
        "",
    )
    assert len(conf.jobs) == 1
    job = conf.jobs[0]
    assert job.onFailure == (
        {
            "report": {
                "mail": {
                    "from": None,
                    "smtpHost": "127.0.0.1",
                    "smtpPort": 10025,
                    "to": None,
                    "body": (
                        config.DEFAULT_CONFIG["onFailure"]["report"]["mail"][
                            "body"
                        ]
                    ),
                    "subject": (
                        config.DEFAULT_CONFIG["onFailure"]["report"]["mail"][
                            "subject"
                        ]
                    ),
                    "username": None,
                    "password": {
                        "fromEnvVar": None,
                        "fromFile": None,
                        "value": None,
                    },
                    "tls": False,
                    "starttls": False,
                    "validate_certs": True,
                    "html": False,
                },
                "sentry": (
                    config.DEFAULT_CONFIG["onFailure"]["report"]["sentry"]
                ),
                "shell": config.DEFAULT_CONFIG["onFailure"]["report"]["shell"],
                "webhook": (
                    config.DEFAULT_CONFIG["onFailure"]["report"]["webhook"]
                ),
            },
            "retry": {
                "backoffMultiplier": 2,
                "initialDelay": 1,
                "maximumDelay": 300,
                "maximumRetries": 0,
            },
        }
    )


def test_empty_config1():
    conf = config.parse_config_string("", "")
    assert len(conf.jobs) == 0
    assert conf.web_config is None


def test_environ_file():
    conf = config.parse_config_string(
        """
defaults:
  shell: /bin/bash

jobs:
  - name: test
    command: |
      echo VAR_STD: $VAR_STD
      echo VAR_ENV_FILE: $VAR_ENV_FILE
      echo VAR_OVERRIDE: $VAR_OVERRIDE
    schedule:
      minute: "*"
    captureStderr: true
    environment:
        - key: VAR_STD
          value: STD
        - key: VAR_OVERRIDE
          value: STD
    env_file: tests/fixtures/.testenv
""",
        "",
    )
    job = conf.jobs[0]

    # NOTE: the file format implicitly verifies that the parsing is being
    # done correctly on these fronts:
    # * comments
    # * empty lines
    # * trailing spaces
    # * spaces around the separation character
    # * other ``=`` in the value

    dict_environment = {env["key"]: env["value"] for env in job.environment}
    # check config-only
    assert dict_environment["VAR_STD"] == "STD"
    # check file-only variable
    assert dict_environment["VAR_ENV_FILE"] == "ENV_FILE"
    # check config variables override env_file's
    assert dict_environment["VAR_OVERRIDE"] == "STD"
    # check the multiple ``=``
    assert dict_environment["VAR_TEST_EQUAL_SIGN"] == "ENV_FILE==="


def test_invalid_environ_file():
    # invalid file (no key-value)
    with pytest.raises(ConfigError) as exc:
        config.parse_config_string(
            """
    defaults:
      shell: /bin/bash

    jobs:
      - name: test
        command: |
          echo VAR_STD: $VAR_STD
          echo VAR_ENV_FILE: $VAR_ENV_FILE
          echo VAR_OVERRIDE: $VAR_OVERRIDE
        schedule:
          minute: "*"
        captureStderr: true
        environment:
            - key: VAR_STD
              value: STD
            - key: VAR_OVERRIDE
              value: STD
        env_file: tests/fixtures/.testenv-invalid
    """,
            "",
        )

    assert "env_file" in str(exc.value)

    # non-existent file should raise ConfigError, not OSError
    with pytest.raises(ConfigError) as exc:
        config.parse_config_string(
            """
    defaults:
      shell: /bin/bash

    jobs:
      - name: test
        command: |
          echo VAR_STD: $VAR_STD
          echo VAR_ENV_FILE: $VAR_ENV_FILE
          echo VAR_OVERRIDE: $VAR_OVERRIDE
        schedule:
          minute: "*"
        captureStderr: true
        environment:
            - key: VAR_STD
              value: STD
            - key: VAR_OVERRIDE
              value: STD
        env_file: .testenv-nonexistent
    """,
            "",
        )

    assert "env_file" in str(exc.value)


def test_config_include():
    conf = config.parse_config(
        os.path.join(os.path.dirname(__file__), "test_include_parent.yaml")
    )
    assert len(conf.jobs) == 2
    job1, job2 = conf.jobs
    assert job1.name == "common-task"
    assert job2.name == "test-03"
    assert job1.shell == "/bin/ksh"
    assert job2.shell == "/bin/ksh"


def test_logging_config():
    conf = config.parse_config_string(
        """
logging:
    version: 1
    incremental: false
    disable_existing_loggers: false
    formatters: one
    filters: two
    handlers: three
    loggers: four
    root: five
        """,
        "",
    )
    assert conf.logging_config == {
        "version": 1,
        "incremental": False,
        "disable_existing_loggers": False,
        "formatters": "one",
        "filters": "two",
        "handlers": "three",
        "loggers": "four",
        "root": "five",
    }


def test_mergedicts_environment_dedup():
    # when both defaults and a job define `environment`, the job's value must
    # override the default for the same key instead of producing a duplicate.
    merged = dict(
        config.mergedicts(
            {"environment": [{"key": "FOO", "value": "default"}]},
            {"environment": [{"key": "FOO", "value": "job"}]},
        )
    )
    assert merged["environment"] == [{"key": "FOO", "value": "job"}]


def test_defaults_environment_merge_with_job():
    conf = config.parse_config_string(
        """
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
""",
        "",
    )
    env = {e["key"]: e["value"] for e in conf.jobs[0].environment}
    assert env == {
        "SHARED": "from-job",  # job overrides default, no duplicate
        "ONLY_DEFAULT": "d",
        "ONLY_JOB": "j",
    }


def test_monitor_resources_default_off():
    conf = config.parse_config_string(
        """
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
""",
        "",
    )
    assert conf.jobs[0].monitorResources is False


def test_monitor_resources_from_defaults_and_override():
    conf = config.parse_config_string(
        """
defaults:
  monitorResources: true

jobs:
  - name: on-from-default
    command: foo
    schedule: "* * * * *"
  - name: explicit-off
    command: bar
    schedule: "* * * * *"
    monitorResources: false
""",
        "",
    )
    by_name = {j.name: j.monitorResources for j in conf.jobs}
    assert by_name == {"on-from-default": True, "explicit-off": False}


def test_monitor_resources_applies_to_dag_tasks():
    conf = config.parse_config_string(
        """
dags:
  - name: pipe
    schedule: "* * * * *"
    tasks:
      - id: a
        command: foo
        monitorResources: true
""",
        "",
    )
    task = conf.dags[0].tasks[0]
    assert task.id == "a"
    assert task.job_template.monitorResources is True


def test_report_defaults_not_aliased():
    # onFailure/onPermanentFailure/onSuccess report blocks must be independent
    # objects so mutating one cannot corrupt the others (or the global
    # default).
    conf = config.parse_config_string(
        """
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
""",
        "",
    )
    job = conf.jobs[0]
    assert (
        job.onFailure["report"]["sentry"]["fingerprint"]
        is not job.onSuccess["report"]["sentry"]["fingerprint"]
    )


def test_parse_config_empty_dir(tmp_path):
    # an existing but empty config directory must yield an empty config,
    # not crash with UnboundLocalError.
    conf = config.parse_config(str(tmp_path))
    assert conf.jobs == []
    assert conf.web_config is None
    assert conf.logging_config is None


def test_parse_config_dir_aggregates(tmp_path):
    (tmp_path / "10-jobs.yaml").write_text(
        """
jobs:
  - name: job-a
    command: foo
    schedule: "* * * * *"
"""
    )
    (tmp_path / "20-web.yaml").write_text(
        """
web:
  listen:
    - http://127.0.0.1:8080
"""
    )
    (tmp_path / "30-logging.yaml").write_text(
        """
logging:
  version: 1
"""
    )
    # underscore-prefixed files are ignored
    (tmp_path / "_ignored.yaml").write_text(
        """
jobs:
  - name: ignored
    command: foo
    schedule: "* * * * *"
"""
    )
    conf = config.parse_config(str(tmp_path))
    assert [j.name for j in conf.jobs] == ["job-a"]
    # web config from one file is not dropped in favor of the last file
    assert conf.web_config == {"listen": ["http://127.0.0.1:8080"]}
    # logging config from a different file is aggregated, not lost
    assert conf.logging_config == {"version": 1}


def test_parse_config_dir_multiple_web(tmp_path):
    (tmp_path / "a.yaml").write_text(
        "web:\n  listen:\n    - http://127.0.0.1:8080\n"
    )
    (tmp_path / "b.yaml").write_text(
        "web:\n  listen:\n    - http://127.0.0.1:8081\n"
    )
    with pytest.raises(ConfigError) as exc:
        config.parse_config(str(tmp_path))
    assert "Multiple 'web'" in str(exc.value)


@pytest.mark.parametrize(
    "field, value, message",
    [
        ("saveLimit", -1, "saveLimit"),
        ("maxLineLength", 0, "maxLineLength"),
        ("killTimeout", -5, "killTimeout"),
        ("executionTimeout", -1, "executionTimeout"),
    ],
)
def test_invalid_numeric_ranges(field, value, message):
    with pytest.raises(ConfigError) as exc:
        config.parse_config_string(
            f"""
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    {field}: {value}
""",
            "",
        )
    assert message in str(exc.value)


def test_invalid_retry_backoff():
    with pytest.raises(ConfigError) as exc:
        config.parse_config_string(
            """
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    onFailure:
      retry:
        maximumRetries: 3
        initialDelay: 1
        maximumDelay: 10
        backoffMultiplier: 0
""",
            "",
        )
    assert "backoffMultiplier" in str(exc.value)


def test_sentry_fingerprint_override_replaces():
    # a job that supplies its own sentry fingerprint must replace the default
    # entirely, not have the three default entries prepended to it.
    conf = config.parse_config_string(
        """
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
""",
        "",
    )
    job = conf.jobs[0]
    assert job.onFailure["report"]["sentry"]["fingerprint"] == [
        "my-group",
        "{{ name }}",
    ]


def test_parse_config_dir_sorted_order(tmp_path):
    # job order across files must be deterministic (sorted by filename), not
    # dependent on the arbitrary order os.scandir returns.
    (tmp_path / "20-b.yaml").write_text(
        "jobs:\n  - name: b\n    command: foo\n    schedule: '* * * * *'\n"
    )
    (tmp_path / "10-a.yaml").write_text(
        "jobs:\n  - name: a\n    command: foo\n    schedule: '* * * * *'\n"
    )
    (tmp_path / "30-c.yaml").write_text(
        "jobs:\n  - name: c\n    command: foo\n    schedule: '* * * * *'\n"
    )
    conf = config.parse_config(str(tmp_path))
    assert [j.name for j in conf.jobs] == ["a", "b", "c"]


def test_include_cycle_detected(tmp_path):
    # a file that includes itself must raise a clear ConfigError instead of
    # recursing until RecursionError.
    cfg = tmp_path / "a.yaml"
    cfg.write_text("include:\n  - a.yaml\n")
    with pytest.raises(ConfigError) as exc:
        config.parse_config(str(cfg))
    assert "cycle" in str(exc.value)


def test_environ_file_utf8(tmp_path):
    # env files are decoded as UTF-8 regardless of the system locale.
    env = tmp_path / "vars.env"
    env.write_text("GREETING=héllo\n", encoding="utf-8")
    conf = config.parse_config_string(
        f"""
jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
    env_file: {env}
""",
        "",
    )
    environment = {e["key"]: e["value"] for e in conf.jobs[0].environment}
    assert environment["GREETING"] == "héllo"


# ---------------------------------------------------------------------------
# POSIX user/group resolution (_resolve_user_group) -- POSIX only.
#
# Resolving a configured user/group to a uid/gid runs on every deploy that uses
# the feature, yet was entirely untested. A regression here runs a job as the
# wrong account or fails to fail-closed when not root. The passwd/group lookups
# and os.geteuid are mocked, so the tests need no real users or root.
# ---------------------------------------------------------------------------


def _passwd(name, uid, gid):
    return SimpleNamespace(pw_name=name, pw_uid=uid, pw_gid=gid)


def _mock_userdb(monkeypatch, *, pwnam=None, pwuid=None, grnam=None, euid=0):
    # Imported here (not at module top) so the module still imports on Windows,
    # where grp/pwd do not exist; only the POSIX-gated tests below call this.
    import grp
    import pwd

    if pwnam is not None:
        monkeypatch.setattr(pwd, "getpwnam", pwnam)
    if pwuid is not None:
        monkeypatch.setattr(pwd, "getpwuid", pwuid)
    if grnam is not None:
        monkeypatch.setattr(grp, "getgrnam", grnam)
    monkeypatch.setattr(os, "geteuid", lambda: euid)


def _parse_user_group(line):
    return config.parse_config_string(
        "jobs:\n"
        "  - name: t\n"
        "    command: echo hi\n"
        '    schedule: "* * * * *"\n'
        "    " + line + "\n",
        "",
    )


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_user_string_resolves_uid_gid_and_name(monkeypatch):
    _mock_userdb(monkeypatch, pwnam=lambda n: _passwd("svc", 1000, 2000))
    job = _parse_user_group("user: svc").jobs[0]
    assert (job.uid, job.gid, job.username) == (1000, 2000, "svc")
    # the *configured* value is retained (for the fingerprint), not resolved
    assert job.user == "svc" and job.group is None


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_numeric_user_resolves_uid_and_derives_gid_name(monkeypatch):
    # a numeric `user: 1000` is taken directly as the uid; the primary gid and
    # login name are derived from the passwd db so a numeric user does not
    # silently keep yacron2's (root) gid. (The schema is Int() | Str(), so a
    # bare number parses as an int and reaches this branch.)
    def getpwnam(name):
        raise AssertionError("a numeric user must not be looked up by name")

    _mock_userdb(
        monkeypatch,
        pwnam=getpwnam,
        pwuid=lambda u: _passwd("svc", 1000, 2000),
    )
    job = _parse_user_group("user: 1000").jobs[0]
    assert (job.uid, job.gid, job.username) == (1000, 2000, "svc")
    assert job.user == 1000  # configured value retained, as an int


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_numeric_user_unknown_to_passwd_keeps_uid_only(monkeypatch):
    # a numeric uid absent from the passwd db is still honored (uid set); no
    # gid/name can be derived, so they stay None.
    def getpwuid(uid):
        raise KeyError(uid)

    _mock_userdb(monkeypatch, pwuid=getpwuid)
    job = _parse_user_group("user: 1000").jobs[0]
    assert job.uid == 1000
    assert job.gid is None
    assert job.username is None


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_quoted_numeric_user_is_still_a_uid(monkeypatch):
    # LIMITATION: Int() validates on the scalar's text, so even a quoted
    # `user: "1000"` parses as the integer 1000 (a uid), not the login name
    # "1000". An all-digits username therefore cannot be expressed; use the
    # numeric uid, or a non-numeric name. Documented so the limitation is a
    # conscious contract, not an accident.
    _mock_userdb(monkeypatch, pwuid=lambda u: _passwd("svc", 1000, 2000))
    job = _parse_user_group('user: "1000"').jobs[0]
    assert job.uid == 1000
    assert job.user == 1000


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_user_not_found_raises(monkeypatch):
    def getpwnam(name):
        raise KeyError(name)

    _mock_userdb(monkeypatch, pwnam=getpwnam)
    with pytest.raises(ConfigError, match="User not found"):
        _parse_user_group("user: ghost")


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_group_string_resolves_gid(monkeypatch):
    _mock_userdb(monkeypatch, grnam=lambda n: SimpleNamespace(gr_gid=3000))
    job = _parse_user_group("group: staff").jobs[0]
    assert job.gid == 3000
    assert job.uid is None
    assert job.group == "staff"


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_numeric_group_sets_gid_directly(monkeypatch):
    # a numeric `group: 3000` is used directly as the gid, with no name lookup.
    def getgrnam(name):
        raise AssertionError("a numeric group must not be looked up by name")

    _mock_userdb(monkeypatch, grnam=getgrnam)
    job = _parse_user_group("group: 3000").jobs[0]
    assert job.gid == 3000
    assert job.group == 3000


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_group_not_found_raises(monkeypatch):
    def getgrnam(name):
        raise KeyError(name)

    _mock_userdb(monkeypatch, grnam=getgrnam)
    with pytest.raises(ConfigError, match="Group not found"):
        _parse_user_group("group: nogroup")


@pytest.mark.skipif(IS_WINDOWS, reason="user/group resolution is POSIX-only")
def test_user_group_requires_superuser(monkeypatch):
    # changing user/group while not root must fail closed at config time, not
    # silently run the job as the wrong (current) account.
    _mock_userdb(
        monkeypatch, pwnam=lambda n: _passwd("svc", 1000, 2000), euid=1000
    )
    with pytest.raises(ConfigError, match="not running as superuser"):
        _parse_user_group("user: svc")


# ---------------------------------------------------------------------------
# web.metrics (the native Prometheus endpoint; see yacron2/prometheus.py)
# ---------------------------------------------------------------------------

_WEB_METRICS_BASE = """
web:
  listen:
    - http://127.0.0.1:8080
"""


def test_web_metrics_absent_by_default():
    conf = config.parse_config_string(_WEB_METRICS_BASE, "")
    # the key is simply absent; enabled-by-default is applied at the use
    # site (yacron2.prometheus.resolve_metrics_config)
    assert "metrics" not in conf.web_config


def test_web_metrics_bool_shorthand():
    conf = config.parse_config_string(
        _WEB_METRICS_BASE + "  metrics: false\n", ""
    )
    assert conf.web_config["metrics"] is False


def test_web_metrics_map_form():
    conf = config.parse_config_string(
        _WEB_METRICS_BASE
        + "  metrics:\n"
        + "    public: true\n"
        + "    durationBuckets:\n"
        + "      - 0.5\n"
        + "      - 30\n",
        "",
    )
    metrics = conf.web_config["metrics"]
    assert metrics["public"] is True
    assert metrics["durationBuckets"] == [0.5, 30.0]


def test_web_metrics_buckets_must_increase():
    with pytest.raises(ConfigError, match="strictly increasing"):
        config.parse_config_string(
            _WEB_METRICS_BASE
            + "  metrics:\n"
            + "    durationBuckets:\n"
            + "      - 10\n"
            + "      - 5\n",
            "",
        )


def test_web_metrics_buckets_must_be_positive():
    with pytest.raises(ConfigError, match="strictly increasing"):
        config.parse_config_string(
            _WEB_METRICS_BASE
            + "  metrics:\n"
            + "    durationBuckets:\n"
            + "      - -1\n"
            + "      - 5\n",
            "",
        )


def test_web_metrics_buckets_must_not_be_empty():
    # strictyaml cannot express an empty block sequence, so the empty case
    # is validated directly against the builder-level check
    with pytest.raises(ConfigError, match="must not be empty"):
        config._validate_web_config(
            config.WebConfig({"metrics": {"durationBuckets": []}})
        )


def test_web_metrics_buckets_must_be_finite():
    with pytest.raises(ConfigError, match="finite"):
        config._validate_web_config(
            config.WebConfig(
                {"metrics": {"durationBuckets": [1.0, float("inf")]}}
            )
        )


# --- second-level scheduling -------------------------------------------------


def _one_job(schedule_yaml: str):
    conf = config.parse_config_string(
        "jobs:\n  - name: t\n    command: echo hi\n" + schedule_yaml, ""
    )
    return conf.jobs[0]


def test_schedule_object_second_builds_seven_field_crontab():
    # An explicit second: opts into second-level scheduling; the object form
    # renders to a full 7-field crontab line (year defaults to "*").
    job = _one_job('    schedule:\n      second: "*/15"\n      minute: "*"\n')
    assert job.has_seconds is True
    from yacron2.cronexpr import CronTab

    assert isinstance(job.schedule, CronTab)
    # fires at seconds 0,15,30,45 of the minute (7-field dialect)
    assert config.schedule_object_to_crontab(job.schedule_unparsed) == (
        "*/15 * * * * * *"
    )


def test_schedule_string_seven_field_has_seconds():
    job = _one_job('    schedule: "*/10 * * * * * *"\n')
    assert job.has_seconds is True


@pytest.mark.parametrize(
    "schedule, expect_seconds",
    [
        ('    schedule: "*/5 * * * *"\n', False),  # 5-field minute
        ('    schedule: "0 12 * * * 2030"\n', False),  # 6-field year, not sec
        ('    schedule: "0 0 12 * * * *"\n', True),  # 7-field, explicit second
        ('    schedule: "@reboot"\n', False),
        ('    schedule: "@daily"\n', False),
    ],
)
def test_has_seconds_string_forms(schedule, expect_seconds):
    assert _one_job(schedule).has_seconds is expect_seconds


def test_schedule_object_year_now_honored_six_field():
    # Regression: the object-form year: used to be silently dropped. It now
    # maps to the trailing year column (6-field line), no seconds.
    job = _one_job(
        '    schedule:\n      minute: "*/5"\n'
        '      dayOfMonth: "19"\n      month: "7"\n      year: "2017"\n'
    )
    assert job.has_seconds is False
    assert config.schedule_object_to_crontab(job.schedule_unparsed) == (
        "*/5 * 19 7 * 2017"
    )


def test_schedule_object_minute_only_unchanged():
    # No second/year -> the exact 5-field line it always produced (so existing
    # job-set fingerprints are unperturbed).
    job = _one_job('    schedule:\n      minute: "*/5"\n')
    assert job.has_seconds is False
    assert config.schedule_object_to_crontab(job.schedule_unparsed) == (
        "*/5 * * * *"
    )


def test_out_of_range_second_reports_config_error():
    with pytest.raises(ConfigError, match="invalid schedule"):
        _one_job('    schedule: "99 * * * * * *"\n')


def test_bad_schedule_string_reports_config_error():
    # a malformed field is a ConfigError (was an anonymous ValueError before)
    with pytest.raises(ConfigError, match="invalid schedule"):
        _one_job('    schedule: "* * * notamonth *"\n')


@pytest.mark.parametrize("blank", ['""', '" "'])
def test_blank_second_is_not_second_granular(blank):
    # A blank/whitespace `second:` value collapses to a minute-granular line;
    # has_seconds must be False so it does not force the whole scheduler to
    # tick per-second for a job that only fires once a minute. has_seconds is
    # derived from the actual rendered field count, not mere key presence.
    job = _one_job(
        "    schedule:\n      second: {}\n      minute: \"5\"\n".format(blank)
    )
    assert job.has_seconds is False


# ---- monitorResources: bool-or-map forms ------------------------------------


def _monitor_job(snippet):
    conf = config.parse_config_string(
        "jobs:\n"
        "  - name: mon\n"
        "    command: echo hi\n"
        '    schedule: "* * * * *"\n' + snippet,
        "",
    )
    return conf.jobs[0]


def test_monitor_resources_defaults_off():
    job = _monitor_job("")
    assert job.monitorResources is False
    # the sampling knobs still normalize so consumers never branch on shape
    assert job.monitorResourcesInterval == config.SAMPLE_INTERVAL
    assert job.monitorResourcesHistory == config.MONITOR_HISTORY_DEFAULT


def test_monitor_resources_bool_form():
    job = _monitor_job("    monitorResources: true\n")
    assert job.monitorResources is True
    assert job.monitorResourcesInterval == config.SAMPLE_INTERVAL
    assert job.monitorResourcesHistory == config.MONITOR_HISTORY_DEFAULT


def test_monitor_resources_map_form():
    job = _monitor_job(
        "    monitorResources:\n"
        "      interval: 0.25\n"
        "      history: 100\n"
    )
    # writing the map at all opts in (enabled defaults true)
    assert job.monitorResources is True
    assert job.monitorResourcesInterval == 0.25
    assert job.monitorResourcesHistory == 100


def test_monitor_resources_map_enabled_false():
    job = _monitor_job(
        "    monitorResources:\n"
        "      enabled: false\n"
        "      interval: 0.5\n"
    )
    assert job.monitorResources is False


def test_monitor_resources_history_zero_allowed():
    # 0 = summary numbers only, no chart series
    job = _monitor_job("    monitorResources:\n      history: 0\n")
    assert job.monitorResources is True
    assert job.monitorResourcesHistory == 0


def test_monitor_resources_defaults_block_merges_with_job_map():
    conf = config.parse_config_string(
        "defaults:\n"
        "  monitorResources:\n"
        "    interval: 0.5\n"
        "jobs:\n"
        "  - name: mon\n"
        "    command: echo hi\n"
        '    schedule: "* * * * *"\n'
        "    monitorResources:\n"
        "      history: 50\n",
        "",
    )
    job = conf.jobs[0]
    # both map forms merge key-wise (mergedicts), like every nested section
    assert job.monitorResources is True
    assert job.monitorResourcesInterval == 0.5
    assert job.monitorResourcesHistory == 50


def test_monitor_resources_interval_floor():
    with pytest.raises(ConfigError, match="monitorResources.interval"):
        _monitor_job("    monitorResources:\n      interval: 0.01\n")


def test_monitor_resources_history_ceiling():
    with pytest.raises(ConfigError, match="monitorResources.history"):
        _monitor_job("    monitorResources:\n      history: 5000\n")


# ---- web.nodeHistory validation ---------------------------------------------


def _web_config(snippet):
    return config.parse_config_string(
        "web:\n  listen:\n    - http://127.0.0.1:8080\n" + snippet, ""
    ).web_config


def test_web_node_history_forms_parse():
    assert _web_config("")["listen"]  # no nodeHistory key at all
    assert _web_config("  nodeHistory: false\n")["nodeHistory"] is False
    cfg = _web_config("  nodeHistory:\n    interval: 2.0\n    points: 120\n")
    assert cfg["nodeHistory"] == {"interval": 2.0, "points": 120}


def test_web_node_history_interval_floor():
    with pytest.raises(ConfigError, match="nodeHistory.interval"):
        _web_config("  nodeHistory:\n    interval: 0.5\n")


def test_web_node_history_points_range():
    with pytest.raises(ConfigError, match="nodeHistory.points"):
        _web_config("  nodeHistory:\n    points: 5\n")
    with pytest.raises(ConfigError, match="nodeHistory.points"):
        _web_config("  nodeHistory:\n    points: 999999\n")
