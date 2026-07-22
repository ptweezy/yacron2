import os
from types import SimpleNamespace

import pytest

from cronstable import config
from cronstable.config import (
    DEFAULT_CONFIG,
    ConfigError,
    DagConfig,
    JobConfig,
    mergedicts,
    parse_config_string,
)
from cronstable.platform import IS_WINDOWS


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


def test_parse_config_dir_cache_validates_content_not_mtime(tmp_path):
    # The per-file dir cache must reuse a parse only when the bytes are
    # unchanged, NOT merely when (mtime_ns, size) match. A size-preserving
    # edit whose mtime is then pinned back (coarse-granularity network FS,
    # rsync -a, cp --preserve=timestamps, backup-restore) must still be
    # picked up -- otherwise a reparse triggered by a neighbouring file
    # would keep serving the stale schedule the pre-cache full reparse read.
    import os

    a = tmp_path / "a.yaml"
    a.write_text(
        'jobs:\n  - name: bak\n    command: echo x\n    schedule: "0 3 * * *"\n'
    )
    first = config.parse_config_with_sources(str(tmp_path))[0]
    assert str(first.jobs[0].schedule) == "0 3 * * *"
    st = os.stat(a)

    # "0 3" -> "0 5": identical byte length; then pin mtime + size back
    a.write_text(
        'jobs:\n  - name: bak\n    command: echo x\n    schedule: "0 5 * * *"\n'
    )
    os.utime(a, ns=(st.st_atime_ns, st.st_mtime_ns))
    again = os.stat(a)
    assert (again.st_mtime_ns, again.st_size) == (st.st_mtime_ns, st.st_size)

    second = config.parse_config_with_sources(str(tmp_path))[0]
    assert str(second.jobs[0].schedule) == "0 5 * * *"  # not the stale 0 3


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
    # silently keep cronstable's (root) gid. (The schema is Int() | Str(), so a
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
# web.metrics (the native Prometheus endpoint; see cronstable/prometheus.py)
# ---------------------------------------------------------------------------

_WEB_METRICS_BASE = """
web:
  listen:
    - http://127.0.0.1:8080
"""


def test_web_metrics_absent_by_default():
    conf = config.parse_config_string(_WEB_METRICS_BASE, "")
    # the key is simply absent; enabled-by-default is applied at the use
    # site (cronstable.prometheus.resolve_metrics_config)
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
    from cronstable.cronexpr import CronTab

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
def test_blank_second_is_rejected_not_column_shifted(blank):
    # A blank/whitespace `second:` value used to render a vanishing column
    # (so has_seconds quietly read False); but the same collapse silently
    # shifted every LATER field one column left -- a 60x fire-rate change
    # with no error -- so the renderer now rejects any value that is not
    # exactly one whitespace-free token, naming the offending key.
    with pytest.raises(ConfigError, match="schedule.second"):
        _one_job(
            "    schedule:\n      second: {}\n      minute: \"5\"\n".format(
                blank
            )
        )


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


# ---- _resolve_secret hardening ----------------------------------------------


def test_resolve_secret_binary_file_is_config_error(tmp_path):
    # A fromFile pointing at binary data (a .p12 bundle, a gzip, a key with a
    # stray high byte) raises UnicodeDecodeError from read(). It must surface
    # as ConfigError -- the only exception callers handle: the job-secret
    # staging path (cron._prepare_job_api_run) then skips the secret with a
    # warning, instead of the raw UnicodeDecodeError escaping the scheduler
    # loop and crash-looping the daemon at every fire of that job.
    blob = tmp_path / "secret.bin"
    # invalid in UTF-8 (lone continuation bytes), cp1252 (0x9d undefined) and,
    # via the dangling 0xff lead at EOF, the common CJK multibyte codecs too.
    blob.write_bytes(b"\x9d\x80\x00\xff")
    with pytest.raises(ConfigError, match="could not be read"):
        config._resolve_secret({"fromFile": str(blob)}, "job j secret s")


def test_resolve_secret_missing_file_is_config_error(tmp_path):
    with pytest.raises(ConfigError, match="could not be read"):
        config._resolve_secret(
            {"fromFile": str(tmp_path / "nope")}, "job j secret s"
        )


def test_web_allowed_origins_parse():
    cfg = _web_config("  allowedOrigins:\n    - https://dash.example\n")
    assert cfg["allowedOrigins"] == ["https://dash.example"]

# ===========================================================================
# Parse edges: schedule/timezone failures, section merge conflicts,
# cluster/state/mcp guards, and the pure address/secret helpers.
# ===========================================================================

# ---------------------------------------------------------------------------
# schedule / timezone edges
# ---------------------------------------------------------------------------


def test_schedule_has_seconds_non_schedule_type():
    assert config.schedule_has_seconds(None) is False
    assert config.schedule_has_seconds(1234) is False


def test_schedule_invalid_type_is_config_error():
    job_dict = mergedicts(
        DEFAULT_CONFIG,
        {"name": "j", "command": "true", "schedule": 1234},
    )
    with pytest.raises(ConfigError, match="invalid schedule"):
        JobConfig(job_dict)


def test_unknown_timezone_is_config_error():
    yaml = (
        "jobs:\n"
        "  - name: j\n"
        "    command: true\n"
        "    schedule: '* * * * *'\n"
        "    timezone: Not/AZone\n"
    )
    with pytest.raises(ConfigError, match="unknown timezone"):
        parse_config_string(yaml, "")


def test_dag_level_job_error_names_the_dag():
    yaml = (
        "dags:\n"
        "  - name: etl\n"
        "    schedule: '0 2 * * *'\n"
        "    timezone: Not/AZone\n"
        "    tasks:\n"
        "      - id: a\n"
        "        command: 'true'\n"
    )
    with pytest.raises(ConfigError, match="dag 'etl'.*unknown timezone"):
        parse_config_string(yaml, "")


# ---------------------------------------------------------------------------
# cluster validation guards
# ---------------------------------------------------------------------------

_TLS = "  tls:\n    ca: /ca\n    cert: /cert\n    key: /key\n"


def _cluster_yaml(extra):
    return (
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n" + _TLS + "  peers:\n"
        "    - host: b:8443\n" + extra
    )


def test_cluster_connect_timeout_must_be_positive():
    with pytest.raises(ConfigError, match="connectTimeout must be > 0"):
        parse_config_string(_cluster_yaml("  connectTimeout: 0\n"), "")


def test_cluster_ipv6_peer_needs_valid_port():
    yaml = (
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n" + _TLS + "  peers:\n"
        "    - host: '[::1]:notaport'\n"
    )
    with pytest.raises(ConfigError, match=r"must be \[ipv6\]:port"):
        parse_config_string(yaml, "")


def test_observability_mesh_tuning_keys_forwarded():
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: node-a\n"
        "  observability:\n"
        "    listen: '0.0.0.0:8140'\n"
        "    tls:\n      ca: /oca\n      cert: /ocert\n      key: /okey\n"
        "    peers:\n      - host: b:8140\n"
        "    nodeName: obs-a\n"
        "    interval: 7\n"
    )
    cfg = parse_config_string(yaml, "").cluster_config
    mesh = cfg["observabilityMesh"]
    assert mesh["nodeName"] == "obs-a"
    assert mesh["interval"] == 7


def test_kubernetes_lease_namespace_charset():
    yaml = (
        "cluster:\n"
        "  backend: kubernetes\n"
        "  nodeName: node-a\n"
        "  kubernetes:\n"
        "    leaseNamespace: 'Bad_NS'\n"
    )
    with pytest.raises(ConfigError, match="leaseNamespace must be a valid"):
        parse_config_string(yaml, "")


# ---------------------------------------------------------------------------
# state.jobApi validation guards
# ---------------------------------------------------------------------------


def _state_yaml(extra):
    return "state:\n  path: /tmp/st\n  jobApi:\n    enabled: true\n" + extra


def test_job_api_max_value_bytes_negative():
    with pytest.raises(ConfigError, match="maxValueBytes must be >= 0"):
        parse_config_string(_state_yaml("    maxValueBytes: -1\n"), "")


def test_job_api_max_artifact_bytes_negative():
    with pytest.raises(ConfigError, match="maxArtifactBytes must be >= 0"):
        parse_config_string(_state_yaml("    maxArtifactBytes: -1\n"), "")


def test_job_api_listen_port_out_of_range():
    with pytest.raises(ConfigError, match="invalid port"):
        parse_config_string(
            _state_yaml("    listen: 'http://127.0.0.1:70000'\n"), ""
        )


# ---------------------------------------------------------------------------
# web metrics / mcp cross-checks
# ---------------------------------------------------------------------------


def test_web_metrics_map_without_buckets_keeps_defaults():
    yaml = (
        "web:\n"
        "  listen:\n    - http://127.0.0.1:8080\n"
        "  metrics:\n    public: true\n"
    )
    conf = parse_config_string(yaml, "")
    assert conf.web_config["metrics"]["public"] is True


def test_mcp_act_with_read_only_warns_but_loads(caplog):
    yaml = (
        "web:\n  listen:\n    - http://127.0.0.1:8080\n"
        "mcp:\n  enabled: true\n"
        "  toolsets:\n    - observe\n    - act\n"
    )
    with caplog.at_level("WARNING", logger="cronstable.config"):
        conf = parse_config_string(yaml, "")
        config._validate_cross_sections(conf)
    assert conf.mcp_config["enabled"] is True
    assert "mutating tools stay suppressed" in caplog.text


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------


def test_resolve_secret_unset_is_none():
    assert config._resolve_secret(None, "x") is None
    assert config._resolve_secret({}, "x") is None


def test_likely_self_fqdn_exact_match_is_not_degenerate():
    # an exact self-listing was already dropped by _is_self_listed; the
    # fuzzy FQDN heuristic must not re-flag it.
    assert (
        config._likely_self_fqdn("node1:8443", "0.0.0.0:8443", "node1")
        is False
    )


def test_is_local_listener_forms():
    assert config._is_local_listener("unix:///tmp/x.sock") is True
    assert config._is_local_listener("localhost:8080") is True
    assert config._is_local_listener("127.0.0.1:8080") is True
    # a hostname is not an IP literal: fails the loopback check closed
    assert config._is_local_listener("example.com:8080") is False


# ---------------------------------------------------------------------------
# include: per-section merge + conflicts
# ---------------------------------------------------------------------------

_CHILD_ALL = (
    "web:\n  listen:\n    - http://127.0.0.1:8080\n"
    "cluster:\n"
    "  listen: '0.0.0.0:8443'\n" + _TLS + "  peers:\n"
    "    - host: b:8443\n"
    "state:\n  path: /tmp/st\n"
    "mcp:\n  enabled: true\n"
    "logging:\n  version: 1\n"
)


def _write(path, text):
    path.write_text(text)
    return str(path)


def test_include_adopts_child_sections(tmp_path):
    _write(tmp_path / "child.yaml", _CHILD_ALL)
    parent = _write(tmp_path / "parent.yaml", "include:\n  - child.yaml\n")
    conf = config.parse_config_file(parent)
    assert conf.web_config is not None
    assert conf.cluster_config is not None
    assert conf.state_config is not None
    assert conf.mcp_config["enabled"] is True
    assert conf.logging_config is not None


@pytest.mark.parametrize(
    "section,inline",
    [
        ("web", "web:\n  listen:\n    - http://127.0.0.1:9090\n"),
        (
            "cluster",
            "cluster:\n"
            "  listen: '0.0.0.0:9443'\n" + _TLS + "  peers:\n"
            "    - host: c:9443\n",
        ),
        ("mcp", "mcp:\n  enabled: false\n"),
        ("logging", "logging:\n  version: 1\n"),
    ],
)
def test_include_duplicate_section_conflicts(tmp_path, section, inline):
    _write(tmp_path / "child.yaml", _CHILD_ALL)
    parent = _write(
        tmp_path / "parent.yaml", inline + "include:\n  - child.yaml\n"
    )
    with pytest.raises(
        ConfigError, match="multiple {} configs".format(section)
    ):
        config.parse_config_file(parent)


# ---------------------------------------------------------------------------
# directory loading: per-section conflicts + unreadable files
# ---------------------------------------------------------------------------


def test_parse_config_dir_multiple_cluster(tmp_path):
    cluster = (
        "cluster:\n"
        "  listen: '0.0.0.0:8443'\n" + _TLS + "  peers:\n"
        "    - host: b:8443\n"
    )
    _write(tmp_path / "a.yaml", cluster)
    _write(tmp_path / "b.yaml", cluster)
    with pytest.raises(ConfigError, match="Multiple 'cluster'"):
        config.parse_config(str(tmp_path))


def test_parse_config_dir_multiple_mcp(tmp_path):
    _write(tmp_path / "a.yaml", "mcp:\n  enabled: true\n")
    _write(tmp_path / "b.yaml", "mcp:\n  enabled: false\n")
    with pytest.raises(ConfigError, match="Multiple 'mcp'"):
        config.parse_config(str(tmp_path))


def test_parse_config_dir_multiple_logging(tmp_path):
    _write(tmp_path / "a.yaml", "logging:\n  version: 1\n")
    _write(tmp_path / "b.yaml", "logging:\n  version: 1\n")
    with pytest.raises(ConfigError, match="Multiple 'logging'"):
        config.parse_config(str(tmp_path))


def test_parse_config_dir_records_oserror(tmp_path, monkeypatch):
    _write(tmp_path / "a.yaml", "jobs: []\n")
    real = config.parse_config_file

    def flaky(path, *args, **kwargs):
        if path.endswith("a.yaml"):
            raise OSError("disk on fire")
        return real(path, *args, **kwargs)

    monkeypatch.setattr(config, "parse_config_file", flaky)
    # the collected per-file errors surface as one aggregate ConfigError
    with pytest.raises(ConfigError, match="disk on fire"):
        config.parse_config(str(tmp_path))


# ---------------------------------------------------------------------------
# dag + state/etcd builder error branches
# ---------------------------------------------------------------------------


def test_dag_task_wraps_job_template_config_error():
    # a launch field that JobConfig rejects (executionTimeout must be > 0)
    # surfaces wrapped with the dag/task context, not as a bare Job error.
    with pytest.raises(ConfigError, match=r"dag 'd': task 't1':"):
        DagConfig(
            {
                "name": "d",
                "tasks": [
                    {"id": "t1", "command": "true", "executionTimeout": 0}
                ],
            }
        )


def test_dag_requires_at_least_one_task():
    with pytest.raises(ConfigError, match="needs at least one task"):
        DagConfig({"name": "d", "tasks": []})


def test_state_jobapi_listen_invalid_port_rejected():
    # a listen authority whose port is out of range makes urlparse.port raise
    # ValueError; the state builder turns that into a clean ConfigError instead
    # of letting it escape and permanently disable the loopback endpoint.
    with pytest.raises(ConfigError, match="invalid port"):
        parse_config_string(
            "state:\n  path: /x\n  jobApi:\n    listen: 127.0.0.1:70000\n", ""
        )


def test_etcd_endpoints_must_be_non_empty():
    # an etcd cluster with an empty endpoints list has nothing to talk to.
    with pytest.raises(ConfigError, match="at least one URL"):
        config._build_etcd_cluster_config(
            {"backend": "etcd", "nodeName": "node-a", "etcd": {"endpoints": []}}
        )


# ---------------------------------------------------------------------------
# fuzzing findings: schedule-object token validation, _resolve_secret's
# except tuple, and strictyaml's bare-ValueError escape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec, key",
    [
        ({"second": "0", "year": ""}, "year"),  # blank drops a column
        ({"minute": ""}, "minute"),
        ({"minute": " "}, "minute"),
        ({"minute": "0 12"}, "minute"),  # embedded space adds a column
        ({"dayOfWeek": "* 2030"}, "dayOfWeek"),  # would pin the YEAR
        ({"minute": "0\xa0"}, "minute"),  # NBSP: str.split() eats it too
        ({"minute": "0 "}, "minute"),
        ({"hour": "1\t2"}, "hour"),
    ],
)
def test_schedule_object_rejects_blank_or_split_values(spec, key):
    # the rendered line is re-read under a whitespace split, so a blank
    # value used to DELETE its column and an embedded space to ADD one --
    # silently shifting every later field (second: "0" + a leftover year:
    # turned "every second at :00" into "hourly at minute 0", a 60x rate
    # change with no error and no lint finding).
    with pytest.raises(ConfigError, match="schedule.{}".format(key)):
        config.schedule_object_to_crontab(spec)


def test_schedule_object_valid_forms_render_exactly_as_before():
    assert config.schedule_object_to_crontab({}) == "* * * * *"
    assert config.schedule_object_to_crontab({"minute": "*/5"}) == (
        "*/5 * * * *"
    )
    assert config.schedule_object_to_crontab({"second": "0"}) == (
        "0 * * * * * *"
    )
    assert config.schedule_object_to_crontab({"year": "2030"}) == (
        "* * * * * 2030"
    )
    assert config.schedule_object_to_crontab(
        {"second": "30", "year": "2030"}
    ) == "30 * * * * * 2030"
    assert config.schedule_has_seconds({"second": "0"}) is True
    assert config.schedule_has_seconds({"minute": "5"}) is False


def test_blank_schedule_object_value_is_config_error_from_yaml():
    # reachable straight from a config file: a leftover `year:` with no
    # value used to silently retarget the second column into the minute
    # column; now it is a parse-time ConfigError naming the key.
    with pytest.raises(ConfigError, match="schedule.year"):
        parse_config_string(
            "jobs:\n"
            "  - name: heartbeat\n"
            "    command: echo tick\n"
            "    schedule:\n"
            '      second: "0"\n'
            "      year:\n",
            "",
        )


def test_resolve_secret_nul_and_surrogate_sources_are_config_errors():
    # open() raises ValueError for a NUL in the path and UnicodeEncodeError
    # for a lone surrogate; os.environ.get raises UnicodeEncodeError too.
    # None is an OSError, and on the per-fire job-secret staging path
    # anything but ConfigError crash-loops the daemon at every fire.
    with pytest.raises(ConfigError, match="fromFile could not be read"):
        config._resolve_secret({"name": "s", "fromFile": "a\x00b"}, "what")
    with pytest.raises(ConfigError, match="fromFile could not be read"):
        config._resolve_secret(
            {"name": "s", "fromFile": "/tmp/A\ud800.txt"}, "what"
        )
    # POSIX encodes environ to bytes, so a lone surrogate in the name fails
    # to encode; Windows environ is native UTF-16, where that name is merely
    # absent and the empty result fails closed on the same ConfigError path.
    with pytest.raises(
        ConfigError,
        match=(
            "resolved to an empty secret"
            if IS_WINDOWS
            else "fromEnvVar could not be read"
        ),
    ):
        config._resolve_secret(
            {"name": "s", "fromEnvVar": "A\ud800B"}, "what"
        )


@pytest.mark.parametrize(
    "yaml_escape_block",
    [
        # config-parse route, from a PURE-ASCII file via YAML \u escapes
        '    password:\n      fromFile: "/tmp/A\\uD800.txt"\n',
        '    password:\n      fromEnvVar: "A\\uD800B"\n',
        '    password:\n      fromFile: "a\\u0000b"\n',
    ],
)
def test_resolve_secret_yaml_escaped_sources_stay_config_errors(
    yaml_escape_block,
):
    yaml = (
        "cluster:\n  backend: etcd\n  etcd:\n"
        "    endpoints:\n      - https://127.0.0.1:2379\n"
        "    username: u\n" + yaml_escape_block
    )
    with pytest.raises(ConfigError):
        parse_config_string(yaml, "cronstable.yaml")


@pytest.mark.parametrize(
    "snippet, label",
    [
        ("    executionTimeout:\n", "empty Float"),
        ('    killTimeout: "."\n', "lone dot Float"),
        ('    saveLimit: "_"\n', "underscore Int"),
        ('    catchupJitterSeconds: "__"\n', "double underscore Int"),
    ],
)
def test_strictyaml_scalar_validator_valueerror_is_config_error(
    snippet, label
):
    # strictyaml's is_decimal/is_integer prefilters accept '', '.', '_' ...
    # which float()/int() then reject with a BARE ValueError -- previously
    # escaping parse_config_string entirely: a raw traceback at startup and
    # a 'please report this as a bug' log on every reload, for an ordinary
    # config typo.  ~26 Float()/Int() keys shared this escape.
    yaml = (
        "jobs:\n  - name: j\n    command: x\n"
        '    schedule: "* * * * *"\n' + snippet
    )
    with pytest.raises(ConfigError):
        parse_config_string(yaml, "t.yaml")


def test_control_character_in_config_is_config_error_not_attributeerror():
    # strictyaml's reader raises AttributeError (its ReaderError lacks
    # context_mark) for a control character in the file; it must surface as
    # ConfigError so a config-directory load reports the one bad file
    # instead of aborting wholesale.
    try:
        parse_config_string("jobs:\x07\n", "t.yaml")
    except ConfigError:
        pass  # the expected translation
    # any other exception type propagates and fails the test


# ---------------------------------------------------------------------------
# sla + onLate (per-job SLA thresholds and the late-report hook)
# ---------------------------------------------------------------------------


def _sla_job(snippet):
    conf = config.parse_config_string(
        "jobs:\n"
        "  - name: sla-job\n"
        "    command: echo hi\n"
        '    schedule: "* * * * *"\n' + snippet,
        "",
    )
    return conf.jobs[0]


def test_sla_defaults_off():
    job = _sla_job("")
    assert job.sla == {
        "maxTimeSinceSuccessSeconds": None,
        "lateAfterSeconds": None,
        "maxRuntimeSeconds": None,
    }


def test_sla_partial_block_merges_over_defaults():
    job = _sla_job("    sla:\n      lateAfterSeconds: 300\n")
    assert job.sla == {
        "maxTimeSinceSuccessSeconds": None,
        "lateAfterSeconds": 300,
        "maxRuntimeSeconds": None,
    }


def test_sla_all_keys_parse():
    job = _sla_job(
        "    sla:\n"
        "      maxTimeSinceSuccessSeconds: 86400\n"
        "      lateAfterSeconds: 300\n"
        "      maxRuntimeSeconds: 1200\n"
    )
    assert job.sla == {
        "maxTimeSinceSuccessSeconds": 86400,
        "lateAfterSeconds": 300,
        "maxRuntimeSeconds": 1200,
    }


def test_sla_empty_value_means_off():
    # the EmptyNone() arm: an empty scalar disables the check, matching
    # startingDeadlineSeconds
    job = _sla_job("    sla:\n      lateAfterSeconds:\n")
    assert job.sla["lateAfterSeconds"] is None


def test_sla_inherited_from_defaults_and_overridable():
    conf = config.parse_config_string(
        """
defaults:
  sla:
    maxTimeSinceSuccessSeconds: 3600

jobs:
  - name: inherits
    command: foo
    schedule: "* * * * *"
  - name: overrides
    command: bar
    schedule: "* * * * *"
    sla:
      maxTimeSinceSuccessSeconds: 60
      lateAfterSeconds: 30
""",
        "",
    )
    by_name = {j.name: j.sla for j in conf.jobs}
    assert by_name["inherits"] == {
        "maxTimeSinceSuccessSeconds": 3600,
        "lateAfterSeconds": None,
        "maxRuntimeSeconds": None,
    }
    assert by_name["overrides"] == {
        "maxTimeSinceSuccessSeconds": 60,
        "lateAfterSeconds": 30,
        "maxRuntimeSeconds": None,
    }


@pytest.mark.parametrize(
    "key",
    [
        "maxTimeSinceSuccessSeconds",
        "lateAfterSeconds",
        "maxRuntimeSeconds",
    ],
)
@pytest.mark.parametrize("value", [0, -5])
def test_sla_values_must_be_positive(key, value):
    with pytest.raises(ConfigError, match="sla.{} must be > 0".format(key)):
        _sla_job("    sla:\n      {}: {}\n".format(key, value))


def test_onlate_defaults_use_late_templates():
    report = config.DEFAULT_CONFIG["onLate"]["report"]
    assert report["mail"]["subject"] == config.DEFAULT_LATE_SUBJECT_TEMPLATE
    assert report["mail"]["body"] == config.DEFAULT_LATE_BODY_TEMPLATE
    assert report["webhook"]["body"] == (
        config.DEFAULT_LATE_WEBHOOK_BODY_TEMPLATE
    )
    assert report["sentry"]["fingerprint"] == [
        "cronstable",
        "sla",
        "{{ name }}",
    ]
    # everything else keeps the standard report defaults
    assert report["shell"] == config._REPORT_DEFAULTS["shell"]
    assert report["mail"]["smtpPort"] == (
        config._REPORT_DEFAULTS["mail"]["smtpPort"]
    )
    # and the shared defaults were not repointed at the late templates
    assert config._REPORT_DEFAULTS["mail"]["subject"] == (
        config.DEFAULT_SUBJECT_TEMPLATE
    )


def test_onlate_report_defaults_not_aliased():
    # the onLate report block must be an independent object like the other
    # three, so mutating one cannot corrupt the others.
    job = _sla_job("")
    assert (
        job.onLate["report"]["sentry"]["fingerprint"]
        is not job.onFailure["report"]["sentry"]["fingerprint"]
    )
    assert (
        job.onLate["report"]["sentry"]["fingerprint"]
        is not job.onSuccess["report"]["sentry"]["fingerprint"]
    )


def test_onlate_report_merges_over_late_defaults():
    job = _sla_job(
        "    sla:\n"
        "      lateAfterSeconds: 300\n"
        "    onLate:\n"
        "      report:\n"
        "        mail:\n"
        "          from: example@foo.com\n"
        "          to: example@bar.com\n"
    )
    mail = job.onLate["report"]["mail"]
    assert mail["from"] == "example@foo.com"
    assert mail["to"] == "example@bar.com"
    # unset keys inherit the late-specific defaults
    assert mail["subject"] == config.DEFAULT_LATE_SUBJECT_TEMPLATE
    assert mail["body"] == config.DEFAULT_LATE_BODY_TEMPLATE


def test_sla_and_onlate_via_defaults_block():
    conf = config.parse_config_string(
        """
defaults:
  sla:
    lateAfterSeconds: 300
  onLate:
    report:
      mail:
        from: example@foo.com
        to: example@bar.com

jobs:
  - name: test
    command: foo
    schedule: "* * * * *"
""",
        "",
    )
    job = conf.jobs[0]
    assert job.sla["lateAfterSeconds"] == 300
    assert job.onLate["report"]["mail"]["to"] == "example@bar.com"


@pytest.mark.parametrize(
    "snippet",
    [
        "        sentry:\n          dsn:\n            value: https://k@e/1\n",
        (
            "        sentry:\n          dsn:\n"
            "            fromEnvVar: SENTRY_DSN\n"
        ),
        "        mail:\n          from: a@b.com\n          to: c@d.com\n",
        "        shell:\n          command: notify\n",
        "        webhook:\n          url:\n            value: https://h/x\n",
        "        webhook:\n          url:\n            fromFile: /run/hook\n",
    ],
)
def test_onlate_without_sla_is_rejected(snippet):
    # a configured onLate reporter with no sla thresholds would never fire;
    # that is a config mistake, not a silent no-op.
    with pytest.raises(ConfigError, match="onLate requires sla"):
        _sla_job("    onLate:\n      report:\n" + snippet)


def test_onlate_with_sla_is_accepted():
    job = _sla_job(
        "    sla:\n"
        "      maxRuntimeSeconds: 600\n"
        "    onLate:\n"
        "      report:\n"
        "        shell:\n"
        "          command: notify\n"
    )
    assert job.onLate["report"]["shell"]["command"] == "notify"
    assert job.sla["maxRuntimeSeconds"] == 600


def test_onlate_with_no_reporter_configured_needs_no_sla():
    # an onLate block whose reporters are all left at their null defaults
    # would never fire, so it does not demand an sla block (a defaults block
    # may carry an onLate skeleton that only some jobs activate).
    job = _sla_job(
        "    onLate:\n"
        "      report:\n"
        "        mail:\n"
        "          from:\n"
        "          to:\n"
    )
    assert job.sla["lateAfterSeconds"] is None


# --- ${ENV} interpolation over the validated config document -----------------


def test_env_interp_basic_and_default(monkeypatch):
    monkeypatch.setenv("PORT", "9000")
    monkeypatch.delenv("BIND_HOST", raising=False)
    conf = config.parse_config_string(
        """
web:
  listen:
    - "0.0.0.0:${PORT}"
    - "${BIND_HOST:-127.0.0.1}:8080"
""",
        "test.yaml",
    )
    assert conf.web_config["listen"] == ["0.0.0.0:9000", "127.0.0.1:8080"]


def test_env_interp_in_job_scalar_fields(monkeypatch):
    monkeypatch.setenv("ENVNAME", "staging")
    monkeypatch.setenv("TZONE", "Europe/Berlin")
    conf = config.parse_config_string(
        """
jobs:
  - name: sync-${ENVNAME}
    command: echo hi
    schedule:
      minute: "*"
    timezone: ${TZONE}
""",
        "test.yaml",
    )
    job = conf.jobs[0]
    assert job.name == "sync-staging"
    assert str(job.timezone) == "Europe/Berlin"


def test_env_interp_skips_command_and_shell(monkeypatch):
    # A ${VAR} inside a job command or the shell interpreter belongs to the
    # runtime shell and must survive verbatim -- even when the variable is
    # unset (so it is never a load-time error either).
    monkeypatch.delenv("HOME_DIR", raising=False)
    monkeypatch.delenv("MY_SHELL", raising=False)
    conf = config.parse_config_string(
        """
jobs:
  - name: n
    shell: ${MY_SHELL}
    command:
      echo ${HOME_DIR}
    schedule:
      minute: "*"
""",
        "test.yaml",
    )
    job = conf.jobs[0]
    assert job.command == "echo ${HOME_DIR}"
    assert job.shell == "${MY_SHELL}"


def test_env_interp_skips_shell_reporter_command(monkeypatch):
    # The shell reporter runs a command too; the whole `report.shell` block is
    # left untouched, so an unset ${VAR} there is not a load-time error, while
    # a sibling webhook URL still expands.
    monkeypatch.setenv("HOOK", "abc123")
    monkeypatch.delenv("PAGER_CMD_ARG", raising=False)
    conf = config.parse_config_string(
        """
jobs:
  - name: n
    command: c
    schedule:
      minute: "*"
    onFailure:
      report:
        shell:
          command: page ${PAGER_CMD_ARG}
        webhook:
          url:
            value: https://hooks.example/${HOOK}
""",
        "test.yaml",
    )
    report = conf.jobs[0].onFailure["report"]
    assert report["shell"]["command"] == "page ${PAGER_CMD_ARG}"
    assert report["webhook"]["url"]["value"] == "https://hooks.example/abc123"


def test_env_interp_unset_without_default_raises_naming_key(monkeypatch):
    monkeypatch.delenv("NEEDED_PORT", raising=False)
    with pytest.raises(ConfigError) as exc:
        config.parse_config_string(
            """
web:
  listen:
    - "0.0.0.0:${NEEDED_PORT}"
""",
            "prod.yaml",
        )
    msg = str(exc.value)
    # names the missing variable, the config location, and the source file
    assert "NEEDED_PORT" in msg
    assert "web.listen[0]" in msg
    assert "prod.yaml" in msg


def test_env_interp_dollar_escape(monkeypatch):
    monkeypatch.setenv("X", "val")
    conf = config.parse_config_string(
        """
jobs:
  - name: n
    command: c
    schedule:
      minute: "*"
    streamPrefix: "$$5 and $${X} literal but ${X} expands"
""",
        "test.yaml",
    )
    assert conf.jobs[0].streamPrefix == "$5 and ${X} literal but val expands"


def test_env_interp_default_used_only_when_unset_or_empty(monkeypatch):
    # `:-` mirrors the shell: default when unset OR set-but-empty; a bare
    # ${VAR} on a set-but-empty variable expands to empty (never an error).
    monkeypatch.setenv("EMPTY", "")
    monkeypatch.setenv("SET", "here")
    conf = config.parse_config_string(
        """
jobs:
  - name: n
    command: c
    schedule:
      minute: "*"
    streamPrefix: "[${EMPTY}][${EMPTY:-fb}][${SET:-fb}]"
""",
        "test.yaml",
    )
    assert conf.jobs[0].streamPrefix == "[][fb][here]"


def test_env_interp_leaves_unbraced_and_malformed_literal(monkeypatch):
    # Only the braced forms are recognised; a bare $VAR, a lone $, and a
    # malformed ${...} are passed through untouched so pre-existing configs
    # that never used the syntax keep their exact values.
    monkeypatch.delenv("VAR", raising=False)
    conf = config.parse_config_string(
        """
jobs:
  - name: n
    command: c
    schedule:
      minute: "*"
    streamPrefix: "price $5, $VAR, ${unclosed and a{brace}"
""",
        "test.yaml",
    )
    assert (
        conf.jobs[0].streamPrefix == "price $5, $VAR, ${unclosed and a{brace}"
    )


def test_env_interp_expands_state_path_and_include_path(monkeypatch, tmp_path):
    # A whole section (state.path) and an include path can both come from the
    # environment; the included file is resolved after expansion.
    monkeypatch.setenv("STATE_DIR", str(tmp_path / "state"))
    (tmp_path / "extra.yaml").write_text(
        "jobs:\n  - name: extra\n    command: c\n    schedule:\n"
        "      minute: '*'\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXTRA", "extra.yaml")
    main = tmp_path / "main.yaml"
    main.write_text(
        "state:\n  path: ${STATE_DIR}\ninclude:\n  - ${EXTRA}\n"
        "jobs:\n  - name: inline\n    command: c\n    schedule:\n"
        "      minute: '*'\n",
        encoding="utf-8",
    )
    conf = config.parse_config(str(main))
    assert conf.state_config["path"] == str(tmp_path / "state")
    assert sorted(j.name for j in conf.jobs) == ["extra", "inline"]


def test_env_interp_empty_config_is_noop(monkeypatch):
    assert config.parse_config_string("", "test.yaml").jobs == []


def test_env_interp_skips_logging_section(monkeypatch):
    # The `logging` section is handed to logging.config.dictConfig verbatim; a
    # `$`-style formatter legitimately writes ${asctime}/${message} in its
    # format string, so the whole section must be left untouched (not treated
    # as unset environment variables, which would fail an otherwise valid
    # config to load).
    monkeypatch.delenv("asctime", raising=False)
    monkeypatch.delenv("message", raising=False)
    conf = config.parse_config_string(
        """
logging:
  version: 1
  formatters:
    tmpl:
      style: "$"
      format: "${asctime} ${levelname} ${message}"
  root:
    level: INFO
jobs:
  - name: j
    command: c
    schedule:
      minute: "*"
""",
        "test.yaml",
    )
    fmt = conf.logging_config["formatters"]["tmpl"]["format"]
    assert fmt == "${asctime} ${levelname} ${message}"


def test_env_interp_in_defaults_block_inherited(monkeypatch):
    # A ${VAR} in a `defaults:` value expands and is inherited by jobs; the
    # `shell` default is skipped like any other shell field.
    monkeypatch.setenv("PREFIX", "east-")
    conf = config.parse_config_string(
        """
defaults:
  streamPrefix: ${PREFIX}
jobs:
  - name: j
    command: c
    schedule:
      minute: "*"
""",
        "test.yaml",
    )
    assert conf.jobs[0].streamPrefix == "east-"


def test_env_interp_skips_dag_task_command(monkeypatch):
    monkeypatch.setenv("TID", "load")
    monkeypatch.delenv("TASK_ARG", raising=False)
    conf = config.parse_config_string(
        """
state:
  path: /tmp/st
  jobApi:
    enabled: true
dags:
  - name: d
    schedule:
      minute: "*"
    tasks:
      - id: t-${TID}
        command: run ${TASK_ARG}
""",
        "test.yaml",
    )
    task = conf.dags[0].tasks[0]
    assert task.id == "t-load"
    assert task.job_template.command == "run ${TASK_ARG}"


def test_env_interp_expanded_value_feeds_section_validator(monkeypatch):
    # Expansion runs before the section builders, so an interpolated value that
    # is invalid is rejected by the normal validator (here: an empty state
    # path), not silently accepted.
    monkeypatch.setenv("SDIR", "")
    with pytest.raises(ConfigError, match="state.path is required"):
        config.parse_config_string(
            "state:\n  path: ${SDIR}\n", "test.yaml"
        )


def test_env_interp_unterminated_default_is_linear_not_quadratic():
    # A value carrying many unterminated ``${x:-`` fragments made the old
    # re.sub restart its ``[^}]*`` default scan to end-of-string at every
    # ``${`` offset: O(n^2), a config-load / --validate-config / hot-reload
    # stall.  The hand-written scanner keeps it amortised linear; assert via
    # growth ratio (~4x per 2x input before the fix, ~2x after) rather than
    # wall-clock, so a slow CI box cannot flake it.
    import time

    # unterminated (no closing ``}``): every fragment is left verbatim, and a
    # leading ``}`` sitting *behind* the fragments must not rescue the match.
    assert config._interpolate_env_value("${a:-" * 40, "p", "loc") == (
        "${a:-" * 40
    )
    assert config._interpolate_env_value("}" + "${a:-" * 40, "p", "loc") == (
        "}" + "${a:-" * 40
    )
    timings = []
    for n in (16_000, 32_000, 64_000):
        value = "${a:-" * n
        started = time.perf_counter()
        config._interpolate_env_value(value, "p", "loc")
        timings.append(time.perf_counter() - started)
    assert timings[2] < timings[0] * 8, timings  # quadratic would be ~16x


def test_env_interp_skip_is_structural_not_by_key_name(monkeypatch):
    # command/shell/logging are skipped only where they are runtime-shell or
    # logging.config territory (a job/task field, a shell reporter block, the
    # top-level logging section).  The very same names as user-chosen keys in
    # an arbitrary-key map (web.headers here) are ordinary values and must
    # interpolate like any other.
    monkeypatch.setenv("HDR", "expanded")
    conf = config.parse_config_string(
        "web:\n"
        "  listen:\n"
        '    - "127.0.0.1:8080"\n'
        "  headers:\n"
        '    command: "c=${HDR}"\n'
        '    shell: "s=${HDR}"\n'
        '    logging: "l=${HDR}"\n'
        '    normal: "n=${HDR}"\n',
        "test.yaml",
    )
    assert conf.web_config["headers"] == {
        "command": "c=expanded",
        "shell": "s=expanded",
        "logging": "l=expanded",
        "normal": "n=expanded",
    }


def test_env_interp_in_reporter_value_maps(monkeypatch):
    # Inside a report block only the shell reporter sub-block is skipped; a
    # webhook header or a sentry extra keyed command/shell still interpolates.
    monkeypatch.setenv("TOK", "xyz")
    conf = config.parse_config_string(
        "jobs:\n"
        "  - name: n\n"
        "    command: c\n"
        "    schedule:\n"
        '      minute: "*"\n'
        "    onFailure:\n"
        "      report:\n"
        "        webhook:\n"
        "          url:\n"
        "            value: https://h/x\n"
        "          headers:\n"
        '            command: "Bearer ${TOK}"\n'
        "        sentry:\n"
        "          dsn:\n"
        "            value: https://k@o/1\n"
        "          extra:\n"
        '            shell: "ctx-${TOK}"\n',
        "test.yaml",
    )
    report = conf.jobs[0].onFailure["report"]
    assert report["webhook"]["headers"]["command"] == "Bearer xyz"
    assert report["sentry"]["extra"]["shell"] == "ctx-xyz"


def test_env_interp_unset_under_value_map_key_named_shell_fails_fast(
    monkeypatch,
):
    # The over-broad skip used to swallow an unset ${VAR} under a header keyed
    # ``shell``; it now fail-fasts like any other value, naming the location.
    monkeypatch.delenv("MISSING", raising=False)
    with pytest.raises(ConfigError) as exc:
        config.parse_config_string(
            "web:\n"
            "  listen:\n"
            '    - "127.0.0.1:8080"\n'
            "  headers:\n"
            '    shell: "${MISSING}"\n',
            "prod.yaml",
        )
    msg = str(exc.value)
    assert "MISSING" in msg
    assert "web.headers.shell" in msg
    assert "prod.yaml" in msg


def test_env_interp_hostile_timezone_raises_configerror_not_oserror(
    monkeypatch,
):
    # ${ENV} interpolation lets an environment value flow into `timezone`;
    # an over-long / OS-invalid component makes ZoneInfo raise OSError
    # (ENAMETOOLONG / EINVAL), which _resolve_timezone must surface as a
    # ConfigError so config load only ever fails with ConfigError (never a raw
    # traceback that crashes startup / a hot-reload tick).
    monkeypatch.setenv("TZ_FROM_ENV", "A" * 4096)
    with pytest.raises(ConfigError, match="unknown timezone"):
        config.parse_config_string(
            "jobs:\n"
            "  - name: n\n"
            "    command: c\n"
            "    schedule:\n"
            '      minute: "*"\n'
            "    timezone: ${TZ_FROM_ENV}\n",
            "test.yaml",
        )
