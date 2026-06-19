import os

import pytest

from yacron2 import config
from yacron2.config import ConfigError


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


def test_report_defaults_not_aliased():
    # onFailure/onPermanentFailure/onSuccess report blocks must be independent
    # objects so mutating one cannot corrupt the others (or the global default).
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
    # web config from one file is not dropped in favour of the last file
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
