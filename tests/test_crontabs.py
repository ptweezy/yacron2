"""Classic (Vixie-style) crontab support: parsing, detection, integration."""

import datetime

import pytest

from cronstable import config, crontabs
from cronstable.config import ConfigError
from cronstable.cronexpr import CronTab


def test_basic_line_gets_standard_defaults():
    conf = config.parse_crontab_string(
        "*/15 * * * * /usr/local/bin/backup --incremental\n",
        "legacy.crontab",
    )
    assert conf.web_config is None
    assert conf.cluster_config is None
    assert conf.logging_config is None
    assert len(conf.jobs) == 1
    job = conf.jobs[0]
    assert job.name == "legacy.crontab:1"
    assert job.command == "/usr/local/bin/backup --incremental"
    assert job.schedule_unparsed == "*/15 * * * *"
    assert isinstance(job.schedule, CronTab)
    # the "reasonable standards" contract: a crontab entry is an ordinary
    # cronstable job carrying the standard defaults, not a cron emulation.
    assert job.shell == config.DEFAULT_CONFIG["shell"]
    assert job.captureStderr is True
    assert job.captureStdout is False
    assert job.concurrencyPolicy == "Allow"
    assert job.enabled is True
    assert job.environment == []
    assert job.timezone == datetime.timezone.utc
    assert job.failsWhen["nonzeroReturn"] is True
    assert job.failsWhen["producesStderr"] is True


def test_environment_is_positional():
    conf = config.parse_crontab_string(
        "0 1 * * * before\n"
        "PATH = /usr/local/bin:/usr/bin:/bin\n"
        'MAILTO="ops@example.com"\n'
        "0 2 * * * after\n",
        "jobs.crontab",
    )
    before, after = conf.jobs
    assert before.environment == []
    assert after.environment == [
        {"key": "PATH", "value": "/usr/local/bin:/usr/bin:/bin"},
        {"key": "MAILTO", "value": "ops@example.com"},
    ]


def test_environment_reassignment_snapshots():
    conf = config.parse_crontab_string(
        "FOO=one\n0 1 * * * first\nFOO=two\n0 2 * * * second\n",
        "jobs.crontab",
    )
    first, second = conf.jobs
    assert first.environment == [{"key": "FOO", "value": "one"}]
    assert second.environment == [{"key": "FOO", "value": "two"}]


def test_quoted_value_preserves_blanks():
    conf = config.parse_crontab_string(
        "GREETING=' hello world '\n* * * * * env\n", "t.crontab"
    )
    assert conf.jobs[0].environment == [
        {"key": "GREETING", "value": " hello world "}
    ]


def test_shell_assignment_maps_to_shell_setting():
    conf = config.parse_crontab_string(
        "SHELL=/bin/bash\n0 4 * * * echo hi\n", "t.crontab"
    )
    job = conf.jobs[0]
    assert job.shell == "/bin/bash"
    # like cron, the assignment also stays exported to the job.
    assert {"key": "SHELL", "value": "/bin/bash"} in job.environment


def test_cron_tz_sets_schedule_timezone():
    conf = config.parse_crontab_string(
        "0 6 * * * utc-job\n"
        "CRON_TZ=Europe/Berlin\n"
        "0 6 * * * berlin-job\n",
        "t.crontab",
    )
    utc_job, berlin_job = conf.jobs
    assert utc_job.timezone == datetime.timezone.utc
    assert str(berlin_job.timezone) == "Europe/Berlin"


def test_cron_tz_invalid_reports_assignment_line():
    with pytest.raises(ConfigError) as exc:
        config.parse_crontab_string(
            "\nCRON_TZ=Not/AZone\n", "bad.crontab"
        )
    assert "bad.crontab:2" in str(exc.value)
    assert "Not/AZone" in str(exc.value)


def test_nicknames():
    conf = config.parse_crontab_string(
        "@daily daily-cmd\n@midnight midnight-cmd\n@reboot reboot-cmd\n",
        "t.crontab",
    )
    daily, midnight, reboot = conf.jobs
    assert daily.schedule_unparsed == "@daily"
    assert isinstance(daily.schedule, CronTab)
    # @midnight is a man-page synonym for @daily; it is rewritten because
    # the crontab expression library knows every nickname except it.
    assert midnight.schedule_unparsed == "@daily"
    # @reboot stays the scheduler-level special string, as in YAML configs.
    assert reboot.schedule == "@reboot"


def test_unknown_nickname_rejected():
    with pytest.raises(ConfigError) as exc:
        config.parse_crontab_string("@fortnightly cmd\n", "t.crontab")
    assert "t.crontab:1" in str(exc.value)
    assert "@fortnightly" in str(exc.value)


def test_escaped_percent_becomes_literal():
    conf = config.parse_crontab_string(
        "0 0 * * * date +\\%Y-\\%m-\\%d\n", "t.crontab"
    )
    assert conf.jobs[0].command == "date +%Y-%m-%d"


def test_bare_percent_rejected_with_advice():
    with pytest.raises(ConfigError) as exc:
        config.parse_crontab_string(
            "0 0 * * * echo done 100%\n", "t.crontab"
        )
    message = str(exc.value)
    assert "t.crontab:1" in message
    assert "\\%" in message


def test_bad_schedule_reports_file_and_line():
    with pytest.raises(ConfigError) as exc:
        config.parse_crontab_string(
            "# fine\n\n61 * * * * cmd\n", "broken.crontab"
        )
    assert "broken.crontab:3" in str(exc.value)


def test_schedule_without_command_rejected():
    with pytest.raises(ConfigError):
        config.parse_crontab_string("* * * * *\n", "t.crontab")
    with pytest.raises(ConfigError):
        config.parse_crontab_string("@daily\n", "t.crontab")


def test_gibberish_line_rejected():
    with pytest.raises(ConfigError) as exc:
        config.parse_crontab_string("this is not cron\n", "t.crontab")
    assert "t.crontab:1" in str(exc.value)


def test_comments_and_blanks_only():
    conf = config.parse_crontab_string(
        "# nothing here\n\n   \n# still nothing\n", "t.crontab"
    )
    assert conf.jobs == []


def test_names_use_basename_and_line_numbers():
    conf = config.parse_crontab_string(
        "# comment\n0 0 * * * one\n\n0 1 * * * two\n",
        "/etc/cron.d/backup.crontab",
    )
    assert [job.name for job in conf.jobs] == [
        "backup.crontab:2",
        "backup.crontab:4",
    ]
    # no path at all (string parsing) falls back to a generic label.
    conf = config.parse_crontab_string("0 0 * * * x\n", "")
    assert conf.jobs[0].name == "crontab:1"


def test_system_crontab_user_column_is_not_stripped():
    # /etc/crontab and /etc/cron.d files carry a sixth "user" column.
    # There is no reliable way to tell that apart from a command's first
    # word, so cronstable reads the user-crontab format only and the column
    # lands in the command -- a documented pitfall, asserted here so a
    # future "clever" heuristic has to face this test.
    conf = config.parse_crontab_string(
        "0 0 * * * root /usr/sbin/logrotate\n", "t.crontab"
    )
    assert conf.jobs[0].command == "root /usr/sbin/logrotate"


@pytest.mark.parametrize(
    "path,expected",
    [
        ("legacy.crontab", True),
        ("jobs.cron", True),
        ("crontab", True),
        ("CRONTAB", True),
        ("some/dir/crontab", True),
        ("Backup.CronTab", True),
        ("config.yaml", False),
        ("config.yml", False),
        ("crontab.yaml", False),
        ("notes.txt", False),
        ("mycrontab", False),
        (".crontab", False),
    ],
)
def test_is_crontab_path(path, expected):
    assert crontabs.is_crontab_path(path) is expected


@pytest.mark.parametrize(
    "data,expected",
    [
        ("# comment\n\n*/5 * * * * cmd\n", True),
        ("MAILTO=x@y\n", True),
        ("PATH = /bin\n", True),
        ("@reboot cmd\n", True),
        ("@bogus cmd\n", True),  # '@' can never open a YAML config
        ("jobs:\n  - name: x\n", False),
        ("defaults:\n  shell: /bin/sh\n", False),
        ("---\njobs: []\n", False),
        ("", False),
        ("# only comments\n", False),
        ("* * * * *\n", False),  # five fields but no command: inconclusive
        ("61 * * * * cmd\n", False),  # invalid fields: stay conservative
    ],
)
def test_looks_like_crontab(data, expected):
    assert crontabs.looks_like_crontab(data) is expected


def test_parse_config_single_crontab_file(tmp_path):
    path = tmp_path / "legacy.crontab"
    path.write_text("0 0 * * * echo hi\n", encoding="utf-8")
    conf = config.parse_config(str(path))
    assert [job.name for job in conf.jobs] == ["legacy.crontab:1"]


def test_parse_config_sniffs_extensionless_crontab(tmp_path):
    # e.g. -c /var/spool/cron/crontabs/root: no marker either way, so the
    # content decides.
    path = tmp_path / "root"
    path.write_text("MAILTO=me\n*/5 * * * * job\n", encoding="utf-8")
    conf = config.parse_config(str(path))
    assert [job.name for job in conf.jobs] == ["root:2"]


def test_parse_config_extensionless_yaml_still_yaml(tmp_path):
    # regression guard: sniffing must not steal extensionless YAML files.
    path = tmp_path / "myconfig"
    path.write_text(
        'jobs:\n  - name: x\n    command: echo\n    schedule: "@reboot"\n',
        encoding="utf-8",
    )
    conf = config.parse_config(str(path))
    assert [job.name for job in conf.jobs] == ["x"]


def test_yaml_extension_is_never_sniffed(tmp_path):
    # crontab content under a YAML name is a YAML error, deterministically.
    path = tmp_path / "oops.yaml"
    path.write_text("*/5 * * * * cmd\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        config.parse_config(str(path))


def test_parse_config_dir_mixes_formats(tmp_path):
    (tmp_path / "10-jobs.yaml").write_text(
        "jobs:\n  - name: from-yaml\n    command: echo\n"
        '    schedule: "@reboot"\n',
        encoding="utf-8",
    )
    (tmp_path / "20-legacy.crontab").write_text(
        "0 0 * * * legacy-one\n", encoding="utf-8"
    )
    (tmp_path / "crontab").write_text(
        "@daily legacy-two\n", encoding="utf-8"
    )
    (tmp_path / "_disabled.crontab").write_text(
        "0 0 * * * skipped\n", encoding="utf-8"
    )
    (tmp_path / "README.md").write_text("not config\n", encoding="utf-8")
    (tmp_path / "data.txt").write_text("1 2 3\n", encoding="utf-8")
    conf = config.parse_config(str(tmp_path))
    # deterministic name-sorted file order, crontab files included, and
    # everything else left alone.
    assert [job.name for job in conf.jobs] == [
        "from-yaml",
        "20-legacy.crontab:1",
        "crontab:1",
    ]


def test_crontab_error_in_dir_reports_offending_file(tmp_path):
    (tmp_path / "bad.crontab").write_text(
        "61 * * * * cmd\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError) as exc:
        config.parse_config(str(tmp_path))
    assert "bad.crontab:1" in str(exc.value)


def test_yaml_can_include_a_crontab(tmp_path):
    (tmp_path / "main.yaml").write_text(
        "include:\n  - legacy.crontab\n"
        "jobs:\n  - name: native\n    command: echo\n"
        '    schedule: "@reboot"\n',
        encoding="utf-8",
    )
    (tmp_path / "legacy.crontab").write_text(
        "0 0 * * * included\n", encoding="utf-8"
    )
    conf = config.parse_config(str(tmp_path / "main.yaml"))
    assert sorted(job.name for job in conf.jobs) == [
        "legacy.crontab:1",
        "native",
    ]


def test_h_schedules_parse_with_the_line_derived_name(tmp_path):
    # the classic loader validates each line with its would-be job name as
    # the hash key, so H entries load; and the sniffer accepts H lines too
    jobs = crontabs.parse_crontab(
        "H * * * * /usr/local/bin/spread-me\n", "legacy.crontab"
    )
    assert jobs[0]["name"] == "legacy.crontab:1"
    assert jobs[0]["schedule"] == "H * * * *"
    assert crontabs.looks_like_crontab("H 4 * * * /bin/backup") is True
