import re

import pytest

from cronstable.config import parse_config_string
from cronstable.fingerprint import (
    SCHEME_VERSION,
    _redact_action,
    _redact_report,
    _SECRET_PLACEHOLDER,
    canonical_job,
    job_digest,
    job_set_id,
)
from cronstable.platform import IS_WINDOWS


def _jobs(yaml: str):
    return parse_config_string(yaml, "").jobs


def _id(yaml: str) -> str:
    return job_set_id(_jobs(yaml))


ALPHA = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "*/5 * * * *"
  - name: beta
    command: echo beta
    schedule: "0 0 * * *"
"""

# same two jobs, declared in the opposite order
ALPHA_REVERSED = """
jobs:
  - name: beta
    command: echo beta
    schedule: "0 0 * * *"
  - name: alpha
    command: echo alpha
    schedule: "*/5 * * * *"
"""


def test_id_format():
    job_set = _id(ALPHA)
    assert job_set.startswith(SCHEME_VERSION + ":")
    body = job_set.split(":", 1)[1]
    assert re.fullmatch(r"[0-9a-f]{64}", body)


def test_order_independent():
    assert _id(ALPHA) == _id(ALPHA_REVERSED)


def test_empty_job_set_is_stable():
    assert job_set_id([]) == job_set_id([])
    assert job_set_id([]).startswith(SCHEME_VERSION + ":")
    # an empty set must not collide with a non-empty one
    assert job_set_id([]) != _id(ALPHA)


def test_inline_vs_defaults_block_match():
    inline = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    shell: /bin/bash
    captureStdout: true
"""
    via_defaults = """
defaults:
  shell: /bin/bash
  captureStdout: true
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
"""
    # fingerprint is over the *effective* config, so where a setting is
    # written must not change the id
    assert _id(inline) == _id(via_defaults)


def test_inline_default_numeric_matches_inherited_default():
    # killTimeout's default (int 30) is inherited when omitted; written inline
    # strictyaml coerces it to float 30.0. The two must still fingerprint the
    # same, or HA replicas (one omitting the field, one spelling out the
    # default) would wrongly disagree.
    bare = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
"""
    inline = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    killTimeout: 30
"""
    assert _id(bare) == _id(inline)


def test_inline_default_retry_block_matches_inherited():
    # the same int-vs-float hazard for the retry delays (defaults 1/300/2 are
    # ints; written inline they parse to floats)
    bare = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
"""
    full_retry = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    onFailure:
      retry:
        maximumRetries: 0
        initialDelay: 1
        maximumDelay: 300
        backoffMultiplier: 2
"""
    assert _id(bare) == _id(full_retry)


def test_fractional_float_is_preserved():
    # normalization must only collapse whole-number floats, not lose precision
    half = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    killTimeout: 0.5
"""
    one = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    killTimeout: 1
"""
    assert _id(half) != _id(one)


def test_utc_flag_redundant_when_timezone_set():
    # with an explicit timezone, the raw utc flag has no effect on firing, so
    # two configs differing only in utc must fingerprint the same
    a = """
jobs:
  - name: a
    command: echo a
    schedule: "0 0 * * *"
    timezone: America/New_York
    utc: false
"""
    b = a.replace("utc: false", "utc: true")
    assert _id(a) == _id(b)


def test_utc_flag_matters_without_timezone():
    # with no explicit timezone, utc:false (local) vs utc:true (UTC) is a real
    # firing-frame difference and must change the id
    local = """
jobs:
  - name: a
    command: echo a
    schedule: "0 0 * * *"
    utc: false
"""
    utc = local.replace("utc: false", "utc: true")
    assert _id(local) != _id(utc)


def test_schedule_string_whitespace_normalized():
    single = """
jobs:
  - name: a
    command: echo a
    schedule: "*/5 * * * *"
"""
    doubled = """
jobs:
  - name: a
    command: echo a
    schedule: "*/5  *   * * *"
"""
    assert _id(single) == _id(doubled)


def test_schedule_string_and_object_forms_match():
    as_string = """
jobs:
  - name: a
    command: echo a
    schedule: "*/5 * * * *"
"""
    as_object = """
jobs:
  - name: a
    command: echo a
    schedule:
      minute: "*/5"
"""
    assert _id(as_string) == _id(as_object)


def test_schedule_seconds_string_and_object_forms_match():
    # the object second: form fingerprints identically to the equivalent
    # 7-field crontab string (same equivalence property, extended to seconds)
    as_string = """
jobs:
  - name: a
    command: echo a
    schedule: "*/15 * * * * * *"
"""
    as_object = """
jobs:
  - name: a
    command: echo a
    schedule:
      second: "*/15"
      minute: "*"
"""
    assert _id(as_string) == _id(as_object)


def test_schedule_seconds_change_id():
    # a schedule that pins seconds is a different job set from the minute-only
    # one (the second column is part of the identity)
    minute_only = """
jobs:
  - name: a
    command: echo a
    schedule: "*/15 * * * *"
"""
    with_seconds = """
jobs:
  - name: a
    command: echo a
    schedule: "*/15 * * * * * *"
"""
    assert _id(minute_only) != _id(with_seconds)


def test_schedule_year_string_and_object_forms_match():
    # the object year: form (now honored, 6-field) matches the crontab string
    as_string = """
jobs:
  - name: a
    command: echo a
    schedule: "0 12 * * * 2030"
"""
    as_object = """
jobs:
  - name: a
    command: echo a
    schedule:
      minute: "0"
      hour: "12"
      year: "2030"
"""
    assert _id(as_string) == _id(as_object)


def test_environment_order_independent():
    one = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    environment:
      - key: FOO
        value: "1"
      - key: BAR
        value: "2"
"""
    two = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    environment:
      - key: BAR
        value: "2"
      - key: FOO
        value: "1"
"""
    assert _id(one) == _id(two)


def test_environment_value_not_part_of_identity():
    # only env var NAMES are fingerprinted, not values (values may be secret /
    # per-host); two configs differing only in a value must match
    a = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    environment:
      - key: TOKEN
        value: secret-A
"""
    b = a.replace("secret-A", "secret-B")
    assert _id(a) == _id(b)


def test_environment_key_set_is_part_of_identity():
    # adding/renaming a variable does change the id
    one = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    environment:
      - key: FOO
        value: "1"
"""
    two = one.replace("FOO", "BAR")
    assert _id(one) != _id(two)


def test_command_change_changes_id():
    other = ALPHA.replace("echo alpha", "echo ALPHA")
    assert _id(ALPHA) != _id(other)


def test_schedule_change_changes_id():
    other = ALPHA.replace("*/5 * * * *", "*/10 * * * *")
    assert _id(ALPHA) != _id(other)


def test_enabled_toggle_changes_id():
    disabled = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    enabled: false
"""
    enabled = disabled.replace("enabled: false", "enabled: true")
    assert _id(disabled) != _id(enabled)


def test_cluster_policy_changes_id():
    # clusterPolicy is behaviour-affecting and host-independent, so two
    # configs differing only in it must fingerprint differently (replicas
    # disagreeing on it should surface as drift, not coordinate differently).
    leader = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    clusterPolicy: Leader
"""
    every = leader.replace("Leader", "EveryNode")
    assert _id(leader) != _id(every)
    # the default is Leader, so omitting it matches an explicit Leader
    omitted = leader.replace('    clusterPolicy: Leader\n', "")
    assert _id(omitted) == _id(leader)


def test_shell_command_vs_argv_do_not_collide():
    as_shell = """
jobs:
  - name: a
    command: echo a b
    schedule: "* * * * *"
"""
    as_argv = """
jobs:
  - name: a
    command:
      - echo
      - a
      - b
    schedule: "* * * * *"
"""
    assert _id(as_shell) != _id(as_argv)


def test_inline_secret_value_is_redacted():
    # two jobs differing only in the literal sentry DSN value must produce the
    # same id: the fingerprint must not embed secret material.
    tmpl = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    onFailure:
      report:
        sentry:
          dsn:
            value: {secret}
"""
    a = tmpl.format(secret="https://aaa@example.com/1")
    b = tmpl.format(secret="https://bbb@example.com/2")
    assert _id(a) == _id(b)


def test_having_a_secret_still_differs_from_not_having_one():
    with_dsn = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    onFailure:
      report:
        sentry:
          dsn:
            value: https://aaa@example.com/1
"""
    without = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
"""
    # redaction hides the *value*, not the fact that sentry is configured
    assert _id(with_dsn) != _id(without)


def test_canonical_job_uses_configured_user_not_resolved_uid():
    # construct a job without user (so no root/passwd resolution is needed),
    # then check the fingerprint reflects the configured user/group attributes
    # rather than any resolved uid/gid.
    (job,) = _jobs(
        """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
"""
    )
    canon = canonical_job(job)
    assert canon["user"] is None and canon["group"] is None
    assert "uid" not in canon and "gid" not in canon

    before = job_digest(job)
    job.user = "www-data"
    job.group = "staff"
    after = canonical_job(job)
    assert after["user"] == "www-data"
    assert after["group"] == "staff"
    assert job_digest(job) != before


def test_canonical_job_is_json_safe_and_pure():
    # canonical_job must not mutate the job's own (shared) config dicts when it
    # redacts secrets, and must be stable across repeated calls.
    (job,) = _jobs(
        """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    onFailure:
      report:
        sentry:
          dsn:
            value: https://aaa@example.com/1
"""
    )
    first = job_digest(job)
    # the real config still holds the original secret (not redacted in place)
    assert job.onFailure["report"]["sentry"]["dsn"]["value"] == (
        "https://aaa@example.com/1"
    )
    assert job_digest(job) == first


# ---------------------------------------------------------------------------
# Stability + completeness of the fingerprint.
#
# The job-set id is a cross-instance coordination contract: operators compare
# stored ids across versions, so the scheme must be STABLE (a canonicalization
# refactor must not silently change every id) and COMPLETE (every
# behavior-affecting field must be in identity). The existing tests are all
# relational (_id(A) vs _id(B)); these add a golden value, a field-set
# lock, and redaction coverage for the mail password and the onSuccess /
# onPermanentFailure action blocks.
# ---------------------------------------------------------------------------

# The golden values are the POSIX reference. The fingerprint is platform-scoped
# by design: besides the job's own `shell` (pinned below), the default report
# block's shell also defaults to platform.DEFAULT_SHELL (/bin/sh on POSIX, ""
# on Windows), so the digest legitimately differs across platforms. HA replicas
# are compared per-platform, so the POSIX value is the right tripwire and the
# test is POSIX-only. If this literal changes on POSIX, the canonicalization
# scheme changed, which REQUIRES bumping SCHEME_VERSION, not just editing the
# constant. Treat an unexpected change here as a bug, not a test to "fix".
GOLDEN_CONFIG = """
jobs:
  - name: alpha
    command: echo alpha
    schedule: "*/5 * * * *"
    shell: /bin/sh
  - name: beta
    command:
      - echo
      - beta
    schedule: "0 0 * * *"
    shell: /bin/sh
    captureStdout: true
"""

# Regenerated when onlyIfLastSucceeded entered the identity (it gates every
# scheduled fire, like `enabled`, so replicas disagreeing on it must show as
# drift). The v1 scheme had not shipped in any release at that point, so no
# operator-stored ids existed to invalidate and SCHEME_VERSION stays v1; a
# post-release change of this kind WOULD require the bump the comment above
# demands.
GOLDEN_JOB_SET_ID = (
    "v1:9e1086b120f4685e4ddd53c579b59c476b1e418ec4cdce6199c56953d13c2958"
)
GOLDEN_ALPHA_DIGEST = (
    "988333f6ca07051fb8546d20c34da1e1539f3bfb284ea3919a8e5ebf30836d21"
)


@pytest.mark.skipif(
    IS_WINDOWS, reason="golden digest is the POSIX reference (platform-scoped)"
)
def test_job_set_id_golden_value():
    jobs = _jobs(GOLDEN_CONFIG)
    assert job_set_id(jobs) == GOLDEN_JOB_SET_ID
    # the per-job digest is pinned too, so a drift can be localized to the
    # per-job canonicalization vs the set-combining step.
    assert job_digest(jobs[0]) == GOLDEN_ALPHA_DIGEST


# The golden above is the strongest tripwire for an accidental identity change,
# but it is POSIX-only (the report block's default shell is platform-scoped), so
# a Windows-only test run reports green while CI's Linux row fails. These lock
# the omit-when-default rule for the *nested* report keys on every platform:
# they assert on the presence/absence of the key and on relative digests, never
# on a platform-scoped literal. A new entry in config._REPORT_DEFAULTS merges
# into every job's report block and reaches identity without fingerprint.py
# changing at all -- which is exactly how the reporter `timeout` first shipped
# into every job's digest, repointing the persisted retry ladders and @reboot
# markers keyed by job_digest.
_REPORT_TIMEOUT_JOB = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    onFailure:
      report:
        shell:
          command: "true"
{timeout}
"""


def _report_timeout_job(timeout=None):
    line = "" if timeout is None else "          timeout: {}".format(timeout)
    (job,) = _jobs(_REPORT_TIMEOUT_JOB.format(timeout=line))
    return job


def test_default_report_timeout_stays_out_of_identity():
    # inherited-from-defaults: the key must not appear anywhere in identity, or
    # every pre-existing job's digest changes on upgrade.
    canon = canonical_job(_report_timeout_job())
    for action in ("onFailure", "onPermanentFailure", "onSuccess"):
        shell = canon[action]["report"]["shell"]
        assert "timeout" not in shell, action


def test_explicit_default_report_timeout_matches_inherited():
    # the inline-vs-defaults guarantee, at the default value: writing the
    # default out longhand must not fork the identity. `Float()` parses it to
    # 60.0 where the inherited default is the int 60, so this also pins that
    # the two spellings agree (cf. _normalize_numbers).
    assert job_digest(_report_timeout_job(60)) == job_digest(
        _report_timeout_job()
    )


def test_non_default_report_timeout_enters_identity():
    # the other half of omit-when-default: a job that actually sets the field
    # gets a new identity, so replicas disagreeing on it show as drift.
    default = _report_timeout_job()
    tightened = _report_timeout_job(5)
    canon = canonical_job(tightened)
    assert canon["onFailure"]["report"]["shell"]["timeout"] == 5
    assert job_digest(tightened) != job_digest(default)


EXPECTED_CANONICAL_FIELDS = frozenset(
    {
        "name",
        "command",
        "schedule",
        "shell",
        "concurrencyPolicy",
        "clusterPolicy",
        "captureStderr",
        "captureStdout",
        "streamPrefix",
        "saveLimit",
        "maxLineLength",
        "timezone",
        "enabled",
        # gates every scheduled fire (like `enabled`): replicas disagreeing
        # on it must show as drift, so it is part of the identity.
        "onlyIfLastSucceeded",
        "failsWhen",
        "onFailure",
        "onPermanentFailure",
        "onSuccess",
        "environment",
        "executionTimeout",
        "killTimeout",
        "statsd",
        "user",
        "group",
    }
)


def test_canonical_job_field_set_is_locked():
    # adding or removing a field from the identity changes every id, so it must
    # be a deliberate decision. This fails loudly and names the drift, instead
    # of a field silently entering/leaving identity in an unrelated edit.
    (job,) = _jobs(
        """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
"""
    )
    assert set(canonical_job(job).keys()) == EXPECTED_CANONICAL_FIELDS


_IDENTITY_BASE = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    concurrencyPolicy: Allow
    captureStderr: false
    captureStdout: false
    streamPrefix: "p"
    saveLimit: 4096
    maxLineLength: 4096
    executionTimeout: 10
    killTimeout: 30
"""


@pytest.mark.parametrize(
    "old, new",
    [
        ("concurrencyPolicy: Allow", "concurrencyPolicy: Forbid"),
        ("captureStderr: false", "captureStderr: true"),
        ("captureStdout: false", "captureStdout: true"),
        ('streamPrefix: "p"', 'streamPrefix: "q"'),
        ("saveLimit: 4096", "saveLimit: 8192"),
        ("maxLineLength: 4096", "maxLineLength: 8192"),
        ("executionTimeout: 10", "executionTimeout: 20"),
        ("killTimeout: 30", "killTimeout: 60"),
        ("name: a", "name: b"),
    ],
)
def test_identity_field_change_changes_id(old, new):
    # each of these fields affects firing/behavior, so a change must change the
    # id (else HA replicas with different behavior would wrongly match).
    variant = _IDENTITY_BASE.replace(old, new)
    assert variant != _IDENTITY_BASE  # the replacement actually fired
    assert _id(_IDENTITY_BASE) != _id(variant)


def test_statsd_presence_changes_id():
    without = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
"""
    with_statsd = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    statsd:
      host: localhost
      port: 8125
      prefix: yacron
"""
    assert _id(without) != _id(with_statsd)


def test_fails_when_change_changes_id():
    base = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    failsWhen:
      producesStdout: false
"""
    variant = base.replace("producesStdout: false", "producesStdout: true")
    assert _id(base) != _id(variant)


def _mail_pw_config(secret):
    return """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    onFailure:
      report:
        mail:
          from: a@b.com
          to: c@d.com
          smtpHost: smtp
          password:
            value: {secret}
""".format(secret=secret)


def test_inline_mail_password_is_redacted():
    # two configs differing only in the mail password value must produce the
    # same: the id is logged and HTTP-served, so it must embed no secret.
    assert _id(_mail_pw_config("hunter2")) == _id(_mail_pw_config("swordfish"))


def test_mail_password_presence_still_changes_id():
    with_pw = _mail_pw_config("hunter2")
    without_pw = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    onFailure:
      report:
        mail:
          from: a@b.com
          to: c@d.com
          smtpHost: smtp
"""
    # redaction hides the value, not the fact that a password is configured
    assert _id(with_pw) != _id(without_pw)


def _sentry_action_config(action, secret):
    return """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    {action}:
      report:
        sentry:
          dsn:
            value: {secret}
""".format(action=action, secret=secret)


@pytest.mark.parametrize("action", ["onSuccess", "onPermanentFailure"])
def test_sentry_secret_redacted_in_action_block(action):
    # redaction must be wired through ALL action blocks, not just onFailure
    # (which the existing test covers).
    a = _sentry_action_config(action, "https://aaa@example.com/1")
    b = _sentry_action_config(action, "https://bbb@example.com/2")
    assert _id(a) == _id(b)


def _webhook_config(url, auth="s3cret"):
    return """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
    onFailure:
      report:
        webhook:
          url:
            value: {url}
          headers:
            Authorization: {auth}
""".format(url=url, auth=auth)


def test_inline_webhook_url_is_redacted():
    # a Slack/Discord-style webhook URL embeds its token, so two configs
    # differing only in the inline URL value must produce the same id.
    a = _webhook_config("https://hooks.slack.com/services/AAA")
    b = _webhook_config("https://hooks.slack.com/services/BBB")
    assert _id(a) == _id(b)


def test_webhook_header_values_are_redacted():
    # header values commonly carry credentials (e.g. Authorization); only the
    # header *names* are part of the identity.
    a = _webhook_config("https://example.com/hook", auth="tokenA")
    b = _webhook_config("https://example.com/hook", auth="tokenB")
    assert _id(a) == _id(b)


def test_webhook_presence_still_changes_id():
    with_hook = _webhook_config("https://example.com/hook")
    without = """
jobs:
  - name: a
    command: echo a
    schedule: "* * * * *"
"""
    # redaction hides the value, not the fact that a webhook is configured
    assert _id(with_hook) != _id(without)


# ---------------------------------------------------------------------------
# Unit tests for the report/action redactors directly, including the arcs
# where the secret-bearing blocks are absent.
# ---------------------------------------------------------------------------


def test_redact_report_passes_through_when_no_secret_bearing_blocks():
    # no sentry / mail / webhook: every isinstance gate is skipped and the
    # non-secret content survives unchanged (a fresh copy, input untouched).
    report = {"shell": {"command": "true"}}
    out = _redact_report(report)
    assert out == {"shell": {"command": "true"}}
    assert out is not report


def test_redact_report_redacts_every_secret_leaf():
    report = {
        "sentry": {"dsn": {"value": "https://k@example.com/1"}},
        "mail": {"password": {"value": "hunter2"}},
        "webhook": {
            "url": {"value": "https://hooks.example.com/AAA"},
            "headers": {"Authorization": "token-abc"},
        },
    }
    out = _redact_report(report)
    assert out["sentry"]["dsn"]["value"] == _SECRET_PLACEHOLDER
    assert out["mail"]["password"]["value"] == _SECRET_PLACEHOLDER
    assert out["webhook"]["url"]["value"] == _SECRET_PLACEHOLDER
    assert out["webhook"]["headers"]["Authorization"] == _SECRET_PLACEHOLDER
    # copy-on-write: the caller's dict still holds the real secrets
    assert report["mail"]["password"]["value"] == "hunter2"
    assert report["webhook"]["headers"]["Authorization"] == "token-abc"


def test_redact_report_webhook_url_only_without_headers():
    # webhook present with a url but no headers dict: exercises the
    # headers-absent arc while still redacting the url value.
    out = _redact_report({"webhook": {"url": {"value": "https://x/AAA"}}})
    assert out["webhook"]["url"]["value"] == _SECRET_PLACEHOLDER
    assert "headers" not in out["webhook"]


def test_redact_report_webhook_with_no_url_and_no_headers_untouched():
    # webhook is a dict but carries neither a url value nor headers, so no new
    # webhook copy is made and the block passes through unchanged.
    out = _redact_report({"webhook": {"body": "static template"}})
    assert out["webhook"] == {"body": "static template"}


def test_redact_action_without_report_preserves_other_keys():
    # no report key: the redactor copies the action and returns it as-is.
    out = _redact_action({"retry": {"maximumRetries": 3}})
    assert out == {"retry": {"maximumRetries": 3}}


def test_redact_action_with_report_redacts_nested_secret():
    action = {"report": {"sentry": {"dsn": {"value": "https://k@e/1"}}}}
    out = _redact_action(action)
    assert out["report"]["sentry"]["dsn"]["value"] == _SECRET_PLACEHOLDER
    # original untouched
    assert action["report"]["sentry"]["dsn"]["value"] == "https://k@e/1"
