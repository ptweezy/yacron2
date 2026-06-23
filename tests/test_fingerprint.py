import re

from yacron2.config import parse_config_string
from yacron2.fingerprint import (
    SCHEME_VERSION,
    canonical_job,
    job_digest,
    job_set_id,
)


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
    # normalisation must only collapse whole-number floats, not lose precision
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
