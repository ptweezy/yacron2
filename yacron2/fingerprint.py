"""Compute a deterministic, order-independent fingerprint of a job set.

The *job-set ID* is a hash over the effective configuration of every job a
yacron2 instance is running.  Two instances produce the **same** ID if and only
if they are running the same set of jobs, regardless of:

* the order the jobs appear in the configuration;
* whether a setting was written inline on each job or hoisted into a
  ``defaults`` block (the fingerprint is taken over the *merged*, effective
  :class:`~yacron2.config.JobConfig`, not the raw YAML text);
* equivalent spellings of the same schedule (the ``minute:``/``hour:`` object
  form normalizes to the same five-field crontab line as the string form).

This is intended for coordinating replicas: several yacron2 instances deployed
from the same configuration can confirm they hold an identical job set (e.g.
for leader election, to avoid double-running jobs) by comparing IDs.

Design notes that matter for that "byte-identical across hosts" guarantee:

* **user/group are fingerprinted as configured, not as resolved.**  A job's
  ``user: www-data`` resolves to a numeric uid from the *local* passwd
  database, and that uid can differ host to host.  We hash the configured
  value so the intent matches across hosts (see ``JobConfig.user``/``group``).
* **secret/value material is never embedded.**  The ID is logged at startup
  and served on a (possibly unauthenticated) HTTP endpoint, so it must never
  embed secret material.  Inline reporting secrets (Sentry DSN, mail password,
  webhook URL and header values) are redacted, and only the *names* of
  ``environment`` variables are hashed,
  never their values (env is a common place to carry secrets, and a per-host
  value, e.g. from ``env_file``, would otherwise make identical configs
  fingerprint differently across hosts).  We hash *whether* and *how* a secret
  is configured, never its literal value.
* **the scheme is versioned** (the ``v1:`` prefix).  IDs are only comparable
  within the same scheme version; bumping :data:`SCHEME_VERSION` lets the
  canonicalization evolve without silently making old and new IDs "disagree".

Because the fingerprint is over *effective* config, it also reflects
platform-dependent defaults (e.g. the default ``shell`` is ``/bin/sh`` on POSIX
and ``cmd.exe`` on Windows).  Compare instances running on the same platform,
which HA replicas are.
"""

import hashlib
import json
from typing import Any, Dict, Iterable, List, Union

from yacron2.config import JobConfig, schedule_object_to_crontab

# Canonicalization scheme version.  Prefixes the emitted ID and is folded into
# the hash input, so a future change to what/how we canonicalize can bump this
# and old/new IDs will compare unequal instead of silently colliding.
SCHEME_VERSION = "v1"

# Placeholder substituted for any inline secret *value* so the fingerprint
# never embeds secret material.  The surrounding structure (whether a secret
# is set, and via value/fromFile/fromEnvVar) is still part of the identity.
_SECRET_PLACEHOLDER = "<redacted>"


def _schedule_repr(job: JobConfig) -> str:
    """Normalize a job's schedule to a canonical string.

    The object form (``minute:``/``hour:``/...) collapses to the same crontab
    line as the equivalent string form, so the two spellings fingerprint
    identically -- a plain 5-field line when neither ``second`` nor ``year`` is
    used (unchanged from before), and the matching 7-/6-field line when they
    are.  Bare strings (a crontab line, or ``@reboot``) are used verbatim.
    """
    unparsed = job.schedule_unparsed
    if isinstance(unparsed, str):
        # Collapse runs of whitespace (and trim) so a trivially reformatted
        # crontab line, or an '@'-macro with stray spacing, fingerprints the
        # same. Cron syntax (and the '@reboot'/'@daily'/... sentinels) is
        # whitespace-delimited single tokens, so this is lossless.
        return " ".join(unparsed.split())
    # Shared object->crontab builder, so the fingerprint, the parsed schedule
    # and the web UI label can never disagree on how the object form maps to a
    # crontab line (including the second/year columns).
    return schedule_object_to_crontab(unparsed)


def _command_repr(command: Union[str, List[str]]) -> Dict[str, Any]:
    """Structural representation of a job's command.

    Keeps the shell-string vs argv-list distinction (they behave differently),
    rather than joining a list into a string, which would be lossy:
    ``["echo", "a b"]`` and ``["echo", "a", "b"]`` must not collide.
    """
    if isinstance(command, list):
        return {"argv": list(command)}
    return {"shell_command": command}


def _redact_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """Copy a report block, replacing inline secret values with a marker.

    Only the literal ``value`` of a sentry DSN / mail password / webhook URL
    (plus webhook header values) is redacted; the ``fromFile`` /
    ``fromEnvVar`` references are paths and env-var names (not secrets) and
    are kept, since they are part of the job's identity.

    Copy-on-write, not ``deepcopy``: the report carries long immutable template
    strings (sentry body, the ~300-char webhook body) that we only ever read,
    so we shallow-copy just the dicts on the path to each redacted leaf and
    share every untouched subtree by reference. The input ``report`` (a live
    JobConfig dict) is never mutated -- see test_canonical_job_is_json_safe_
    and_pure -- and downstream (``_normalize_numbers``, ``json.dumps``) only
    reads, so reference-sharing is safe and the output stays byte-identical.
    """
    out = dict(report)
    sentry = out.get("sentry")
    if isinstance(sentry, dict):
        dsn = sentry.get("dsn")
        if isinstance(dsn, dict) and dsn.get("value") is not None:
            out["sentry"] = {
                **sentry,
                "dsn": {**dsn, "value": _SECRET_PLACEHOLDER},
            }
    mail = out.get("mail")
    if isinstance(mail, dict):
        password = mail.get("password")
        if isinstance(password, dict) and password.get("value") is not None:
            out["mail"] = {
                **mail,
                "password": {**password, "value": _SECRET_PLACEHOLDER},
            }
    webhook = out.get("webhook")
    if isinstance(webhook, dict):
        new_webhook = None
        url = webhook.get("url")
        if isinstance(url, dict) and url.get("value") is not None:
            new_webhook = dict(webhook)
            new_webhook["url"] = {**url, "value": _SECRET_PLACEHOLDER}
        headers = webhook.get("headers")
        if isinstance(headers, dict):
            # header *values* commonly carry credentials (e.g. an
            # Authorization token); keep the names, which are identity.
            if new_webhook is None:
                new_webhook = dict(webhook)
            new_webhook["headers"] = dict.fromkeys(
                headers, _SECRET_PLACEHOLDER
            )
        if new_webhook is not None:
            out["webhook"] = new_webhook
    return out


def _redact_action(action: Dict[str, Any]) -> Dict[str, Any]:
    """Copy an on{Failure,PermanentFailure,Success} block, redacting secrets.

    Preserves everything else (e.g. the ``retry`` policy under ``onFailure``).
    """
    out = dict(action)
    if "report" in out and isinstance(out["report"], dict):
        out["report"] = _redact_report(out["report"])
    return out


def canonical_job(job: JobConfig) -> Dict[str, Any]:
    """Build the canonical, host-independent identity dict for one job.

    Includes every behavior-affecting field of the effective config.  The
    resolved uid/gid (host-specific) are deliberately excluded in favor of the
    configured ``user``/``group``; inline secret values are redacted.
    """
    return {
        "name": job.name,
        "command": _command_repr(job.command),
        "schedule": _schedule_repr(job),
        "shell": job.shell,
        "concurrencyPolicy": job.concurrencyPolicy,
        # where the job runs under leader election: a behaviour-affecting,
        # host-independent field, so replicas disagreeing on it should show as
        # drift rather than silently coordinate differently.
        "clusterPolicy": job.clusterPolicy,
        "captureStderr": job.captureStderr,
        "captureStdout": job.captureStdout,
        "streamPrefix": job.streamPrefix,
        "saveLimit": job.saveLimit,
        "maxLineLength": job.maxLineLength,
        # The resolved scheduling frame fully captures firing behavior, so the
        # raw ``utc`` flag is NOT hashed separately: it would be redundant and
        # would split behaviorally-identical configs. job.timezone is "UTC"
        # when utc=true (or unset), the IANA name when a timezone is set (the
        # raw utc flag is then inert), and None for local time (utc=false, no
        # timezone).
        "timezone": (str(job.timezone) if job.timezone is not None else None),
        "enabled": job.enabled,
        "failsWhen": job.failsWhen,
        "onFailure": _redact_action(job.onFailure),
        "onPermanentFailure": _redact_action(job.onPermanentFailure),
        "onSuccess": _redact_action(job.onSuccess),
        # Only the SET of variable NAMES is identity, never the values: env
        # values are a common place to carry secrets (and the id is logged and
        # served), and a per-host value (e.g. from env_file, read at parse
        # time) would otherwise make byte-identical configs fingerprint
        # differently across hosts, defeating HA coordination. The name set
        # DOES include names contributed by env_file, so replicas must ship an
        # env_file with the same variable names (only the values may differ per
        # host). Sorted so it is independent of declaration order.
        "environment": sorted(e["key"] for e in job.environment),
        "executionTimeout": job.executionTimeout,
        "killTimeout": job.killTimeout,
        "statsd": job.statsd,
        # configured values, NOT the resolved uid/gid (which are host-specific)
        "user": job.user,
        "group": job.group,
    }


def _normalize_numbers(obj: Any) -> Any:
    """Collapse the int/float distinction by value, recursively.

    A whole-number float (``30.0``) canonicalizes to the same int (``30``) it
    would be if inherited from a ``DEFAULT_CONFIG`` int literal.  Without this,
    a ``Float()``-typed field (``killTimeout``, the retry delays, ...) written
    *inline*, where strictyaml coerces it to a float, would hash differently
    from the *same value inherited from defaults* (which stays a Python int),
    breaking the inline-vs-defaults guarantee.  ``bool`` is left untouched (it
    is an ``int`` subclass but must stay ``true``/``false`` in JSON), and a
    fractional float (``0.5``) is preserved.
    """
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, float) and obj.is_integer():
        return int(obj)
    if isinstance(obj, dict):
        return {k: _normalize_numbers(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_normalize_numbers(v) for v in obj]
    return obj


def _canonical_bytes(obj: Any) -> bytes:
    # sort_keys for order-independence; ensure_ascii so the byte output is
    # pure-ASCII and identical regardless of locale/encoding; compact
    # separators so there is exactly one serialization; _normalize_numbers so
    # int and float spellings of the same value cannot diverge.
    return json.dumps(
        _normalize_numbers(obj),
        sort_keys=True,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")


def job_digest(job: JobConfig) -> str:
    """Hex SHA-256 of a single job's canonical identity."""
    return hashlib.sha256(_canonical_bytes(canonical_job(job))).hexdigest()


def job_set_id(jobs: Iterable[JobConfig]) -> str:
    """Compute the order-independent fingerprint of a set of jobs.

    Returns a string of the form ``"v1:<64 hex chars>"``.  Per-job digests are
    sorted (neutralizing order) and hashed together; an empty job set yields a
    stable, well-defined ID.
    """
    digests = sorted(job_digest(job) for job in jobs)
    combined = SCHEME_VERSION + "\n" + "\n".join(digests)
    final = hashlib.sha256(combined.encode("ascii")).hexdigest()
    return "{}:{}".format(SCHEME_VERSION, final)
