import copy
import datetime
import ipaddress
import logging
import math
import os
import re
import socket
import sys
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    NewType,
    Optional,
    Tuple,
    Union,  # noqa
)
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import strictyaml
from crontab import CronTab
from strictyaml import Any as YamlAny
from strictyaml import (
    Bool,
    EmptyDict,
    EmptyNone,
    Enum,
    Float,
    Int,
    Map,
    MapPattern,
    Seq,
    Str,
)
from strictyaml import Optional as Opt
from strictyaml.ruamel.error import YAMLError

from yacron2 import crontabs, platform

logger = logging.getLogger("yacron2.config")
WebConfig = NewType("WebConfig", Dict[str, Any])
ClusterConfig = NewType("ClusterConfig", Dict[str, Any])
JobDefaults = NewType("JobDefaults", Dict[str, Any])
LoggingConfig = NewType("LoggingConfig", Dict[str, Any])

# Defaults for an (optional) cluster block. Only applied when a `cluster`
# section is present; see _build_cluster_config.
DEFAULT_CLUSTER = {
    # which leadership backend gates jobs:
    #   "gossip" (default) - the embedded mTLS, no-shared-state, best-effort
    #                        quorum election (listen/tls/peers below);
    #   "kubernetes"       - a coordination.k8s.io/v1 Lease (fenced);
    #   "etcd"             - a lease-backed etcd key/election (fenced).
    # The two lease backends talk to their store over plain HTTP (the core
    # aiohttp dependency), so neither adds a runtime dependency.
    "backend": "gossip",
    "interval": 30,  # seconds between peer-attestation rounds
    "driftAfter": 3,  # reachable-but-mismatched rounds before "drifted"
    "nodeName": None,  # defaults to the system hostname at load time
    "connectTimeout": 10,  # seconds per peer request
    # When true, only the elected leader runs *scheduled* jobs (manual API
    # triggers and retries are unaffected); see yacron2.cluster.elect_leader.
    # Off by default so a cluster section is observe-only until opted in.
    "electLeader": False,
    # How leader-gated jobs are distributed across the quorate cluster:
    #   "single-leader" (default) - one elected leader runs every Leader job;
    #   "spread"                  - per-job ownership via rendezvous hashing,
    #                               so the work fans out across the quorate
    #                               nodes (same quorum gate, same guarantee).
    # Inert unless electLeader is on; see yacron2.cluster.elect_job_owner.
    "distribution": "single-leader",
}

# Defaults merged over a `cluster.kubernetes` block (backend: kubernetes). The
# values mirror client-go's leaderelection defaults; see
# yacron2.backends.kubernetes.
DEFAULT_K8S: Dict[str, Any] = {
    "leaseName": "yacron2-leader",
    # None -> the in-cluster service-account namespace file at runtime.
    "leaseNamespace": None,
    "leaseDurationSeconds": 15,
    "renewDeadlineSeconds": 10,
    "retryPeriodSeconds": 2,
    "identity": None,  # None -> nodeName
    "kubeconfig": None,  # for out-of-cluster / local (Docker) testing
    # override the apiserver URL (e.g. point at a kube-rbac-proxy sidecar);
    # must be https. Wins on BOTH credential paths and both transports:
    # in-cluster it keeps the ServiceAccount token/CA, and with kubeconfig set
    # it overrides the kubeconfig's cluster.server while keeping its
    # credentials (see backends.kubernetes._setup_sync/_load_kubeconfig).
    "apiServer": None,
    # auto (native `kubernetes` client if importable, else hand-rolled HTTP) |
    # library (require the native client) | http (force hand-rolled).
    "clientLibrary": "auto",
}

# Defaults merged over a `cluster.etcd` block (backend: etcd). See
# yacron2.backends.etcd.
DEFAULT_ETCD: Dict[str, Any] = {
    "endpoints": ["http://127.0.0.1:2379"],
    "electionName": "yacron2/leader",
    "ttl": 15,  # lease time-to-live, seconds
    "username": None,
    # resolved like web.authToken: value / fromFile / fromEnvVar
    "password": {"value": None, "fromFile": None, "fromEnvVar": None},
    # optional client TLS to the etcd endpoints
    "tls": {"ca": None, "cert": None, "key": None},
}


class ConfigError(Exception):
    pass


DEFAULT_BODY_TEMPLATE = """
{% if fail_reason -%}
(job failed because {{fail_reason}})
{% endif %}
{% if stdout and stderr -%}
STDOUT:
---
{{stdout}}
---
STDERR:
{{stderr}}
{% elif stdout -%}
{{stdout}}
{% elif stderr -%}
{{stderr}}
{% else -%}
(no output was captured)
{% endif %}
"""

DEFAULT_SUBJECT_TEMPLATE = (
    "Cron job '{{name}}' {% if success %}completed{% else %}failed{% endif %}"
)

# Same text as the default sentry body (subject + body), JSON-encoded into a
# {"text": ...} payload -- the shape Slack, Mattermost and Teams incoming
# webhooks accept out of the box. Override `body` for other services (e.g.
# Discord wants {"content": ...}, ntfy takes a plain-text body).
DEFAULT_WEBHOOK_BODY_TEMPLATE = (
    '{"text": {% filter tojson %}'
    + DEFAULT_SUBJECT_TEMPLATE
    + "\n"
    + DEFAULT_BODY_TEMPLATE
    + "{% endfilter %}}"
)

_REPORT_DEFAULTS = {
    "sentry": {
        "dsn": {"value": None, "fromFile": None, "fromEnvVar": None},
        "body": DEFAULT_SUBJECT_TEMPLATE + "\n" + DEFAULT_BODY_TEMPLATE,
        "fingerprint": ["yacron2", "{{ environment.HOSTNAME }}", "{{ name }}"],
        "environment": None,
        "maxStringLength": 8192,
    },
    "mail": {
        "from": None,
        "to": None,
        "smtpHost": None,
        "smtpPort": 25,
        "tls": False,
        "starttls": False,
        "validate_certs": True,
        "html": False,
        "subject": DEFAULT_SUBJECT_TEMPLATE,
        "body": DEFAULT_BODY_TEMPLATE,
        "username": None,
        "password": {"value": None, "fromFile": None, "fromEnvVar": None},
    },
    "shell": {
        "shell": platform.DEFAULT_SHELL,
        "command": None,
    },
    "webhook": {
        # resolved like sentry "dsn": value / fromFile / fromEnvVar. Treated
        # as a secret (a Slack/Discord webhook URL embeds its token).
        "url": {"value": None, "fromFile": None, "fromEnvVar": None},
        "method": "POST",
        "contentType": "application/json",
        "headers": {},
        "body": DEFAULT_WEBHOOK_BODY_TEMPLATE,
        "timeout": 10,
    },
}


DEFAULT_CONFIG = {
    "shell": platform.DEFAULT_SHELL,
    "concurrencyPolicy": "Allow",
    # where this job runs under cluster leader election (inert unless
    # cluster.electLeader is set); see yacron2.cron._cluster_allows.
    "clusterPolicy": "Leader",
    "captureStderr": True,
    "captureStdout": False,
    "saveLimit": 4096,
    "maxLineLength": 16 * 1024 * 1024,
    "utc": True,
    "timezone": None,
    "failsWhen": {
        "producesStdout": False,
        "producesStderr": True,
        "nonzeroReturn": True,
        "always": False,
    },
    "onFailure": {
        "retry": {
            "maximumRetries": 0,
            "initialDelay": 1,
            "maximumDelay": 300,
            "backoffMultiplier": 2,
        },
        # deepcopy so the three report blocks below do not alias the same
        # mutable object (and its nested lists, e.g. sentry "fingerprint").
        "report": copy.deepcopy(_REPORT_DEFAULTS),
    },
    "onPermanentFailure": {"report": copy.deepcopy(_REPORT_DEFAULTS)},
    "onSuccess": {"report": copy.deepcopy(_REPORT_DEFAULTS)},
    "environment": [],
    "env_file": None,
    "executionTimeout": None,
    "killTimeout": 30,
    "statsd": None,
    "streamPrefix": "[{job_name} {stream_name}] ",
    "enabled": True,
}


_report_schema = Map(
    {
        Opt("sentry"): Map(
            {
                Opt("dsn"): Map(
                    {
                        Opt("value"): EmptyNone() | Str(),
                        Opt("fromFile"): EmptyNone() | Str(),
                        Opt("fromEnvVar"): EmptyNone() | Str(),
                    }
                ),
                Opt("fingerprint"): Seq(Str()),
                Opt("level"): Str(),
                Opt("extra"): MapPattern(Str(), Str() | Int() | Bool()),
                Opt("body"): Str(),
                Opt("environment"): Str(),
                Opt("maxStringLength"): Int(),
            }
        ),
        Opt("mail"): Map(
            {
                "from": EmptyNone() | Str(),
                "to": EmptyNone() | Str(),
                Opt("smtpHost"): Str(),
                Opt("smtpPort"): Int(),
                Opt("subject"): Str(),
                Opt("body"): Str(),
                Opt("username"): Str(),
                Opt("password"): Map(
                    {
                        Opt("value"): EmptyNone() | Str(),
                        Opt("fromFile"): EmptyNone() | Str(),
                        Opt("fromEnvVar"): EmptyNone() | Str(),
                    }
                ),
                Opt("tls"): Bool(),
                Opt("starttls"): Bool(),
                Opt("validate_certs"): Bool(),
                Opt("html"): Bool(),
            }
        ),
        Opt("shell"): Map(
            {
                Opt("shell"): Str(),
                "command": Str() | Seq(Str()),
            }
        ),
        Opt("webhook"): Map(
            {
                Opt("url"): Map(
                    {
                        Opt("value"): EmptyNone() | Str(),
                        Opt("fromFile"): EmptyNone() | Str(),
                        Opt("fromEnvVar"): EmptyNone() | Str(),
                    }
                ),
                Opt("method"): Str(),
                Opt("contentType"): Str(),
                Opt("headers"): MapPattern(Str(), Str()),
                Opt("body"): Str(),
                Opt("timeout"): Float(),
            }
        ),
    }
)

_job_defaults_common = {
    Opt("shell"): Str(),
    Opt("concurrencyPolicy"): Enum(["Allow", "Forbid", "Replace"]),
    Opt("clusterPolicy"): Enum(["Leader", "PreferLeader", "EveryNode"]),
    Opt("captureStderr"): Bool(),
    Opt("captureStdout"): Bool(),
    Opt("saveLimit"): Int(),
    Opt("maxLineLength"): Int(),
    Opt("utc"): Bool(),
    Opt("timezone"): Str(),
    Opt("failsWhen"): Map(
        {
            "producesStdout": Bool(),
            Opt("producesStderr"): Bool(),
            Opt("nonzeroReturn"): Bool(),
            Opt("always"): Bool(),
        }
    ),
    Opt("onFailure"): Map(
        {
            Opt("retry"): Map(
                {
                    "maximumRetries": Int(),
                    "initialDelay": Float(),
                    "maximumDelay": Float(),
                    "backoffMultiplier": Float(),
                }
            ),
            Opt("report"): _report_schema,
        }
    ),
    Opt("onPermanentFailure"): Map({Opt("report"): _report_schema}),
    Opt("onSuccess"): Map({Opt("report"): _report_schema}),
    Opt("environment"): Seq(Map({"key": Str(), "value": Str()})),
    Opt("env_file"): Str(),
    Opt("executionTimeout"): Float(),
    Opt("killTimeout"): Float(),
    Opt("statsd"): Map({"prefix": Str(), "host": Str(), "port": Int()}),
    # Int() is tried first so a numeric ``user: 1000`` parses as the integer
    # 1000 (a uid/gid), reaching the isinstance(..., int) branches in
    # _resolve_user_group. With Str() first, strictyaml's union would match the
    # always-accepting Str() and a bare number would arrive as the string
    # "1000", silently looked up as a login *name* (getpwnam("1000")) instead.
    # A non-numeric name (``user: www-data``) fails Int() and uses Str().
    Opt("user"): Int() | Str(),
    Opt("group"): Int() | Str(),
    Opt("streamPrefix"): Str(),
    Opt("enabled"): Bool(),
}

_job_schema_dict = dict(_job_defaults_common)
_job_schema_dict.update(
    {
        "name": Str(),
        "command": Str() | Seq(Str()),
        "schedule": Str()
        | Map(
            {
                # An explicit second opts the job into second-level scheduling
                # (see schedule_object_to_crontab / yacron2.cron). Omit it and
                # the schedule stays minute-granular, exactly as before.
                Opt("second"): Str(),
                Opt("minute"): Str(),
                Opt("hour"): Str(),
                Opt("dayOfMonth"): Str(),
                Opt("month"): Str(),
                Opt("year"): Str(),
                Opt("dayOfWeek"): Str(),
            }
        ),
    }
)

CONFIG_SCHEMA = EmptyDict() | Map(
    {
        Opt("defaults"): Map(_job_defaults_common),
        Opt("jobs"): Seq(Map(_job_schema_dict)),
        Opt("web"): Map(
            {
                "listen": Seq(Str()),
                Opt("headers"): MapPattern(Str(), Str()),
                # optional opt-in bearer-token auth for the web API
                Opt("authToken"): Map(
                    {
                        Opt("value"): EmptyNone() | Str(),
                        Opt("fromFile"): EmptyNone() | Str(),
                        Opt("fromEnvVar"): EmptyNone() | Str(),
                    }
                ),
                # octal permissions to apply to a unix:// listen socket
                Opt("socketMode"): Str(),
                # serve the browser dashboard at "/" (default true)
                Opt("ui"): Bool(),
                # native Prometheus exposition at GET /metrics (default on
                # whenever the web API is on). `metrics: false` disables it;
                # the map form additionally exempts /metrics from authToken
                # (public) and overrides the duration-histogram buckets.
                # See yacron2/prometheus.py.
                Opt("metrics"): Bool()
                | Map(
                    {
                        Opt("enabled"): Bool(),
                        Opt("public"): Bool(),
                        Opt("durationBuckets"): Seq(Float()),
                    }
                ),
            }
        ),
        # Optional cluster section: gate scheduled jobs on a leadership
        # backend. The gossip backend (default) attests the job set against a
        # static peer list over mutual TLS (see yacron2.cluster); the
        # kubernetes/etcd backends use a lease store (see yacron2.backends).
        # listen/tls/peers are required for gossip only -- enforced in
        # _build_cluster_config, not the schema, so a lease backend need not
        # carry them.
        Opt("cluster"): Map(
            {
                # gossip (default) | kubernetes | etcd
                Opt("backend"): Enum(["gossip", "kubernetes", "etcd"]),
                # --- gossip transport (required for backend: gossip) ---
                # host:port the mTLS cluster listener binds to
                Opt("listen"): Str(),
                Opt("tls"): Map(
                    {
                        "ca": Str(),  # trust anchor for peer certificates
                        "cert": Str(),  # this node's certificate
                        "key": Str(),  # this node's private key
                    }
                ),
                Opt("peers"): Seq(Map({"host": Str()})),
                Opt("nodeName"): Str(),
                Opt("interval"): Int(),
                Opt("driftAfter"): Int(),
                Opt("connectTimeout"): Int(),
                # run scheduled jobs on the elected leader only (default false;
                # implicitly true for the lease backends)
                Opt("electLeader"): Bool(),
                # how leader-gated jobs spread across the quorate cluster
                # (gossip only; rejected for the lease backends)
                Opt("distribution"): Enum(["single-leader", "spread"]),
                # --- kubernetes Lease backend (backend: kubernetes) ---
                Opt("kubernetes"): Map(
                    {
                        Opt("leaseName"): Str(),
                        Opt("leaseNamespace"): EmptyNone() | Str(),
                        Opt("leaseDurationSeconds"): Int(),
                        Opt("renewDeadlineSeconds"): Int(),
                        Opt("retryPeriodSeconds"): Int(),
                        Opt("identity"): EmptyNone() | Str(),
                        Opt("kubeconfig"): EmptyNone() | Str(),
                        Opt("apiServer"): EmptyNone() | Str(),
                        Opt("clientLibrary"): Enum(
                            ["auto", "http", "library"]
                        ),
                    }
                ),
                # --- etcd lease-backed election backend (backend: etcd) ---
                Opt("etcd"): Map(
                    {
                        Opt("endpoints"): Seq(Str()),
                        Opt("electionName"): Str(),
                        Opt("ttl"): Int(),
                        Opt("username"): EmptyNone() | Str(),
                        Opt("password"): Map(
                            {
                                Opt("value"): EmptyNone() | Str(),
                                Opt("fromFile"): EmptyNone() | Str(),
                                Opt("fromEnvVar"): EmptyNone() | Str(),
                            }
                        ),
                        Opt("tls"): Map(
                            {
                                Opt("ca"): EmptyNone() | Str(),
                                Opt("cert"): EmptyNone() | Str(),
                                Opt("key"): EmptyNone() | Str(),
                            }
                        ),
                    }
                ),
            }
        ),
        Opt("include"): Seq(Str()),
        Opt("logging"): Map(
            {
                "version": Int(),
                Opt("incremental"): Bool(),
                Opt("disable_existing_loggers"): Bool(),
                Opt("formatters"): YamlAny(),
                Opt("filters"): YamlAny(),
                Opt("handlers"): YamlAny(),
                Opt("loggers"): YamlAny(),
                Opt("root"): YamlAny(),
            }
        ),
    }
)


# Slightly modified version of https://stackoverflow.com/a/7205672/2211825
def mergedicts(dict1, dict2):
    for k in set(dict1.keys()).union(dict2.keys()):
        if k in dict1 and k in dict2:
            v1 = dict1[k]
            v2 = dict2[k]
            if isinstance(v1, dict) and isinstance(v2, dict):
                yield (k, dict(mergedicts(v1, v2)))
            elif isinstance(v1, dict) and v2 is None:  # modification
                yield (k, dict(mergedicts(v1, {})))
            elif isinstance(v1, list) and isinstance(v2, list):  # merge lists
                if k == "environment":
                    # environment is a list of {key, value}; merge by key so a
                    # job's variable overrides the default instead of producing
                    # a duplicate-keyed concatenation.
                    merged = {e["key"]: e["value"] for e in v1}
                    for e in v2:
                        merged[e["key"]] = e["value"]
                    yield (
                        k,
                        [
                            {"key": kk, "value": vv}
                            for kk, vv in merged.items()
                        ],
                    )
                elif k == "fingerprint":
                    # sentry "fingerprint" is a replace-not-append setting: a
                    # job (or a defaults block) that supplies its own
                    # fingerprint must override the default entirely. Plain
                    # concatenation would silently prepend the three default
                    # entries, making custom Sentry issue grouping impossible.
                    yield (k, v2)
                else:
                    yield (k, v1 + v2)
            else:
                yield (k, v2)
        elif k in dict1:
            yield (k, dict1[k])
        else:
            yield (k, dict2[k])


def schedule_object_to_crontab(spec: Dict[str, Any]) -> str:
    """Render the object form of a ``schedule`` to a parse-crontab string.

    parse-crontab's field layout is ``[second] minute hour dayOfMonth month
    dayOfWeek [year]``: a bare 5-field line has an implicit second of 0 and any
    year, a 6-field line adds a trailing ``year`` column, and a 7-field line
    adds a leading ``second`` column.  We emit only the columns actually
    specified, so a schedule that uses neither ``second`` nor ``year`` still
    renders as the exact 5-field line it always did -- keeping its job-set
    fingerprint stable, and equal to the equivalent crontab-string spelling.

    Note this is the single source of truth for the object->crontab mapping,
    shared by parsing (:meth:`JobConfig._parse_schedule`), the web UI's
    schedule label (:func:`yacron2.cron.schedule_str`) and the fingerprint
    (:func:`yacron2.fingerprint._schedule_repr`), so those cannot drift.
    """
    minute = spec.get("minute", "*")
    hour = spec.get("hour", "*")
    day = spec.get("dayOfMonth", "*")
    month = spec.get("month", "*")
    dow = spec.get("dayOfWeek", "*")
    second = spec.get("second")
    year = spec.get("year")
    if second is not None:
        # 7-field: an explicit seconds column. year defaults to "*" (any).
        return "{} {} {} {} {} {} {}".format(
            second,
            minute,
            hour,
            day,
            month,
            dow,
            year if year is not None else "*",
        )
    if year is not None:
        # 6-field: parse-crontab reads the trailing column as the year.
        return "{} {} {} {} {} {}".format(minute, hour, day, month, dow, year)
    return "{} {} {} {} {}".format(minute, hour, day, month, dow)


def schedule_has_seconds(
    schedule_unparsed: Union[str, Dict[str, Any]],
) -> bool:
    """Whether a schedule pins specific seconds (fires at second granularity).

    True for the object ``second:`` key and for a full 7-field crontab string;
    such jobs make the scheduler tick once per second rather than once per
    minute (see :meth:`yacron2.cron.Cron._needs_subminute`).  A 5- or 6-field
    string, ``@reboot`` and the ``@daily``/``@hourly``/... nicknames never do.
    """
    if isinstance(schedule_unparsed, dict):
        # Derive from the ACTUAL rendered field count, not mere key presence:
        # a blank/whitespace ``second:`` value (e.g. a leftover ``second:``
        # with no value) renders a leading empty column that vanishes under
        # parse-crontab's whitespace split, leaving a minute-granular 5-/6-
        # field line. Keying off presence alone would set has_seconds True for
        # such a line and force the whole scheduler to tick per-second for a
        # job that only ever fires once a minute.
        return len(schedule_object_to_crontab(schedule_unparsed).split()) == 7
    if isinstance(schedule_unparsed, str):
        stripped = schedule_unparsed.strip()
        if not stripped or stripped.startswith("@"):
            return False
        # parse-crontab only reads a leading seconds column at 7 fields; a 5-
        # or 6-field line has an implicit second of 0 (6th field is the year).
        return len(stripped.split()) == 7
    return False


class JobConfig:
    # One JobConfig exists per configured job for the life of the process (and
    # is rebuilt on every reload), so trimming its per-instance __dict__ with
    # __slots__ cuts steady-state memory and speeds attribute access on the
    # scheduler's hot path.  Every attribute the class ever sets -- including
    # the host-resolved uid/gid/username and the configured user/group kept for
    # the fingerprint -- must be listed here, or assigning it raises
    # AttributeError.  Nothing outside this class assigns attributes to a
    # JobConfig instance (the prometheus per-job accumulators live on their own
    # slotted _JobMetrics), so the set is closed.
    __slots__ = (
        "name",
        "command",
        "schedule_unparsed",
        "schedule",
        "has_seconds",
        "shell",
        "concurrencyPolicy",
        "clusterPolicy",
        "captureStderr",
        "captureStdout",
        "streamPrefix",
        "saveLimit",
        "maxLineLength",
        "utc",
        "enabled",
        "timezone",
        "failsWhen",
        "onFailure",
        "onPermanentFailure",
        "onSuccess",
        "env_file",
        "environment",
        "executionTimeout",
        "killTimeout",
        "statsd",
        "user",
        "group",
        "uid",
        "gid",
        "username",
    )

    def __init__(self, config: dict) -> None:
        self.name = config["name"]  # type: str
        self.command = config["command"]  # type: Union[str, List[str]]
        self.schedule_unparsed = config.pop("schedule")
        self.schedule: Union[CronTab, str] = self._parse_schedule(
            self.schedule_unparsed
        )
        # True when the schedule pins specific seconds; the scheduler then
        # ticks per-second for this job instead of per-minute (yacron2.cron).
        self.has_seconds: bool = schedule_has_seconds(self.schedule_unparsed)
        self.shell = config.pop("shell")
        self.concurrencyPolicy = config.pop("concurrencyPolicy")
        self.clusterPolicy = config.pop("clusterPolicy")
        self.captureStderr = config.pop("captureStderr")
        self.captureStdout = config.pop("captureStdout")
        self.streamPrefix = config.pop("streamPrefix")
        self.saveLimit = config.pop("saveLimit")
        self.maxLineLength = config.pop("maxLineLength")
        self.utc = config.pop("utc")
        self.enabled: bool = config.pop("enabled")
        # depends on self.utc, so resolve after it is set
        self.timezone: Optional[datetime.tzinfo] = self._resolve_timezone(
            config.pop("timezone")
        )

        self.failsWhen = config.pop("failsWhen")
        self.onFailure = config.pop("onFailure")
        self.onPermanentFailure = config.pop("onPermanentFailure")
        self.onSuccess = config.pop("onSuccess")

        self.env_file = config.pop("env_file")
        self.environment = config.pop("environment")
        if self.env_file is not None:
            self._merge_env_file()

        self.executionTimeout = config.pop("executionTimeout")
        self.killTimeout = config.pop("killTimeout")
        self.statsd = config.pop("statsd")

        self.uid = None  # type: Optional[int]
        self.gid = None  # type: Optional[int]
        # Resolved login name of the target user, used by the child process'
        # privilege-drop (os.initgroups) so it gets the user's supplementary
        # groups instead of inheriting root's. None when unknown.
        self.username: Optional[str] = None
        self._resolve_user_group(config)

        self._validate_numeric_ranges()

    def _parse_schedule(self, schedule_unparsed) -> Union[CronTab, str]:
        if isinstance(schedule_unparsed, str):
            if schedule_unparsed == "@reboot":
                return schedule_unparsed
            return self._crontab(schedule_unparsed)
        if isinstance(schedule_unparsed, dict):
            tab = schedule_object_to_crontab(schedule_unparsed)
            logger.debug("Converted schedule to %r", tab)
            return self._crontab(tab)
        raise ConfigError("invalid schedule: {!r}".format(schedule_unparsed))

    def _crontab(self, tab: str) -> CronTab:
        # parse-crontab raises ValueError on a malformed field (a bad range, an
        # out-of-range second, the wrong field count). Surface it as a
        # ConfigError naming the offending expression, so a bad schedule fails
        # the config load with a clear message the reload loop can log, rather
        # than as an anonymous traceback.
        try:
            return CronTab(tab)
        except ValueError as err:
            raise ConfigError(
                "invalid schedule {!r}: {}".format(tab, err)
            ) from err

    def _resolve_timezone(
        self, timezone: Optional[str]
    ) -> Optional[datetime.tzinfo]:
        if timezone is not None:
            try:
                return ZoneInfo(timezone)
            except (ZoneInfoNotFoundError, ValueError) as err:
                raise ConfigError(
                    "unknown timezone: {}".format(timezone)
                ) from err
        if self.utc:
            return datetime.timezone.utc
        return None

    def _merge_env_file(self) -> None:
        try:
            file_environs = parse_environment_file(self.env_file)
        except OSError as e:
            raise ConfigError("Could not load env_file: {}".format(e)) from e
        # config-defined variables override those loaded from the file
        config_environs = {
            env["key"]: env["value"] for env in self.environment
        }
        file_environs.update(config_environs)
        self.environment = [
            {"key": key, "value": value}
            for key, value in file_environs.items()
        ]

    def _resolve_user_group(self, config: dict) -> None:
        user = config.pop("user", None)
        group = config.pop("group", None)
        # Retain the *configured* user/group (string name or numeric id, or
        # None) for the job-set fingerprint.  The resolved uid/gid below are
        # host-specific (the same name can map to different ids on different
        # hosts), so fingerprinting must use the configured value, not them.
        self.user: Optional[Union[str, int]] = user
        self.group: Optional[Union[str, int]] = group
        if user is None and group is None:
            return  # nothing to switch to: nothing POSIX-only to resolve

        # Windows has no setuid/setgid model that maps onto this feature, so
        # reject it with a clear error instead of silently running the job as
        # the wrong account.  Spelled as ``sys.platform == "win32"`` (rather
        # than platform.IS_WINDOWS) so the type checker statically prunes the
        # POSIX-only imports/calls below on Windows.
        if sys.platform == "win32":
            raise ConfigError(
                "Job {}: changing user/group is not supported on "
                "Windows".format(self.name)
            )

        # POSIX only: the passwd/group databases live in modules that don't
        # exist on Windows; imported lazily (only reached above on POSIX).
        from grp import getgrnam
        from pwd import getpwnam, getpwuid

        if user is not None:
            if isinstance(user, int):
                self.uid = user
                # Derive the primary gid (and login name) from the passwd
                # database so a numeric ``user`` without an explicit ``group``
                # does not silently keep yacron2's (root) gid 0.
                try:
                    pw = getpwuid(user)
                except KeyError:
                    pw = None
                if pw is not None:
                    self.username = pw.pw_name
                    if self.gid is None:
                        self.gid = pw.pw_gid
            else:
                try:
                    pw = getpwnam(user)
                    self.uid = pw.pw_uid
                    self.gid = pw.pw_gid
                    self.username = pw.pw_name
                except KeyError as e:
                    raise ConfigError(
                        "User not found: {!r}".format(user)
                    ) from e

        if group is not None:
            if isinstance(group, int):
                self.gid = group
            else:
                try:
                    self.gid = getgrnam(group).gr_gid
                except KeyError as e:
                    raise ConfigError(
                        "Group not found: {!r}".format(group)
                    ) from e

        if self.uid is not None or self.gid is not None:
            if os.geteuid() != 0:
                raise ConfigError(
                    "Job {} wants to change user or group, "
                    "but yacron2 is not running as superuser".format(self.name)
                )

    def _validate_numeric_ranges(self) -> None:
        # strictyaml only enforces the type (Int/Float); fail fast on values
        # that would otherwise produce obscure runtime behavior instead of a
        # clear configuration error.
        def require(condition: bool, message: str) -> None:
            if not condition:
                raise ConfigError("Job {}: {}".format(self.name, message))

        require(self.saveLimit >= 0, "saveLimit must be >= 0")
        require(self.maxLineLength > 0, "maxLineLength must be > 0")
        require(self.killTimeout >= 0, "killTimeout must be >= 0")
        if self.executionTimeout is not None:
            require(
                self.executionTimeout > 0,
                "executionTimeout must be > 0 when set",
            )
        retry = self.onFailure.get("retry")
        if retry is not None:
            # -1 is the documented sentinel for "retry forever".
            require(
                retry["maximumRetries"] >= -1,
                "onFailure.retry.maximumRetries must be >= -1",
            )
            require(
                retry["initialDelay"] >= 0,
                "onFailure.retry.initialDelay must be >= 0",
            )
            require(
                retry["maximumDelay"] > 0,
                "onFailure.retry.maximumDelay must be > 0",
            )
            require(
                retry["backoffMultiplier"] > 0,
                "onFailure.retry.backoffMultiplier must be > 0",
            )


def parse_environment_file(path: str) -> Dict[str, str]:
    """
    Parse environment variables from file.

    Handles comments (lines starting with ``#``) and blank lines.
    Variables must be specified in ``VARIABLE_NAME=CONTENT`` format.

    :param path: Path to the environment file.
    :raise ConfigError: If a line in the file is not parsable
        (the ``=`` key-value separation character is missing).
    :raise OSError: If an error occurred while opening the file at ``path``.
    :return: key-value map of environment variables.
    """
    environ: Dict[str, str] = {}

    with open(path, "r", encoding="utf-8") as env_file:
        # file parsing
        # you may want to use the `dotenv` library to do the job
        for line in env_file.readlines():
            line = line.strip(" ").rstrip("\n")
            if line.startswith("#") or not line:
                continue
            if "=" not in line:
                raise ConfigError(
                    "Invalid line in env_file: '{}'".format(line)
                )
            key, value = line.split("=", 1)
            key = key.strip(" ")
            value = value.strip(" ")
            environ[key] = value

    return environ


# Hosts that mean "all interfaces" in a `listen` address. A peer entry can't be
# string-matched against these, so a node self-listed by hostname behind a
# wildcard listen needs the nodeName-based recognition in _is_self_listed.
_WILDCARD_LISTEN_HOSTS = frozenset({"0.0.0.0", "::", "[::]", "*", ""})

# The same, split by address family: a wildcard bind holds the port on every
# interface OF ITS FAMILY, which is what makes a same-family literal loopback
# peer entry unambiguously self (see _is_self_listed). "*" and "" bind
# everything, so they belong to both.
_V4_WILDCARD_LISTEN_HOSTS = frozenset({"0.0.0.0", "*", ""})
_V6_WILDCARD_LISTEN_HOSTS = frozenset({"::", "[::]", "*", ""})


def _loopback_ip_version(host: str) -> Optional[int]:
    """The IP version (4 or 6) of a literal loopback host, else ``None``.

    Accepts the bracketed IPv6 form peer entries use (``[::1]``).  Pure
    literal parsing via :mod:`ipaddress`: a hostname -- even ``localhost`` --
    never parses, so no DNS resolution happens and nothing is guessed.
    """
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return None
    return ip.version if ip.is_loopback else None


def _is_self_listed(peer_host: str, listen: str, node_name: str) -> bool:
    """Whether a configured peer entry *unambiguously* points back at us.

    A self entry must be dropped from `peers`: it never counts toward
    agreement, yet it inflates `cluster_size()` -- and so the quorum threshold,
    the size-divergence gate, and the 2-node refusal -- by one. We only drop an
    entry here when it can be *only* this node, never another member:

    * an exact match of our own `listen` address; or
    * the common wildcard case -- a `listen` bound to all interfaces
      (`0.0.0.0` / `::`) self-listed by `nodeName` -- recognised structurally
      when the entry's host equals our `nodeName` *exactly* (which defaults to
      the system hostname, the name the cert SAN and peer address use by
      convention), on the same port; or
    * a *literal loopback* entry (`127.0.0.1` / `[::1]`) on the same port,
      under a wildcard `listen` of the matching family: loopback traffic
      never leaves this host and the wildcard bind holds the port on every
      interface of that family, so connecting to the entry can only land on
      our own listener.  Matched by :func:`_loopback_ip_version` (literal
      parsing only); `localhost` is deliberately NOT matched here (resolving
      it would be the DNS guessing this function refuses -- it gets an
      advisory instead, see :func:`_likely_self_loopback`).

    The match is deliberately *exact*: never drop a peer on a fuzzy match of
    the host FQDN's *first label* against a bare `nodeName` (e.g. dropping
    `node-a.internal` when `nodeName` is `node-a`). A peer host's DNS labels
    have no required relationship to any `nodeName`, so such an over-match can
    silently drop a genuinely distinct member that merely shares a first label
    (`web.dc1.internal` vs our short hostname `web`), shrinking *our* `N` below
    everyone else's -- which either pins `Leader` jobs closed cluster-wide on a
    permanent size-divergence conflict (with no warning emitted), or, if it
    lowers our quorum threshold, opens a split-brain. No runtime backstop
    re-adds a config-dropped peer, so the damage would be permanent.

    A genuine self-listing the exact match does not catch -- e.g. a self listed
    by its FQDN while `nodeName` is the short label -- is simply not dropped
    here; it falls back to the runtime `STATUS_SELF` recognition in
    `ClusterManager.cluster_size` once its self-poll succeeds. The brief `N`
    inflation before that first poll is the *safe* direction (a higher quorum,
    never a split-brain). No DNS resolution is done at config time (it would
    block the per-reload parse).
    """
    if peer_host == listen:
        return True
    listen_host, _, listen_port = listen.rpartition(":")
    if listen_host not in _WILDCARD_LISTEN_HOSTS:
        return False
    peer_h, _, peer_port = peer_host.rpartition(":")
    if peer_port != listen_port:
        return False
    if peer_h == node_name:
        return True
    # A literal loopback of the same family as the wildcard bind, on our own
    # port: unambiguously this node (see the docstring). The family must
    # match -- a "::"-only bind does not necessarily accept v4, so 127.0.0.1
    # there could in principle be a different colocated process.
    version = _loopback_ip_version(peer_h)
    if version == 4 and listen_host in _V4_WILDCARD_LISTEN_HOSTS:
        return True
    return version == 6 and listen_host in _V6_WILDCARD_LISTEN_HOSTS


def _likely_self_fqdn(peer_host: str, listen: str, node_name: str) -> bool:
    """Whether a peer entry *looks like* this node listed by its FQDN.

    A heuristic for diagnostics only (never used to drop a peer -- that fuzzy
    match is exactly the dangerous over-match :func:`_is_self_listed` refuses).
    True when, under a wildcard ``listen``, a peer on our port has a host
    whose first DNS label equals our ``nodeName`` but is not an exact match
    (which :func:`_is_self_listed` would already have dropped). Used to warn
    that a cluster declared as 3 nodes may really be a degenerate 2-node one.
    """
    if _is_self_listed(peer_host, listen, node_name):
        return False
    listen_host, _, listen_port = listen.rpartition(":")
    if listen_host not in _WILDCARD_LISTEN_HOSTS:
        return False
    peer_h, _, peer_port = peer_host.rpartition(":")
    if peer_port != listen_port:
        return False
    return peer_h.split(".", 1)[0] == node_name and peer_h != node_name


def _likely_self_loopback(peer_host: str, listen: str) -> bool:
    """Whether a peer entry *looks like* this node listed via loopback.

    A diagnostics-only heuristic like :func:`_likely_self_fqdn` (never used
    to drop a peer).  True for a loopback-ish entry on our listen port that
    :func:`_is_self_listed` could not drop as unambiguous: ``localhost`` (a
    name, whose family is unknowable without the DNS resolution config time
    refuses), or a loopback literal under a non-wildcard or other-family
    ``listen``.  Loopback traffic never leaves this host, so such an entry
    can never be another cluster member: at best it is this node itself --
    hiding a degenerate effective size behind an inflated declared one -- and
    at worst dead weight that raises the quorum threshold.  Used to warn when
    the remainder would leave the real cluster at <= 2 nodes.
    """
    _, _, listen_port = listen.rpartition(":")
    peer_h, _, peer_port = peer_host.rpartition(":")
    if peer_port != listen_port:
        # a colocated second daemon on another port of this host is a
        # legitimate (if unusual) distinct member; only our own port is
        # suspect.
        return False
    return peer_h == "localhost" or _loopback_ip_version(peer_h) is not None


def _cluster_base(raw: dict) -> "Dict[str, Any]":
    """Fill the shared cluster defaults over a raw (schema-validated) block.

    Covers the keys every backend uses (backend, nodeName, connectTimeout,
    electLeader, distribution, and the inert gossip cadence fields). Each
    backend's builder then layers on its own block.
    """
    cfg: Dict[str, Any] = dict(DEFAULT_CLUSTER)
    cfg.update(raw)
    if not cfg.get("nodeName"):
        # a stable, human-readable identity for this node, used as the lease
        # identity and so a gossip peer can recognise itself in someone else's
        # peer list; the system hostname is a sensible default.
        cfg["nodeName"] = socket.gethostname()
    if cfg["connectTimeout"] <= 0:
        raise ConfigError("cluster.connectTimeout must be > 0")
    return cfg


def _build_cluster_config(raw: dict) -> ClusterConfig:
    """Build a ClusterConfig, dispatching on the chosen ``backend``."""
    backend = raw.get("backend", DEFAULT_CLUSTER["backend"])
    if backend == "kubernetes":
        return _build_kubernetes_cluster_config(raw)
    if backend == "etcd":
        return _build_etcd_cluster_config(raw)
    return _build_gossip_cluster_config(raw)


def _build_gossip_cluster_config(raw: dict) -> ClusterConfig:
    # Fill defaults over the raw (schema-validated) cluster block and validate
    # the numeric fields, mirroring _validate_numeric_ranges for jobs.
    cfg = _cluster_base(raw)
    _reject_foreign_store_blocks(cfg, "gossip")
    # listen/tls/peers are schema-optional now (so a lease backend need not
    # carry them), but the gossip transport requires all three.
    for key in ("listen", "tls", "peers"):
        if cfg.get(key) is None:
            raise ConfigError(
                "cluster.backend gossip requires cluster.{}".format(key)
            )
    if cfg["interval"] <= 0:
        raise ConfigError("cluster.interval must be > 0")
    if cfg["driftAfter"] < 1:
        raise ConfigError("cluster.driftAfter must be >= 1")

    # Validate every address is a well-formed host:port up front, so a typo
    # (a missing port, a non-numeric port) fails the config load pointing at
    # the offending value instead of surfacing later as an opaque per-peer
    # connection error. Mirrors yacron2.cluster._split_host_port, plus a port
    # range check; anything this accepts also parses at runtime.
    def _require_host_port(addr: str, what: str) -> None:
        # Bracketed IPv6 (``[2001:db8::1]:8900``): host is inside the brackets.
        if addr.startswith("["):
            bracket, sep, port = addr.rpartition("]:")
            host = bracket[1:]
            if (
                not sep
                or not host
                or not port.isdigit()
                or not 0 < int(port) <= 65535
            ):
                raise ConfigError(
                    "cluster.{} must be [ipv6]:port, got {!r}".format(
                        what, addr
                    )
                )
            return
        host, _, port = addr.rpartition(":")
        # A bare (unbracketed) IPv6 literal has more colons left in ``host``;
        # rpartition would silently mis-split it (``2001:db8::1`` ->
        # host=``2001:db8:``, port=``1``), passing validation and then failing
        # opaquely at connect/bind time -- and for a peer, silently dropping it
        # from quorum with no error. Require the bracketed form instead.
        if ":" in host:
            raise ConfigError(
                "cluster.{} looks like a bare IPv6 address; write it as "
                "[ipv6]:port, got {!r}".format(what, addr)
            )
        if not host or not port.isdigit() or not 0 < int(port) <= 65535:
            raise ConfigError(
                "cluster.{} must be host:port, got {!r}".format(what, addr)
            )

    _require_host_port(cfg["listen"], "listen")
    for peer in cfg["peers"]:
        _require_host_port(peer["host"], "peers[].host")

    # De-duplicate peers and drop any entry pointing at our own listen address.
    # ClusterView keys peers by host (so duplicates collapse) and a self-listed
    # peer never counts toward agreement -- but cluster_size() (and thus the
    # quorum threshold) is derived from this list, so a duplicate or self entry
    # would otherwise inflate the quorum and cost fault tolerance. Keep the
    # first occurrence to preserve configured order. _is_self_listed also
    # catches the common wildcard case (a `0.0.0.0` listen self-listed by
    # hostname), so config-time N matches the runtime N every correctly-
    # configured peer declares. (An exotic self-listing that escapes it -- e.g.
    # an FQDN vs the short nodeName -- still degrades to the runtime
    # STATUS_SELF exclusion in ClusterManager.cluster_size.)
    seen: "set[str]" = set()
    deduped: List[Dict[str, Any]] = []
    for peer in cfg["peers"]:
        host = peer["host"]
        if (
            _is_self_listed(host, cfg["listen"], cfg["nodeName"])
            or host in seen
        ):
            continue
        seen.add(host)
        deduped.append(peer)
    cfg["peers"] = deduped

    if cfg["electLeader"]:
        # `peers` lists every OTHER member, so the cluster is that many plus
        # this node.
        size = len(cfg["peers"]) + 1
        if size == 2:
            # A 2-node quorum is 2: both must be up for *either* to run, so it
            # is strictly worse than a single replica (lower availability, and
            # still no failover) with no upside. Refuse it rather than silently
            # degrade. This keys off the declared size, so a 3+ node cluster
            # with a peer transiently down (a rolling deploy) is unaffected.
            raise ConfigError(
                "cluster.electLeader needs a fault-tolerant cluster, but "
                "this config declares only 2 nodes (1 peer). A quorum of 2 "
                "requires both nodes up for either to run, so it is strictly "
                "worse than a single replica. Use 3 or more nodes (an odd "
                "count is best), or run a single replica without electLeader."
            )
    return ClusterConfig(cfg)


def _reject_lease_spread(cfg: dict, backend: str) -> None:
    # A single lease holder cannot also be a per-job (spread) owner: there is
    # one fenced identity, not a quorate set to rendezvous-hash across.
    if cfg.get("distribution", "single-leader") != "single-leader":
        raise ConfigError(
            "cluster.distribution: spread is not supported with the {!r} "
            "backend (a single lease holder cannot fan jobs out per-node); "
            "use distribution: single-leader, or the gossip backend".format(
                backend
            )
        )


# Lease store sub-blocks. Each lease builder reads ONLY its own; a block under
# the wrong backend is rejected so the operator's intended endpoints/TLS/creds
# are never silently discarded (see _reject_foreign_store_blocks).
_LEASE_STORE_KEYS = ("etcd", "kubernetes")

# Kubernetes object-name charsets, used to keep leaseName/leaseNamespace clear
# of URL path metacharacters ('/', '?', '#', whitespace) that could retarget
# the apiserver request (see _K8sHttpTransport._lease_url).
_RFC1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_RFC1123_SUBDOMAIN = re.compile(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")


def _reject_foreign_store_blocks(cfg: dict, backend: str) -> None:
    """Reject a lease store sub-block that does not match the chosen backend.

    Both store blocks are schema-optional and can be present at once, and each
    builder consumes ONLY its own -- so a block carried under the wrong
    ``backend:`` (a copy-paste that changed only ``backend:``, an ``etcd:``
    block under ``backend: kubernetes``, or a stray store block under
    ``backend: gossip``) would be silently ignored, discarding the operator's
    intended endpoints / TLS / credentials and arbitrating leadership against
    an unintended (default) store -- landing on either failure the subsystem
    exists to prevent (a job that never runs, or one that double-runs). Fail
    loudly.
    """
    for key in _LEASE_STORE_KEYS:
        if key == backend:
            continue
        if cfg.get(key) is not None:
            raise ConfigError(
                "cluster.{} is configured but cluster.backend is {!r}; that "
                "store block would be silently ignored. Set backend: {} to "
                "use it, or remove the cluster.{} block.".format(
                    key, backend, key, key
                )
            )


# Cluster keys only the gossip transport consumes. A lease backend silently
# ignores them, so a lease config that carries them (e.g. copied from a gossip
# example and re-pointed with `backend:`) does not do what it looks like --
# most dangerously a `tls` block, which the operator may believe secures
# peer/store traffic when it does nothing. interval/driftAfter live in
# DEFAULT_CLUSTER so are always present in the built cfg; detect them from the
# *raw* block instead, where they appear only if the operator wrote them.
_GOSSIP_ONLY_CLUSTER_KEYS = (
    "listen",
    "tls",
    "peers",
    "interval",
    "driftAfter",
)


def _lease_advisories(raw: dict, backend: str) -> List[str]:
    """Non-fatal advisories for a lease (kubernetes/etcd) cluster block.

    Surfaced (once, via :func:`cluster_config_warnings`) rather than raised so
    an upgrade does not fail a previously-accepted config; promote to a hard
    ConfigError behind a deprecation window if stricter validation is wanted.
    """
    advisories: List[str] = []
    present = [k for k in _GOSSIP_ONLY_CLUSTER_KEYS if raw.get(k) is not None]
    if present:
        msg = (
            "cluster.{} configured but ignored by the {!r} backend (those "
            "keys apply only to backend: gossip)".format(
                ", cluster.".join(present), backend
            )
        )
        if "tls" in present:
            # the one with a security-relevant false belief attached.
            msg += (
                "; note cluster.tls does NOT secure the lease store -- the "
                "{} store's TLS is configured under cluster.{}".format(
                    backend, backend
                )
            )
        advisories.append(msg)
    if raw.get("electLeader") is False:
        # the override to True is unconditional (a lease backend is opting into
        # leadership); flag the swallowed explicit contradiction.
        advisories.append(
            "cluster.electLeader: false is ignored by the {!r} backend; a "
            "lease backend always enables leader election".format(backend)
        )
    return advisories


def _resolve_secret(spec: Optional[dict], what: str) -> Optional[str]:
    """Resolve a value/fromFile/fromEnvVar secret block, or ``None`` if unset.

    Mirrors :meth:`yacron2.cron.Cron._resolve_web_token`, but tolerates "no
    source configured" by returning ``None`` (etcd may need no auth at all).
    A source that *is* configured yet resolves empty fails closed.
    """
    if not spec:
        return None
    if spec.get("value"):
        secret = str(spec["value"])
    elif spec.get("fromFile"):
        try:
            with open(spec["fromFile"], "rt") as secret_file:
                secret = secret_file.read().strip()
        except OSError as ex:
            raise ConfigError(
                "{}.fromFile could not be read: {}".format(what, ex)
            ) from ex
    elif spec.get("fromEnvVar"):
        secret = os.environ.get(spec["fromEnvVar"], "")
    else:
        return None  # no source configured
    if not secret:
        raise ConfigError(
            "{} is configured but resolved to an empty secret".format(what)
        )
    return secret


def _build_kubernetes_cluster_config(raw: dict) -> ClusterConfig:
    cfg = _cluster_base(raw)
    _reject_lease_spread(cfg, "kubernetes")
    _reject_foreign_store_blocks(cfg, "kubernetes")
    k8s = dict(DEFAULT_K8S)
    k8s.update(cfg.get("kubernetes") or {})
    cfg["kubernetes"] = k8s
    if not k8s.get("identity"):
        # the lease holderIdentity that distinguishes this node; default it to
        # the (already-defaulted) nodeName.
        k8s["identity"] = cfg["nodeName"]
    # leaseName/leaseNamespace are spliced into the apiserver URL path (see
    # _K8sHttpTransport._lease_url) and passed to the native client; constrain
    # them to the Kubernetes RFC1123 charset so a stray '/', '?', '#' or space
    # cannot retarget the request (silently never acquiring the lease, or
    # the HTTP and native transports resolve different resources).
    lease_name = k8s["leaseName"]
    if not isinstance(lease_name, str) or (
        len(lease_name) > 253 or not _RFC1123_SUBDOMAIN.match(lease_name)
    ):
        raise ConfigError(
            "cluster.kubernetes.leaseName must be a valid RFC1123 name "
            "(lowercase alphanumeric, '-' or '.', <= 253 chars); got "
            "{!r}".format(lease_name)
        )
    lease_ns = k8s.get("leaseNamespace")
    if lease_ns is not None and (
        not isinstance(lease_ns, str)
        or len(lease_ns) > 63
        or not _RFC1123_LABEL.match(lease_ns)
    ):
        raise ConfigError(
            "cluster.kubernetes.leaseNamespace must be a valid RFC1123 label "
            "(lowercase alphanumeric or '-', <= 63 chars); got {!r}".format(
                lease_ns
            )
        )
    # The apiserver override carries the in-cluster ServiceAccount bearer token
    # on every request; a plaintext http:// target would send that high-value
    # credential in cleartext (aiohttp ignores the SSL context for an http URL)
    # and expose the lease store to MITM. Require https, mirroring the etcd
    # backend's auth-over-https guard. (A local kube-rbac-proxy that genuinely
    # needs http should front it with https or use a kubeconfig.)
    api_server = k8s.get("apiServer")
    if api_server and not str(api_server).lower().startswith("https://"):
        raise ConfigError(
            "cluster.kubernetes.apiServer must be an https:// URL so the "
            "ServiceAccount bearer token is not sent in cleartext; got "
            # redact any embedded userinfo: a non-https apiServer carrying
            # credentials (http://tok:secret@host) is echoed into this
            # ConfigError, which the reload loop logs -- never leak the secret.
            "{!r}".format(_redact_userinfo(str(api_server)))
        )
    duration = k8s["leaseDurationSeconds"]
    renew = k8s["renewDeadlineSeconds"]
    retry = k8s["retryPeriodSeconds"]
    if renew <= 0:
        raise ConfigError(
            "cluster.kubernetes.renewDeadlineSeconds must be > 0"
        )
    if duration <= renew:
        # client-go's invariant: a holder must be able to renew well within the
        # window before the lease is considered expired by others.
        raise ConfigError(
            "cluster.kubernetes.leaseDurationSeconds ({}) must be greater "
            "than renewDeadlineSeconds ({})".format(duration, renew)
        )
    if retry <= 0:
        raise ConfigError("cluster.kubernetes.retryPeriodSeconds must be > 0")
    if retry >= renew:
        # client-go's third leaderelection invariant (RenewDeadline must
        # exceed the RetryPeriod). The renew loop sleeps retryPeriodSeconds
        # *between* rounds, unbounded by the lease window: with
        # retry >= renew (and so, with duration > renew already enforced
        # above, retry can also exceed the whole lease duration) a holder
        # cannot complete a renewal before the next attempt is due, so it
        # lapses out of the lease for most of every cycle -- is_leader and
        # is_quorate flap False, no Leader job ever runs stably (a single
        # holder, so no peer leads either) and never-skip PreferLeader jobs
        # double-run on every replica. Reject it rather than silently defeat
        # the at-most-once guarantee a lease backend exists to provide.
        raise ConfigError(
            "cluster.kubernetes.retryPeriodSeconds ({}) must be less than "
            "renewDeadlineSeconds ({}): a holder must be able to renew "
            "within the renew window before the next retry, or it lapses "
            "out of the lease every cycle and no Leader job runs "
            "stably".format(retry, renew)
        )
    if renew + retry >= duration:
        # The renew loop runs one round (bounded by renewDeadlineSeconds) and
        # THEN sleeps retryPeriodSeconds, so the worst-case interval between
        # two successive lease refreshes is renewDeadline + retryPeriod -- but
        # the holder's self-demotion deadline is only leaseDuration ahead of a
        # round's START. The three pairwise invariants above (retry<renew,
        # renew<duration) do NOT bound their SUM, so a config like
        # duration=12/renew=11/retry=10 passes them yet has a ~21s refresh
        # interval against a ~12s deadline: under a slow-but-not-timed-out
        # apiserver the sole healthy holder self-demotes between rounds every
        # cycle (is_leader flaps False), Leader jobs collapse toward
        # at-most-zero and PreferLeader double-runs. Require the sum to fit
        # inside the lease window.
        raise ConfigError(
            "cluster.kubernetes: renewDeadlineSeconds ({}) + "
            "retryPeriodSeconds ({}) must be less than leaseDurationSeconds "
            "({}); otherwise the worst-case gap between lease renewals "
            "exceeds the lease lifetime and the holder lapses out of the "
            "lease every cycle, so no Leader job runs stably".format(
                renew, retry, duration
            )
        )
    # configuring a lease backend is opting into leadership.
    cfg["electLeader"] = True
    advisories = _lease_advisories(raw, "kubernetes")
    if advisories:
        cfg["_advisories"] = advisories
    return ClusterConfig(cfg)


def _url_has_userinfo(url: str) -> bool:
    """Whether ``url``'s authority carries ``user[:pass]@`` userinfo.

    Robust to a scheme-less ``user:pass@host:port`` (which urlparse misreads as
    scheme ``user`` with no username/password), so a credentialed endpoint is
    detected -- and rejected/redacted -- regardless of whether a scheme is
    present.  Userinfo only appears in the authority, before the first ``/``.
    """
    rest = url.partition("://")[2] or url
    authority = rest.partition("/")[0]
    return "@" in authority


def _redact_userinfo(url: str) -> str:
    """Replace ``user:pass@`` userinfo in ``url`` with ``***@`` for logs.

    Deliberately does NOT rely on ``urlparse(...).username``: a scheme-less
    ``user:pass@host`` parses as scheme ``user`` with username/password both
    ``None``, so trusting that would echo the secret verbatim (the leak this
    helper exists to prevent).  Instead the authority is located directly and
    any userinfo it carries is redacted, so the credential never reaches a log
    whether or not a scheme is present.
    """
    scheme, sep, rest = url.partition("://")
    if not sep:
        # scheme-less: urlparse would misread the userinfo as the scheme.
        scheme, rest = "", url
    authority, slash, path = rest.partition("/")
    if "@" not in authority:
        return url
    # Split on the LAST '@': userinfo ends at the final '@', so a password that
    # itself contains '@' (e.g. user:p@ss@host) is not split on its first '@',
    # which would leave a tail of the secret ('ss@host') in the output.
    hostpart = authority.rsplit("@", 1)[1]
    prefix = "{}://".format(scheme) if scheme else ""
    return "{}***@{}{}{}".format(prefix, hostpart, slash, path)


def _build_etcd_cluster_config(raw: dict) -> ClusterConfig:
    cfg = _cluster_base(raw)
    _reject_lease_spread(cfg, "etcd")
    _reject_foreign_store_blocks(cfg, "etcd")
    raw_etcd = cfg.get("etcd") or {}
    etcd = copy.deepcopy(DEFAULT_ETCD)
    etcd.update(raw_etcd)
    # the nested password/tls blocks are merged (a plain update would replace
    # them wholesale, dropping the unset sub-keys' defaults).
    etcd["password"] = {
        **DEFAULT_ETCD["password"],
        **(raw_etcd.get("password") or {}),
    }
    etcd["tls"] = {**DEFAULT_ETCD["tls"], **(raw_etcd.get("tls") or {})}
    cfg["etcd"] = etcd
    # A winning node holds the election key only until (ttl - 1s clock-skew
    # margin) and re-keepalives every max(1s, ttl/3); below 3s that leader
    # window collapses to <= the keepalive period, so a node that wins the
    # campaign immediately considers its own lease expired and NO Leader job
    # ever runs cluster-wide -- a silent at-most-once -> at-most-zero.
    # (kubernetes is protected by its duration>renew invariant; etcd is not,
    # so floor ttl here.)
    if etcd["ttl"] < 3:
        raise ConfigError(
            "cluster.etcd.ttl must be >= 3 seconds (the leader holds the "
            "key only until ttl minus a 1s clock-skew margin and renews "
            "every max(1s, ttl/3); a smaller ttl makes a node that wins "
            "the election immediately treat its own lease as expired, so "
            "no Leader job ever runs); got {}".format(etcd["ttl"])
        )
    if not etcd["endpoints"]:
        raise ConfigError("cluster.etcd.endpoints must list at least one URL")
    for endpoint in etcd["endpoints"]:
        parsed = urlparse(endpoint)
        # Reject credentials embedded in the URL (user:pass@host) FIRST,
        # before the scheme/port check below: they would be logged in cleartext
        # at start() and sent as HTTP Basic auth, bypassing the
        # username/password block's https-only guard. Checking this first
        # matters because an endpoint with BOTH embedded credentials AND a bad
        # scheme/port would otherwise fall into the scheme/port branch and
        # the raw password; here it is always redacted. Use the structured
        # cluster.etcd.username/password fields instead.
        if _url_has_userinfo(endpoint):
            # _url_has_userinfo (not parsed.username) so a scheme-less
            # user:pass@host -- which urlparse misreads as scheme 'user' with
            # no userinfo -- is still caught here and redacted, instead of
            # falling through to the scheme/port branch carrying cleartext.
            raise ConfigError(
                "cluster.etcd.endpoints must not embed credentials in the URL "
                "(userinfo@host); use cluster.etcd.username/password instead, "
                "got {!r}".format(_redact_userinfo(endpoint))
            )
        # urlparse's .port *raises* ValueError on a non-numeric or out-of-range
        # port; guard it so a typo surfaces as a clean ConfigError (config
        # parsing must only ever raise ConfigError) instead of an opaque
        # ValueError that __main__ / the reload loop mistake for a yacron2 bug.
        try:
            port = parsed.port
        except ValueError:
            bad_port = True
        else:
            # a missing port is fine -- it defaults to the scheme's port at
            # connection time (e.g. https://etcd.svc behind 443 ingress); only
            # an explicitly-present out-of-range port is rejected.
            bad_port = port is not None and not 0 < port <= 65535
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or bad_port
        ):
            # redact too (defence in depth): the userinfo check above already
            # rejected any credentialed endpoint; still, never echo a raw URL.
            raise ConfigError(
                "cluster.etcd.endpoints must be http(s)://host[:port], "
                "got {!r}".format(_redact_userinfo(endpoint))
            )
    # mTLS to etcd needs BOTH the client cert and key; one without the other
    # silently degrades to one-way TLS (the cert is never loaded -- see
    # EtcdBackend._build_ssl), which either fails auth opaquely or drops the
    # intended posture. Require them together.
    tls = etcd["tls"]
    if bool(tls.get("cert")) != bool(tls.get("key")):
        raise ConfigError(
            "cluster.etcd.tls.cert and cluster.etcd.tls.key must be set "
            "together (a client certificate needs its private key); got "
            "cert={!r}, key={!r}".format(
                bool(tls.get("cert")), bool(tls.get("key"))
            )
        )
    # TLS material supplied but every endpoint is plaintext -> the material is
    # silently ignored (_build_ssl only builds a context for an https endpoint)
    # and traffic goes in cleartext. That is always a misconfiguration (a
    # forgotten 's' in https://), so refuse it rather than quietly downgrade.
    if any(tls.get(key) for key in ("ca", "cert", "key")) and not any(
        urlparse(endpoint).scheme == "https" for endpoint in etcd["endpoints"]
    ):
        raise ConfigError(
            "cluster.etcd.tls is configured but no endpoint is https:// , so "
            "the TLS material would be ignored and traffic sent in cleartext; "
            "use https:// endpoints or remove cluster.etcd.tls"
        )
    # resolve the password once at load time (fail closed on an empty source),
    # like the web auth token; None when etcd needs no auth.
    etcd["resolved_password"] = _resolve_secret(
        etcd["password"], "cluster.etcd.password"
    )
    if etcd["username"] and not etcd["resolved_password"]:
        # etcd's /v3/auth/authenticate needs a password for the username; a
        # username with no resolvable password would fail auth opaquely every
        # round (a recurring 401, no token, Leader jobs never run) instead of a
        # clean config error.
        raise ConfigError(
            "cluster.etcd.username is set but no password is configured; set "
            "cluster.etcd.password (value/fromFile/fromEnvVar)"
        )
    if etcd["username"] or etcd["resolved_password"]:
        # Authentication credentials (the cleartext username/password POSTed
        # to /v3/auth/authenticate, and the bearer token attached to every
        # request thereafter) must never travel unencrypted. _build_ssl only
        # builds a TLS context when an endpoint is https, and the _post
        # failover loop would otherwise POST those credentials over any
        # plaintext member -- including a single http:// endpoint, or the
        # plaintext one in a mixed http/https list. Refuse the combination at
        # load time so credentials cannot be sniffed; the default loopback
        # endpoint is plaintext but needs no auth, so it is unaffected.
        insecure = [
            endpoint
            for endpoint in etcd["endpoints"]
            if urlparse(endpoint).scheme != "https"
        ]
        if insecure:
            raise ConfigError(
                "cluster.etcd: authentication (username/password) requires "
                "https:// endpoints so credentials are not sent in "
                "cleartext, but these endpoints are plaintext: {}".format(
                    ", ".join(insecure)
                )
            )
    cfg["electLeader"] = True
    advisories = _lease_advisories(raw, "etcd")
    # A small ttl shrinks the renew round's per-request timeout budget
    # (EtcdBackend.request_timeout ~= round_deadline / 5, where round_deadline
    # ~= ttl - max(1, ttl/3) renew - 1s clock skew). Below ~1s that budget can
    # fall under a real cross-AZ/region round-trip to etcd, so every renew POST
    # times out and the node treats a reachable etcd as unreachable (Leader
    # jobs fail closed, and at boot they never recover). It is the operator's
    # explicit ttl choice and a local/low-latency etcd is fine, so warn rather
    # than reject. (Mirrors the backend's cadence constants: 1s skew, ttl/3
    # renew period, 5 POSTs per renew cycle.)
    renew_period = max(1.0, etcd["ttl"] / 3)
    round_deadline = max(1.0, etcd["ttl"] - renew_period - 1.0)
    per_post_budget = round_deadline / 5
    if per_post_budget < 1.0:
        advisories.append(
            "cluster.etcd.ttl={}s leaves only a ~{:.1f}s per-request timeout "
            "for each renew POST to etcd; if a single round-trip is slower "
            "than that (e.g. a cross-AZ/region endpoint) every renew round "
            "will time out and this node will treat a reachable etcd as "
            "unreachable, so Leader jobs fail closed. Raise cluster.etcd.ttl "
            "unless etcd is local and low-latency.".format(
                etcd["ttl"], per_post_budget
            )
        )
    if advisories:
        cfg["_advisories"] = advisories
    return ClusterConfig(cfg)


def cluster_config_warnings(cfg: ClusterConfig) -> List[str]:
    """Non-fatal advisories for a cluster config, returned as messages.

    Returned (not logged) so the caller can emit them *once* — e.g. when the
    cluster manager (re)starts — instead of on every config reload. The daemon
    re-parses its config every wakeup, so logging here would spam the same
    warning every minute for the life of the process.
    """
    # Lease-backend advisories (gossip-only keys / a swallowed
    # electLeader:false) are computed at build time, where the raw block is
    # available, and stashed on the cfg; surface them here so they ride the
    # same emit-once channel.
    warnings: List[str] = list(cfg.get("_advisories", ()))
    if cfg.get("backend", "gossip") != "gossip":
        # The lease backends have no static peer set or even/odd-size
        # trade-off, and always imply electLeader, so the gossip-only
        # advisories below (which read cfg["peers"]) do not apply.
        return warnings
    if cfg.get("electLeader"):
        # `peers` lists every OTHER member, so size is that many plus self.
        size = len(cfg["peers"]) + 1
        # size == 2 is rejected outright in _build_cluster_config; an even
        # size > 2 tolerates the same failures as the odd size below it (e.g. 4
        # tolerates 1, same as 3), so the extra node only adds something that
        # can fail. Allowed, but worth a warning.
        if size > 2 and size % 2 == 0:
            warnings.append(
                "cluster.electLeader: an even cluster size ({} nodes) "
                "tolerates no more failures than {} (the next-lower odd "
                "size); shrink to {} for the same tolerance with one fewer "
                "node, or grow to {} to tolerate one more failure; prefer an "
                "odd size.".format(size, size - 1, size - 1, size + 1)
            )
        # A self-listing by FQDN (vs the short nodeName) is not dropped at
        # config time, so the declared size can be one larger than the real
        # cluster -- which, at the boundary, hides the degenerate 2-node case
        # the size==2 refusal exists to catch (the runtime STATUS_SELF
        # exclusion later drops it, leaving a real quorum of 2). Warn so the
        # operator can fix the listing rather than discover it as flapping
        # leadership.
        listen = cfg.get("listen") or ""
        node_name = cfg["nodeName"]
        self_hosts = [
            peer["host"]
            for peer in cfg["peers"]
            if _likely_self_fqdn(peer["host"], listen, node_name)
        ]
        if self_hosts and size - len(self_hosts) <= 2:
            warnings.append(
                "cluster.electLeader: {} peer(s) look like this node listed "
                "by FQDN ({}) while nodeName is {!r}; if so the real cluster "
                "is only {} node(s) and leader election will be degenerate or "
                "refused at runtime. List the peer by its exact nodeName, or "
                "fix the addresses.".format(
                    len(self_hosts),
                    ", ".join(self_hosts),
                    node_name,
                    size - len(self_hosts),
                )
            )
        # The loopback analogue: a loopback entry can only reach this node
        # (or nothing), so like a self-listing-by-FQDN it hides a smaller --
        # at the boundary, degenerate -- real cluster behind the declared
        # size. Unambiguous forms are dropped at load (_is_self_listed); this
        # advisory covers what remains (localhost, or a family/listen
        # mismatch). A self-listing by a routable IP under a wildcard listen
        # is undetectable without resolving addresses; that case is caught at
        # runtime instead, when the self-poll marks the entry STATUS_SELF
        # (see yacron2.cluster.ClusterManager._log_peer_status_change).
        loopback_hosts = [
            peer["host"]
            for peer in cfg["peers"]
            if _likely_self_loopback(peer["host"], listen)
        ]
        if loopback_hosts and size - len(loopback_hosts) <= 2:
            warnings.append(
                "cluster.electLeader: {} peer(s) are loopback addresses on "
                "this node's own port ({}); a loopback entry never reaches "
                "another host, so at best it is this node itself and the "
                "real cluster is only {} node(s) -- leader election will be "
                "degenerate or refused at runtime. List each peer by an "
                "address the other nodes are reached at.".format(
                    len(loopback_hosts),
                    ", ".join(loopback_hosts),
                    size - len(loopback_hosts),
                )
            )
    elif cfg.get("distribution") != DEFAULT_CLUSTER["distribution"]:
        # distribution only governs how *leader-gated* jobs spread, so it does
        # nothing without electLeader.
        warnings.append(
            "cluster.distribution={!r} has no effect without electLeader; "
            "without leader election every node runs every job.".format(
                cfg.get("distribution")
            )
        )
    return warnings


def _validate_web_config(webconf: WebConfig) -> None:
    """Range checks the schema cannot express, mirroring the cluster
    builders: fail at parse time (so ``--validate-config`` catches it)
    rather than when the first scrape arrives."""
    metrics = webconf.get("metrics")
    if not isinstance(metrics, dict):
        return
    buckets = metrics.get("durationBuckets")
    if buckets is None:
        return
    if not buckets:
        raise ConfigError("web.metrics.durationBuckets must not be empty")
    previous = 0.0
    for bound in buckets:
        # finite, positive, strictly increasing: anything else produces an
        # invalid or duplicate-le histogram exposition.
        if not math.isfinite(bound) or bound <= previous:
            raise ConfigError(
                "web.metrics.durationBuckets must be finite, positive and "
                "strictly increasing (got {!r})".format(buckets)
            )
        previous = bound


@dataclass
class Yacron2Config:
    jobs: List[JobConfig]
    web_config: Optional[WebConfig]
    job_defaults: JobDefaults
    logging_config: Optional[LoggingConfig]
    # Optional; None default so existing constructors (e.g. the empty config in
    # Cron.update_config) need no change.
    cluster_config: Optional[ClusterConfig] = None


def parse_config_string(
    data: str,
    path: str,
    _seen: Optional[set] = None,
    _sources: Optional[set] = None,
) -> Yacron2Config:
    try:
        doc = strictyaml.load(data, CONFIG_SCHEMA, label=path).data
    except YAMLError as ex:
        raise ConfigError(str(ex)) from ex
    return _config_from_doc(doc, path, _seen, _sources)


def parse_crontab_string(data: str, path: str) -> Yacron2Config:
    """Parse classic (Vixie-style) crontab text into a Yacron2Config.

    The crontab is lowered to ordinary job dictionaries
    (:func:`yacron2.crontabs.parse_crontab`) and then built exactly like a
    YAML ``jobs:`` section, so every entry gets yacron2's standard
    defaults -- UTC schedules unless the crontab sets ``CRON_TZ``, stderr
    and exit-status failure detection, and so on -- rather than an
    emulation of cron's environment.  A crontab can only define jobs;
    web / cluster / logging / defaults customization stays YAML-only.
    """
    try:
        job_docs = crontabs.parse_crontab(data, path)
    except crontabs.CrontabError as ex:
        raise ConfigError(str(ex)) from ex
    return _config_from_doc({"jobs": job_docs}, path, None)


def _config_from_doc(
    doc: dict,
    path: str,
    _seen: Optional[set],
    _sources: Optional[set] = None,
) -> Yacron2Config:
    """Build a Yacron2Config from an already-validated plain config doc.

    The shared back half of both front ends: ``parse_config_string``
    arrives here from strictyaml, ``parse_crontab_string`` from the
    classic-crontab lowering, and from this point on the two formats are
    indistinguishable.
    """
    inc_defaults_merged: dict = {}
    jobs = []
    webconf = WebConfig(doc["web"]) if "web" in doc else None
    if webconf is not None:
        # (an included file's web section was already validated when that
        # file was parsed, so validating the inline one here covers all)
        _validate_web_config(webconf)
    clusterconf = (
        _build_cluster_config(doc["cluster"]) if "cluster" in doc else None
    )
    logging_conf = LoggingConfig(doc["logging"]) if "logging" in doc else None
    for include in doc.get("include", ()):
        inc_path = os.path.join(os.path.dirname(path), include)
        # Included jobs arrive already fully constructed, so they carry only
        # their own file's defaults; a top-level ``defaults`` block does NOT
        # retro-apply to them. Only the included files' defaults are merged
        # here, and they affect this file's inline jobs.
        inc_config = parse_config_file(inc_path, _seen, _sources)
        inc_defaults_merged = dict(
            mergedicts(inc_defaults_merged, inc_config.job_defaults)
        )
        jobs.extend(inc_config.jobs)
        if inc_config.web_config:
            if webconf:
                raise ConfigError("multiple web configs")
            webconf = inc_config.web_config
        if inc_config.cluster_config:
            if clusterconf:
                raise ConfigError("multiple cluster configs")
            clusterconf = inc_config.cluster_config
        if inc_config.logging_config:
            if logging_conf:
                raise ConfigError("multiple logging configs")
            logging_conf = inc_config.logging_config
    defaults = dict(mergedicts(DEFAULT_CONFIG, inc_defaults_merged))
    defaults = dict(mergedicts(defaults, doc.get("defaults", {})))
    for config_job in doc.get("jobs", []):
        job_dict = dict(mergedicts(defaults, config_job))
        jobs.append(JobConfig(job_dict))
    return Yacron2Config(
        jobs=jobs,
        web_config=webconf,
        job_defaults=JobDefaults(defaults),
        logging_config=logging_conf,
        cluster_config=clusterconf,
    )


#: Extensions the YAML front end owns.  A file with one of these names is
#: always YAML -- never content-sniffed -- so every config that worked
#: before classic-crontab support keeps its exact behavior and errors.
_YAML_EXTENSIONS = frozenset({".yml", ".yaml"})


def _is_crontab_config(path: str, data: str) -> bool:
    """Decide which front end parses ``path``: classic crontab or YAML.

    The file NAME decides whenever it can: a crontab marker (``.crontab``
    / ``.cron`` extension, or a file named ``crontab``) always means
    crontab, and a YAML extension always means YAML.  Content sniffing is
    a last resort for an explicitly-passed file with a neutral name (e.g.
    ``-c /var/spool/cron/crontabs/root``) and is conservative: only a
    first line no YAML config could open with reads as a crontab
    (see :func:`yacron2.crontabs.looks_like_crontab`).
    """
    if crontabs.is_crontab_path(path):
        return True
    if os.path.splitext(path)[1].lower() in _YAML_EXTENSIONS:
        return False
    return crontabs.looks_like_crontab(data)


def parse_config_file(
    path: str,
    _seen: Optional[set] = None,
    _sources: Optional[set] = None,
) -> Yacron2Config:
    # Guard against include cycles (a file that includes itself directly or
    # transitively) so a misconfiguration raises a clear ConfigError instead
    # of recursing until RecursionError. _seen is scoped per top-level parse,
    # so two independent files including a common file is not flagged.
    abspath = os.path.abspath(path)
    # _sources, when supplied, accumulates every on-disk file the parse reads
    # (this file plus any it includes, transitively) so a caller can stat them
    # and skip an unchanged reparse; it is deliberately NOT _seen (which is
    # per-file cycle scope) so two dir files including a common file are still
    # both recorded without being mistaken for a cycle.
    if _sources is not None:
        _sources.add(abspath)
    if _seen is None:
        _seen = set()
    if abspath in _seen:
        raise ConfigError("include cycle detected at {}".format(path))
    _seen.add(abspath)
    with open(path, "rt", encoding="utf-8") as stream:
        data = stream.read()
    if _is_crontab_config(path, data):
        return parse_crontab_string(data, path)
    return parse_config_string(data, path, _seen, _sources)


def parse_config(
    config_arg: str, _sources: Optional[set] = None
) -> Yacron2Config:
    if os.path.isdir(config_arg):
        return _parse_config_dir(config_arg, _sources)
    try:
        return parse_config_file(config_arg, _sources=_sources)
    except OSError as ex:
        # surface a clean ConfigError (e.g. file not found) rather than a bare
        # OSError, so callers (__main__) handle it uniformly.
        raise ConfigError(str(ex)) from ex


def parse_config_with_sources(
    config_arg: str,
) -> Tuple[Yacron2Config, FrozenSet[str]]:
    """Parse ``config_arg`` and report the on-disk files the parse read.

    Returns ``(config, sources)`` where ``sources`` is the absolute path of
    every YAML/crontab file consulted (the top-level file or directory entries,
    plus anything they ``include`` transitively) and every job's ``env_file``.
    The scheduler stats this exact set to detect that nothing changed on disk
    and skip the (strictyaml-heavy) reparse on an unchanged config; because it
    covers includes and env_files, an edit to any file that actually feeds the
    config is still noticed.  ``env_file`` is abspath'd the same way
    :func:`parse_environment_file` opens it (relative to the process CWD), so
    the recorded path matches what was read.
    """
    sources: set = set()
    config = parse_config(config_arg, sources)
    for job in config.jobs:
        if job.env_file is not None:
            sources.add(os.path.abspath(job.env_file))
    return config, frozenset(sources)


def _parse_config_dir(
    config_arg: str, _sources: Optional[set] = None
) -> Yacron2Config:
    jobs: List[JobConfig] = []
    config_errors: Dict[str, str] = {}
    web_config: Optional[WebConfig] = None
    web_config_source_fname: Optional[str] = None
    cluster_config: Optional[ClusterConfig] = None
    cluster_config_source_fname: Optional[str] = None
    logging_config: Optional[LoggingConfig] = None
    logging_config_source_fname: Optional[str] = None
    job_defaults: JobDefaults = JobDefaults({})
    # Sort by name so job order and the "first config found" error messages
    # are deterministic; os.scandir yields entries in arbitrary FS order.
    for direntry in sorted(os.scandir(config_arg), key=lambda e: e.name):
        base, ext = os.path.splitext(direntry.name)
        if base[0] in {"_", "."}:
            continue
        # YAML by extension, or a classic crontab by filename marker
        # (.crontab / .cron / a file named "crontab"); anything else is
        # skipped, so a stray README or data file never becomes jobs.
        if ext not in {".yml", ".yaml"} and not crontabs.is_crontab_path(
            direntry.name
        ):
            continue
        try:
            config = parse_config_file(direntry.path, _sources=_sources)
        except ConfigError as err:
            config_errors[direntry.path] = str(err)
            continue
        except OSError as ex:
            config_errors[config_arg] = str(ex)
            continue
        jobs.extend(config.jobs)
        if config.web_config is not None:
            if web_config is None:
                web_config = config.web_config
                web_config_source_fname = direntry.path
            else:
                raise ConfigError(
                    "Multiple 'web' configurations found: "
                    "first in {}, now in {}".format(
                        web_config_source_fname, direntry.path
                    )
                )
        if config.cluster_config is not None:
            if cluster_config is None:
                cluster_config = config.cluster_config
                cluster_config_source_fname = direntry.path
            else:
                raise ConfigError(
                    "Multiple 'cluster' configurations found: "
                    "first in {}, now in {}".format(
                        cluster_config_source_fname, direntry.path
                    )
                )
        if config.logging_config is not None:
            if logging_config is None:
                logging_config = config.logging_config
                logging_config_source_fname = direntry.path
            else:
                raise ConfigError(
                    "Multiple 'logging' configurations found: "
                    "first in {}, now in {}".format(
                        logging_config_source_fname, direntry.path
                    )
                )
        job_defaults = JobDefaults(
            dict(mergedicts(job_defaults, config.job_defaults))
        )
    if config_errors:
        raise ConfigError("\n---".join(config_errors.values()))
    # Build the result from the accumulated values (never the last file's
    # config), and return an empty config for an empty/all-skipped directory
    # instead of raising UnboundLocalError.
    return Yacron2Config(
        jobs=jobs,
        web_config=web_config,
        job_defaults=job_defaults,
        logging_config=logging_config,
        cluster_config=cluster_config,
    )
