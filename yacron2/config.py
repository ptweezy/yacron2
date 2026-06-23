import copy
import datetime
import logging
import os
import sys
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    NewType,
    Optional,
    Union,  # noqa
)
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

from yacron2 import platform

logger = logging.getLogger("yacron2.config")
WebConfig = NewType("WebConfig", Dict[str, Any])
JobDefaults = NewType("JobDefaults", Dict[str, Any])
LoggingConfig = NewType("LoggingConfig", Dict[str, Any])


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
}


DEFAULT_CONFIG = {
    "shell": platform.DEFAULT_SHELL,
    "concurrencyPolicy": "Allow",
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
    }
)

_job_defaults_common = {
    Opt("shell"): Str(),
    Opt("concurrencyPolicy"): Enum(["Allow", "Forbid", "Replace"]),
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
    Opt("user"): Str() | Int(),
    Opt("group"): Str() | Int(),
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


class JobConfig:
    def __init__(self, config: dict) -> None:
        self.name = config["name"]  # type: str
        self.command = config["command"]  # type: Union[str, List[str]]
        self.schedule_unparsed = config.pop("schedule")
        self.schedule: Union[CronTab, str] = self._parse_schedule(
            self.schedule_unparsed
        )
        self.shell = config.pop("shell")
        self.concurrencyPolicy = config.pop("concurrencyPolicy")
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
            return CronTab(schedule_unparsed)
        if isinstance(schedule_unparsed, dict):
            minute = schedule_unparsed.get("minute", "*")
            hour = schedule_unparsed.get("hour", "*")
            day = schedule_unparsed.get("dayOfMonth", "*")
            month = schedule_unparsed.get("month", "*")
            dow = schedule_unparsed.get("dayOfWeek", "*")
            tab = f"{minute} {hour} {day} {month} {dow}"
            logger.debug("Converted schedule to %r", tab)
            return CronTab(tab)
        raise ConfigError("invalid schedule: {!r}".format(schedule_unparsed))

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
        # the wrong account.  Spelt as ``sys.platform == "win32"`` (rather than
        # platform.IS_WINDOWS) so the type checker statically prunes the
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
        # that would otherwise produce obscure runtime behaviour instead of a
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


@dataclass
class Yacron2Config:
    jobs: List[JobConfig]
    web_config: Optional[WebConfig]
    job_defaults: JobDefaults
    logging_config: Optional[LoggingConfig]


def parse_config_string(
    data: str, path: str, _seen: Optional[set] = None
) -> Yacron2Config:
    try:
        doc = strictyaml.load(data, CONFIG_SCHEMA, label=path).data
    except YAMLError as ex:
        raise ConfigError(str(ex)) from ex

    inc_defaults_merged: dict = {}
    jobs = []
    webconf = WebConfig(doc["web"]) if "web" in doc else None
    logging_conf = LoggingConfig(doc["logging"]) if "logging" in doc else None
    for include in doc.get("include", ()):
        inc_path = os.path.join(os.path.dirname(path), include)
        # Included jobs arrive already fully constructed, so they carry only
        # their own file's defaults; a top-level ``defaults`` block does NOT
        # retro-apply to them. Only the included files' defaults are merged
        # here, and they affect this file's inline jobs.
        inc_config = parse_config_file(inc_path, _seen)
        inc_defaults_merged = dict(
            mergedicts(inc_defaults_merged, inc_config.job_defaults)
        )
        jobs.extend(inc_config.jobs)
        if inc_config.web_config:
            if webconf:
                raise ConfigError("multiple web configs")
            webconf = inc_config.web_config
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
    )


def parse_config_file(path: str, _seen: Optional[set] = None) -> Yacron2Config:
    # Guard against include cycles (a file that includes itself directly or
    # transitively) so a misconfiguration raises a clear ConfigError instead
    # of recursing until RecursionError. _seen is scoped per top-level parse,
    # so two independent files including a common file is not flagged.
    abspath = os.path.abspath(path)
    if _seen is None:
        _seen = set()
    if abspath in _seen:
        raise ConfigError("include cycle detected at {}".format(path))
    _seen.add(abspath)
    with open(path, "rt", encoding="utf-8") as stream:
        data = stream.read()
    return parse_config_string(data, path, _seen)


def parse_config(config_arg: str) -> Yacron2Config:
    if os.path.isdir(config_arg):
        return _parse_config_dir(config_arg)
    try:
        return parse_config_file(config_arg)
    except OSError as ex:
        # surface a clean ConfigError (e.g. file not found) rather than a bare
        # OSError, so callers (__main__) handle it uniformly.
        raise ConfigError(str(ex)) from ex


def _parse_config_dir(config_arg: str) -> Yacron2Config:
    jobs: List[JobConfig] = []
    config_errors: Dict[str, str] = {}
    web_config: Optional[WebConfig] = None
    web_config_source_fname: Optional[str] = None
    logging_config: Optional[LoggingConfig] = None
    logging_config_source_fname: Optional[str] = None
    job_defaults: JobDefaults = JobDefaults({})
    # Sort by name so job order and the "first config found" error messages
    # are deterministic; os.scandir yields entries in arbitrary FS order.
    for direntry in sorted(os.scandir(config_arg), key=lambda e: e.name):
        base, ext = os.path.splitext(direntry.name)
        if base[0] in {"_", "."}:
            continue
        if ext not in {".yml", ".yaml"}:
            continue
        try:
            config = parse_config_file(direntry.path)
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
    )
