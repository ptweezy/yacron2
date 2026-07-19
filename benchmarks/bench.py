#!/usr/bin/env python3
"""Performance benchmark suite for cronstable.

The suite measures the paths that determine how cronstable feels on small
machines: process startup, import cost, cron expression parsing and next-fire
search, config parsing, schedule seeding at 100k-job scale, DAG graph
construction and planning, durable-state I/O, JSON, fingerprinting, redaction,
calendar rendering, and memory footprint.

The harness is stdlib-only and imports cronstable from whichever interpreter
runs it, so the same script (from the current checkout) can benchmark an older
installed release for a paired comparison: any benchmark whose API that
version lacks is recorded as skipped, never failed.  Results are written as a
JSON document consumed by benchmarks/compare.py.

Usage:
    python benchmarks/bench.py --json out.json      # full suite (CI)
    python benchmarks/bench.py --quick              # roughly 10x smaller
    python benchmarks/bench.py --smoke              # minimal, for unit tests
    python benchmarks/bench.py --only cronexpr      # substring filter
    python benchmarks/bench.py --list               # list benchmarks

Every timed benchmark returns the wall-clock seconds of a fixed workload
(lower is better); memory benchmarks return MB.  Per-benchmark repeats give
the distribution; compare.py uses each metric's declared estimator ("min" for
time, "median" for memory) so one noisy repeat cannot fake a regression.
"""

import argparse
import atexit
import gc
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from datetime import datetime, timezone

SCHEMA = 1

# Workload scale and repeat column per mode: full is the CI configuration,
# quick is for local iteration, smoke keeps the unit test under a few seconds.
_MODES = {"full": (1.0, 0), "quick": (0.1, 1), "smoke": (0.01, 2)}
_MODE = "full"


def _scale() -> float:
    return _MODES[_MODE][0]


def _n(base: int, floor: int = 1) -> int:
    return max(floor, int(base * _scale()))


def _reps(spec) -> int:
    return spec[_MODES[_MODE][1]]


class Skip(Exception):
    """Raised by a benchmark that cannot run in this environment."""


_BENCHMARKS = []
_FIX = {}
_SESSION_TMP = None
_SRC_FALLBACK = None


def _ensure_importable():
    """Prefer the installed cronstable; fall back to the source checkout.

    In CI each side runs from its own venv, where cronstable is installed and
    this is a no-op.  Running the script straight from a checkout without an
    install would otherwise skip every in-process benchmark (a script's
    sys.path[0] is benchmarks/, not the repo root).
    """
    global _SRC_FALLBACK
    try:
        import cronstable  # noqa: F401

        return
    except ImportError:
        pass
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if os.path.isdir(os.path.join(root, "cronstable")):
        sys.path.insert(0, root)
        _SRC_FALLBACK = root
        print(
            "note: cronstable is not installed in this interpreter; "
            "benchmarking the source tree at %s" % root,
            file=sys.stderr,
        )


def _tmpdir() -> str:
    global _SESSION_TMP
    if _SESSION_TMP is None:
        _SESSION_TMP = tempfile.mkdtemp(prefix="cronstable-bench-")
        atexit.register(shutil.rmtree, _SESSION_TMP, ignore_errors=True)
    return _SESSION_TMP


def fixture(name, builder):
    """Build-once shared setup, excluded from every timed region."""
    if name not in _FIX:
        _FIX[name] = builder()
    return _FIX[name]


def bench(
    name,
    group,
    detail="",
    unit="s",
    gate_pct=25.0,
    gate_floor=0.010,
    compare="min",
    repeats=(5, 2, 1),
    info=False,
):
    """Register a benchmark.  The function returns one measured value."""

    def deco(fn):
        _BENCHMARKS.append(
            {
                "name": name,
                "group": group,
                "detail": detail,
                "unit": unit,
                "gate_pct": None if info else gate_pct,
                "gate_floor": gate_floor,
                "compare": compare,
                "repeats": repeats,
                "info": info,
                "fn": fn,
            }
        )
        return fn

    return deco


# ---------------------------------------------------------------------------
# Shared workload generators (deterministic; no randomness, no clock reads
# inside timed regions beyond the measured work itself).
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 15, 12, 30, 45, tzinfo=timezone.utc)
_NAIVE = datetime(2026, 7, 18, 12, 30)

_SIMPLE_EXPRS = [
    "* * * * *",
    "*/5 * * * *",
    "0 * * * *",
    "15 3 * * *",
    "0 9 * * 1-5",
    "30 6 1 * *",
    "0 0 * * 0",
    "45 23 * * 6",
]

_COMPLEX_EXPRS = [
    "*/7 8-18 * * 1-5",
    "0,15,30,45 */2 1,15 * *",
    "5 4 L * *",
    "0 12 15W * *",
    "0 8 * * 1#2",
    "0 22 * * L5",
    "30 2 * 1,4,7,10 *",
    "0 0 1 1 * 2030",
    "*/30 * * * * * *",
    "H H(2-5) * * *",
    "H/15 * * * *",
]


# Step values that divide the minute field's span evenly, so generated
# schedules are lint-clean (a lint finding per job would flood the log and
# add unrepresentative logging cost to config benchmarks).
_EVEN_STEPS = (2, 3, 4, 5, 6, 10, 12, 15, 20, 30)


def _varied_exprs(n):
    """A deterministic mix of realistic 5-field schedules (no H, no L/W,
    valid for classic crontab lowering too)."""
    out = []
    for i in range(n):
        r = i % 10
        if r < 4:
            out.append("%d %d * * *" % (i % 60, (i * 7) % 24))
        elif r < 6:
            out.append("*/%d * * * *" % _EVEN_STEPS[i % len(_EVEN_STEPS)])
        elif r < 8:
            out.append("%d 8-18 * * 1-5" % (i % 60))
        else:
            out.append("%d %d 1,15 * *" % (i % 60, (i * 3) % 24))
    return out


def _crontab_cls():
    try:
        from cronstable.cronexpr import CronTab
    except ImportError as exc:  # pragma: no cover
        raise Skip("cronstable.cronexpr unavailable: %r" % exc) from None
    return CronTab


def _parse_tabs(exprs):
    CronTab = _crontab_cls()
    return [CronTab(e, hash_key="job-%d" % i) for i, e in enumerate(exprs)]


def _config_yaml(n_jobs):
    lines = ["jobs:"]
    for i, expr in enumerate(_varied_exprs(n_jobs)):
        lines.append("  - name: job%05d" % i)
        lines.append("    command: echo job%05d" % i)
        lines.append('    schedule: "%s"' % expr)
        if i % 3 == 0:
            lines.append("    captureStdout: true")
    lines.append("")
    return "\n".join(lines)


def _config_path(n_jobs):
    path = os.path.join(_tmpdir(), "bench-config-%d.yaml" % n_jobs)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write(_config_yaml(n_jobs))
    return path


def _job_dicts(n):
    return [
        {"name": "job%05d" % i, "command": "true", "schedule": expr}
        for i, expr in enumerate(_varied_exprs(n))
    ]


def _job_configs(n):
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    return [
        JobConfig(mergedicts(DEFAULT_CONFIG, raw)) for raw in _job_dicts(n)
    ]


def _schedule_entries(n):
    try:
        from cronstable.croninfo import ScheduleEntry
    except ImportError as exc:
        raise Skip("croninfo.ScheduleEntry unavailable: %r" % exc) from None
    CronTab = _crontab_cls()
    entries = []
    for i in range(n):
        if i % 2 == 0:
            expr = "%d * * * *" % (i % 60)  # hourly
        else:
            expr = "%d %d * * *" % (i % 60, (i * 7) % 24)  # daily
        entries.append(ScheduleEntry("job%05d" % i, CronTab(expr), None))
    return entries


# ---------------------------------------------------------------------------
# startup: cold process starts, timed as real subprocess wall clock.
# ---------------------------------------------------------------------------


def _child_env():
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = "0"
    if _SRC_FALLBACK:
        prior = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            _SRC_FALLBACK + os.pathsep + prior if prior else _SRC_FALLBACK
        )
    return env


def _timed_child(args):
    t0 = time.perf_counter()
    # cwd is a neutral temp dir so the child resolves cronstable from its
    # interpreter's site-packages, never from a checkout it happens to sit
    # in.  In the paired CI run the old side's children must import the old
    # release, not the repo working tree.
    proc = subprocess.run(
        [sys.executable] + args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=_child_env(),
        cwd=_tmpdir(),
    )
    dt = time.perf_counter() - t0
    if proc.returncode != 0:
        raise Skip("child exited %d: %s" % (proc.returncode, " ".join(args)))
    return dt


@bench(
    "startup.python_baseline",
    "startup",
    detail="python -c pass",
    repeats=(40, 5, 1),
    info=True,
)
def bench_python_baseline():
    return _timed_child(["-c", "pass"])


@bench(
    "startup.version",
    "startup",
    detail="cronstable --version",
    repeats=(40, 5, 2),
)
def bench_startup_version():
    return _timed_child(["-m", "cronstable", "--version"])


@bench(
    "startup.import_cronexpr",
    "startup",
    detail="import cronstable.cronexpr",
    repeats=(12, 3, 1),
)
def bench_import_cronexpr():
    return _timed_child(["-c", "import cronstable.cronexpr"])


@bench(
    "startup.import_config",
    "startup",
    detail="import cronstable.config",
    repeats=(12, 3, 1),
)
def bench_import_config():
    return _timed_child(["-c", "import cronstable.config"])


@bench(
    "startup.import_daemon",
    "startup",
    detail="import cronstable.cron (full daemon graph)",
    repeats=(12, 3, 1),
)
def bench_import_daemon():
    return _timed_child(["-c", "import cronstable.cron"])


@bench(
    "startup.validate_config_100",
    "startup",
    detail="cronstable --validate-config, 100 jobs",
    repeats=(8, 2, 1),
)
def bench_validate_config():
    path = _config_path(_n(100))
    return _timed_child(["-m", "cronstable", "-c", path, "--validate-config"])


@bench(
    "startup.job_set_id_100",
    "startup",
    detail="cronstable --job-set-id, 100 jobs",
    repeats=(8, 2, 1),
)
def bench_job_set_id_cli():
    path = _config_path(_n(100))
    return _timed_child(["-m", "cronstable", "-c", path, "--job-set-id"])


# ---------------------------------------------------------------------------
# cronexpr: the scheduling engine itself.
# ---------------------------------------------------------------------------


@bench(
    "cronexpr.parse_simple",
    "cronexpr",
    detail="parse 20k plain 5-field expressions",
)
def bench_parse_simple():
    CronTab = _crontab_cls()
    n = _n(20000)
    exprs = [_SIMPLE_EXPRS[i % len(_SIMPLE_EXPRS)] for i in range(n)]
    t0 = time.perf_counter()
    for e in exprs:
        CronTab(e)
    return time.perf_counter() - t0


@bench(
    "cronexpr.parse_complex",
    "cronexpr",
    detail="parse 5k expressions with ranges/steps/L/W/#/H/seconds",
)
def bench_parse_complex():
    CronTab = _crontab_cls()
    n = _n(5000)
    exprs = [_COMPLEX_EXPRS[i % len(_COMPLEX_EXPRS)] for i in range(n)]
    t0 = time.perf_counter()
    for i, e in enumerate(exprs):
        CronTab(e, hash_key="job-%d" % i)
    return time.perf_counter() - t0


@bench(
    "cronexpr.next_simple",
    "cronexpr",
    detail="next() over 20k pre-parsed plain tabs",
)
def bench_next_simple():
    tabs = fixture(
        "tabs_simple_20k",
        lambda: _parse_tabs(
            [_SIMPLE_EXPRS[i % len(_SIMPLE_EXPRS)] for i in range(_n(20000))]
        ),
    )
    t0 = time.perf_counter()
    for tab in tabs:
        tab.next(_NOW)
    return time.perf_counter() - t0


@bench(
    "cronexpr.next_complex",
    "cronexpr",
    detail="next() over 5k pre-parsed complex tabs",
)
def bench_next_complex():
    tabs = fixture(
        "tabs_complex_5k",
        lambda: _parse_tabs(
            [_COMPLEX_EXPRS[i % len(_COMPLEX_EXPRS)] for i in range(_n(5000))]
        ),
    )
    t0 = time.perf_counter()
    for tab in tabs:
        tab.next(_NOW)
    return time.perf_counter() - t0


@bench(
    "cronexpr.occurrences_1k",
    "cronexpr",
    detail="enumerate 1k fires from 8 generators",
)
def bench_occurrences():
    from itertools import islice

    tabs = fixture("tabs_occ", lambda: _parse_tabs(_SIMPLE_EXPRS))
    count = _n(1000)
    start = _NOW
    t0 = time.perf_counter()
    for tab in tabs:
        for _ in islice(tab.occurrences(start), count):
            pass
    return time.perf_counter() - t0


@bench(
    "cronexpr.test_match",
    "cronexpr",
    detail="test() one instant against 20k tabs",
)
def bench_test_match():
    tabs = fixture(
        "tabs_simple_20k",
        lambda: _parse_tabs(
            [_SIMPLE_EXPRS[i % len(_SIMPLE_EXPRS)] for i in range(_n(20000))]
        ),
    )
    t0 = time.perf_counter()
    for tab in tabs:
        tab.test(_NAIVE)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# config: YAML and classic-crontab parsing, JobConfig construction.
# ---------------------------------------------------------------------------


@bench(
    "config.parse_yaml_300",
    "config",
    detail="parse_config_string, 300-job YAML",
    repeats=(3, 2, 1),
)
def bench_parse_yaml():
    try:
        from cronstable.config import parse_config_string
    except ImportError as exc:
        raise Skip("parse_config_string unavailable: %r" % exc) from None
    text = fixture("yaml_300", lambda: _config_yaml(_n(300)))
    t0 = time.perf_counter()
    parse_config_string(text, "")
    return time.perf_counter() - t0


@bench(
    "config.jobconfig_3k",
    "config",
    detail="JobConfig over merged defaults, 3k jobs",
    repeats=(3, 2, 1),
)
def bench_jobconfig():
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    raws = fixture("job_dicts_3k", lambda: _job_dicts(_n(3000)))
    t0 = time.perf_counter()
    for raw in raws:
        JobConfig(mergedicts(DEFAULT_CONFIG, raw))
    return time.perf_counter() - t0


@bench(
    "config.parse_crontab_1k",
    "config",
    detail="parse_crontab_string, 1k classic lines",
    repeats=(3, 2, 1),
)
def bench_parse_crontab():
    try:
        from cronstable.config import parse_crontab_string
    except ImportError as exc:
        raise Skip("parse_crontab_string unavailable: %r" % exc) from None
    n = _n(1000)
    text = fixture(
        "crontab_1k",
        lambda: (
            "\n".join(
                "%s echo line-%d" % (expr, i)
                for i, expr in enumerate(_varied_exprs(n))
            )
            + "\n"
        ),
    )
    t0 = time.perf_counter()
    parse_crontab_string(text, "bench-crontab")
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# schedule: seeding and analyzing the fleet schedule, 100k jobs.
# ---------------------------------------------------------------------------


@bench(
    "schedule.cold_build_100k",
    "schedule",
    detail="parse + next() + heapify, 100k jobs from cold",
    repeats=(3, 2, 1),
)
def bench_schedule_cold():
    import heapq

    CronTab = _crontab_cls()
    exprs = fixture("exprs_100k", lambda: _varied_exprs(_n(100000)))
    t0 = time.perf_counter()
    heap = []
    for i, e in enumerate(exprs):
        tab = CronTab(e)
        delay = tab.next(_NOW)
        if delay is not None:
            heap.append((delay, i))
    heapq.heapify(heap)
    return time.perf_counter() - t0


@bench(
    "schedule.reseed_100k",
    "schedule",
    detail="next() + heapify over 100k pre-parsed jobs",
    repeats=(3, 2, 1),
)
def bench_schedule_reseed():
    import heapq

    tabs = fixture(
        "tabs_100k",
        lambda: _parse_tabs(
            fixture("exprs_100k", lambda: _varied_exprs(_n(100000)))
        ),
    )
    t0 = time.perf_counter()
    heap = []
    for i, tab in enumerate(tabs):
        delay = tab.next(_NOW)
        if delay is not None:
            heap.append((delay, i))
    heapq.heapify(heap)
    return time.perf_counter() - t0


@bench(
    "schedule.pressure_5k_24h",
    "schedule",
    detail="schedule_pressure, 5k entries over 24h",
    repeats=(3, 2, 1),
)
def bench_schedule_pressure():
    try:
        from cronstable.croninfo import schedule_pressure
    except ImportError as exc:
        raise Skip("schedule_pressure unavailable: %r" % exc) from None
    entries = fixture("entries_5k", lambda: _schedule_entries(_n(5000)))
    t0 = time.perf_counter()
    schedule_pressure(entries, start=_NOW, hours=24)
    return time.perf_counter() - t0


@bench(
    "schedule.next_fires_2k",
    "schedule",
    detail="next_fires(count=5) for 2k schedules",
    repeats=(3, 2, 1),
)
def bench_next_fires():
    try:
        from cronstable.croninfo import next_fires
    except ImportError as exc:
        raise Skip("next_fires unavailable: %r" % exc) from None
    exprs = fixture("exprs_next_fires", lambda: _varied_exprs(_n(2000)))
    t0 = time.perf_counter()
    for e in exprs:
        next_fires(e, 5, start=_NOW)
    return time.perf_counter() - t0


@bench(
    "schedule.duplicates_5k",
    "schedule",
    detail="duplicate_schedules over 5k entries",
    repeats=(3, 2, 1),
)
def bench_duplicates():
    try:
        from cronstable.croninfo import duplicate_schedules
    except ImportError as exc:
        raise Skip("duplicate_schedules unavailable: %r" % exc) from None
    entries = fixture("entries_5k", lambda: _schedule_entries(_n(5000)))
    t0 = time.perf_counter()
    duplicate_schedules(entries)
    return time.perf_counter() - t0


@bench(
    "schedule.suggest_slot_5k",
    "schedule",
    detail="suggest_slot against 5k entries",
    repeats=(3, 2, 1),
)
def bench_suggest_slot():
    try:
        from cronstable.croninfo import suggest_slot
    except ImportError as exc:
        raise Skip("suggest_slot unavailable: %r" % exc) from None
    entries = fixture("entries_5k", lambda: _schedule_entries(_n(5000)))
    t0 = time.perf_counter()
    suggest_slot(entries, period="hourly", start=_NOW)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# dag: graph construction, validation, and the planning transform.
# ---------------------------------------------------------------------------


def _dag_module():
    try:
        from cronstable import dag
    except ImportError as exc:
        raise Skip("cronstable.dag unavailable: %r" % exc) from None
    for attr in ("TaskSpec", "DagSpec", "validate_graph"):
        if not hasattr(dag, attr):
            raise Skip("cronstable.dag lacks %s" % attr)
    return dag


@bench(
    "dag.build_chain_10k",
    "dag",
    detail="build + validate a 10k-task linear chain",
    repeats=(3, 2, 1),
)
def bench_dag_chain():
    dag = _dag_module()
    n = _n(10000)
    t0 = time.perf_counter()
    tasks = [dag.TaskSpec(id="t0")]
    for i in range(1, n):
        tasks.append(dag.TaskSpec(id="t%d" % i, depends_on=("t%d" % (i - 1),)))
    spec = dag.DagSpec.build("chain", tasks)
    dag.validate_graph(spec)
    return time.perf_counter() - t0


@bench(
    "dag.build_layered_10k",
    "dag",
    detail="build + validate 100 layers x 100 tasks, 3 deps each",
    repeats=(3, 2, 1),
)
def bench_dag_layered():
    dag = _dag_module()
    layers = max(2, int(100 * _scale() ** 0.5))
    width = max(2, int(100 * _scale() ** 0.5))
    t0 = time.perf_counter()
    tasks = []
    for layer in range(layers):
        for w in range(width):
            if layer == 0:
                deps = ()
            else:
                deps = tuple(
                    "L%dW%d" % (layer - 1, (w + k) % width) for k in range(3)
                )
            tasks.append(
                dag.TaskSpec(id="L%dW%d" % (layer, w), depends_on=deps)
            )
    spec = dag.DagSpec.build("layered", tasks)
    dag.validate_graph(spec)
    return time.perf_counter() - t0


@bench(
    "dag.plan_claim_2k",
    "dag",
    detail="plan_and_claim over a fresh 2k-task run",
    repeats=(3, 2, 1),
)
def bench_dag_plan():
    dag = _dag_module()
    if not hasattr(dag, "new_run_body") or not hasattr(dag, "plan_and_claim"):
        raise Skip("dag planning API not present")
    n = _n(2000)
    tasks = [dag.TaskSpec(id="t%d" % i) for i in range(n)]
    spec = dag.DagSpec.build("wide", tasks)
    try:
        body = dag.new_run_body(
            dag="wide",
            run_key="bench",
            run_id="bench-run",
            logical_date=None,
            kind="scheduled",
            now=1700000000.0,
            spec=spec,
        )
        transform = dag.plan_and_claim(
            spec, 1700000000.0, "bench-proc", "bench-host", {}
        )
    except TypeError as exc:
        raise Skip("dag planning signature changed: %r" % exc) from None
    t0 = time.perf_counter()
    transform(body)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# state: the durable filesystem backend (async, real disk I/O).
# ---------------------------------------------------------------------------


def _state_backend(path):
    try:
        from cronstable.state import FilesystemStateBackend
    except ImportError as exc:
        raise Skip("cronstable.state unavailable: %r" % exc) from None
    config = {"path": path, "topology": "single-node", "deploymentId": None}
    try:
        return FilesystemStateBackend(config, lambda: "bench-jobset")
    except Exception as exc:
        raise Skip("state backend construction failed: %r" % exc) from None


def _state_dir_with_records():
    """A store pre-seeded with records, built once (untimed)."""

    def build():
        import asyncio

        path = os.path.join(_tmpdir(), "state-seeded")
        os.makedirs(path, exist_ok=True)
        n = _n(2000)

        async def seed():
            backend = _state_backend(path)
            await backend.start()
            for i in range(n):
                await backend.append_record(
                    "runs", {"outcome": "success", "seq": i, "duration": 1.25}
                )
            await backend.stop()

        asyncio.run(seed())
        return path, n

    return fixture("state_seeded", build)


@bench(
    "state.append_1k",
    "state",
    detail="append_record x1k to a fresh store",
    repeats=(3, 2, 1),
    gate_floor=0.050,
)
def bench_state_append():
    import asyncio

    n = _n(1000)
    path = tempfile.mkdtemp(prefix="append-", dir=_tmpdir())

    async def run():
        backend = _state_backend(path)
        await backend.start()
        t0 = time.perf_counter()
        for i in range(n):
            await backend.append_record(
                "runs", {"outcome": "success", "seq": i, "duration": 1.25}
            )
        dt = time.perf_counter() - t0
        await backend.stop()
        return dt

    try:
        return asyncio.run(run())
    finally:
        shutil.rmtree(path, ignore_errors=True)


@bench(
    "state.derive_max_cold",
    "state",
    detail="first derive_max over 2k records (no memo)",
    repeats=(3, 2, 1),
)
def bench_derive_max_cold():
    import asyncio

    path, _ = _state_dir_with_records()

    async def run():
        backend = _state_backend(path)
        await backend.start()
        t0 = time.perf_counter()
        await backend.derive_max("runs", "seq")
        dt = time.perf_counter() - t0
        await backend.stop()
        return dt

    return asyncio.run(run())


@bench(
    "state.derive_max_warm",
    "state",
    detail="200 memoized derive_max calls",
    repeats=(3, 2, 1),
    gate_floor=0.005,
)
def bench_derive_max_warm():
    import asyncio

    path, _ = _state_dir_with_records()
    n = _n(200)

    async def run():
        backend = _state_backend(path)
        await backend.start()
        await backend.derive_max("runs", "seq")  # warm the memo
        t0 = time.perf_counter()
        for _ in range(n):
            await backend.derive_max("runs", "seq")
        dt = time.perf_counter() - t0
        await backend.stop()
        return dt

    return asyncio.run(run())


@bench(
    "state.list_records_2k",
    "state",
    detail="list_records over 2k records",
    repeats=(3, 2, 1),
)
def bench_list_records():
    import asyncio

    path, _ = _state_dir_with_records()

    async def run():
        backend = _state_backend(path)
        await backend.start()
        t0 = time.perf_counter()
        await backend.list_records("runs")
        dt = time.perf_counter() - t0
        await backend.stop()
        return dt

    return asyncio.run(run())


@bench(
    "state.kv_roundtrip_200",
    "state",
    detail="jobstate kv_set + kv_get x200",
    repeats=(3, 2, 1),
)
def bench_kv_roundtrip():
    import asyncio

    try:
        from cronstable import jobstate
    except ImportError as exc:
        raise Skip("cronstable.jobstate unavailable: %r" % exc) from None
    kv_set = getattr(jobstate, "kv_set", None)
    kv_get = getattr(jobstate, "kv_get", None)
    if kv_set is None or kv_get is None:
        raise Skip("jobstate kv API not present")
    n = _n(200)
    path = tempfile.mkdtemp(prefix="kv-", dir=_tmpdir())

    async def run():
        backend = _state_backend(path)
        await backend.start()
        try:
            t0 = time.perf_counter()
            for i in range(n):
                await kv_set(backend, "bench", "key-%d" % (i % 20), {"v": i})
                await kv_get(backend, "bench", "key-%d" % (i % 20))
            dt = time.perf_counter() - t0
        except TypeError as exc:
            raise Skip("jobstate kv signature changed: %r" % exc) from None
        await backend.stop()
        return dt

    try:
        return asyncio.run(run())
    finally:
        shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# json / fingerprint / redact / ical
# ---------------------------------------------------------------------------


def _sample_doc():
    return {
        "schemaVersion": "v1",
        "run": {
            "dag": "nightly-etl",
            "runId": "r-000123",
            "state": "running",
            "startedAt": 1700000000.0,
            "tasks": {
                "t%d" % i: {
                    "state": "success",
                    "attempt": 1,
                    "exitCode": 0,
                    "host": "node-%d" % (i % 4),
                    "startedAt": 1700000000.0 + i,
                    "finishedAt": 1700000042.0 + i,
                }
                for i in range(50)
            },
        },
    }


@bench(
    "json.roundtrip_3k",
    "json",
    detail="dumps_bytes + loads of a run document x3k",
)
def bench_json_roundtrip():
    try:
        from cronstable._json import dumps_bytes, loads
    except ImportError as exc:
        raise Skip("cronstable._json unavailable: %r" % exc) from None
    doc = _sample_doc()
    n = _n(3000)
    t0 = time.perf_counter()
    for _ in range(n):
        loads(dumps_bytes(doc))
    return time.perf_counter() - t0


@bench(
    "fingerprint.job_set_id_10k",
    "fingerprint",
    detail="job_set_id over 10k JobConfigs",
    repeats=(3, 2, 1),
)
def bench_fingerprint():
    try:
        from cronstable.fingerprint import job_set_id
    except ImportError as exc:
        raise Skip("cronstable.fingerprint unavailable: %r" % exc) from None
    jobs = fixture("jobconfigs_10k", lambda: _job_configs(_n(10000)))
    t0 = time.perf_counter()
    job_set_id(jobs)
    return time.perf_counter() - t0


@bench(
    "redact.clean_20k",
    "redact",
    detail="redact_lines over 20k secret-free log lines",
)
def bench_redact_clean():
    try:
        from cronstable.redact import redact_lines
    except ImportError as exc:
        raise Skip("cronstable.redact unavailable: %r" % exc) from None
    n = _n(20000)
    lines = fixture(
        "clean_lines",
        lambda: [
            "2026-07-18 12:00:%02d INFO worker %d: processed batch in 12ms"
            % (i % 60, i)
            for i in range(n)
        ],
    )
    t0 = time.perf_counter()
    redact_lines(lines)
    return time.perf_counter() - t0


@bench(
    "redact.secrets_5k",
    "redact",
    detail="redact_lines over 5k secret-bearing lines",
)
def bench_redact_secrets():
    try:
        from cronstable.redact import redact_lines
    except ImportError as exc:
        raise Skip("cronstable.redact unavailable: %r" % exc) from None
    n = _n(5000)

    def build():
        pem = [
            "-----BEGIN RSA PRIVATE KEY-----",
            "MIIEowIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF0qJps5MTvEV0G4RFY0PGpfx0000",
            "-----END RSA PRIVATE KEY-----",
        ]
        out = []
        for i in range(n):
            r = i % 5
            if r == 0:
                out.append(
                    "export AWS_SECRET_ACCESS_KEY="
                    "wJalrXUtnFEMIbPxRfiCYEXAMPLEKEY%03d" % i
                )
            elif r == 1:
                out.append("PASSWORD=hunter%d" % i)
            elif r == 2:
                out.append(
                    "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.pay%04d.sig"
                    % i
                )
            elif r == 3:
                out.extend(pem)
            else:
                out.append("plain line %d with nothing sensitive" % i)
        return out

    lines = fixture("secret_lines", build)
    t0 = time.perf_counter()
    redact_lines(lines)
    return time.perf_counter() - t0


@bench(
    "ical.render_500x7d",
    "ical",
    detail="render_calendar, 500 entries over 7 days",
    repeats=(3, 2, 1),
)
def bench_ical():
    try:
        from cronstable.ical import CalendarEntry, render_calendar
    except ImportError as exc:
        raise Skip("cronstable.ical unavailable: %r" % exc) from None
    CronTab = _crontab_cls()
    n = _n(500)

    def build():
        entries = []
        for i in range(n):
            if i % 2 == 0:
                expr = "%d * * * *" % (i % 60)
            else:
                expr = "%d %d * * *" % (i % 60, (i * 7) % 24)
            entries.append(
                CalendarEntry("job%05d" % i, CronTab(expr), timezone.utc)
            )
        return entries

    entries = fixture("ical_entries", build)
    start = datetime(2026, 1, 5, tzinfo=timezone.utc)
    t0 = time.perf_counter()
    render_calendar(entries, start=start, days=7, per_job_cap=50)
    return time.perf_counter() - t0


# ---------------------------------------------------------------------------
# memory: deterministic traced allocations plus real child-process RSS.
# ---------------------------------------------------------------------------


@bench(
    "mem.crontab_10k",
    "memory",
    detail="traced MB held by 10k parsed CronTabs",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.5,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_crontab():
    CronTab = _crontab_cls()
    exprs = _varied_exprs(_n(10000))
    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        tabs = [CronTab(e) for e in exprs]
        after, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    del tabs
    return (after - before) / 1048576.0


@bench(
    "mem.jobconfig_2k",
    "memory",
    detail="traced MB held by 2k JobConfigs",
    unit="MB",
    gate_pct=15.0,
    gate_floor=0.5,
    compare="median",
    repeats=(3, 2, 1),
)
def bench_mem_jobconfig():
    raws = _job_dicts(_n(2000))
    try:
        from cronstable.config import DEFAULT_CONFIG, JobConfig, mergedicts
    except ImportError as exc:
        raise Skip("cronstable.config API unavailable: %r" % exc) from None
    gc.collect()
    tracemalloc.start()
    try:
        before, _ = tracemalloc.get_traced_memory()
        jobs = [JobConfig(mergedicts(DEFAULT_CONFIG, raw)) for raw in raws]
        after, _ = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    del jobs
    return (after - before) / 1048576.0


_RSS_WRAPPER = (
    "import resource,subprocess,sys\n"
    "r=subprocess.run(sys.argv[1:],stdout=subprocess.DEVNULL,"
    "stderr=subprocess.DEVNULL)\n"
    "print(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)\n"
    "sys.exit(r.returncode)\n"
)


def _child_peak_rss_mb(args):
    """Peak RSS in MB of one child process, POSIX only.

    A wrapper child runs the target and reports getrusage(RUSAGE_CHILDREN),
    which is scoped to the wrapper's own children, so earlier benchmark
    subprocesses cannot pollute the reading.
    """
    if sys.platform == "win32":
        raise Skip("peak-RSS benchmark requires POSIX getrusage")
    proc = subprocess.run(
        [sys.executable, "-c", _RSS_WRAPPER, sys.executable] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        env=_child_env(),
        cwd=_tmpdir(),
    )
    if proc.returncode != 0:
        raise Skip("child exited %d: %s" % (proc.returncode, " ".join(args)))
    raw = int(proc.stdout.split()[0])
    # ru_maxrss is bytes on macOS, KiB on Linux and the BSDs.
    return raw / 1048576.0 if sys.platform == "darwin" else raw / 1024.0


@bench(
    "mem.rss_version",
    "memory",
    detail="peak RSS of cronstable --version",
    unit="MB",
    gate_pct=25.0,
    gate_floor=3.0,
    compare="median",
    repeats=(5, 2, 1),
)
def bench_rss_version():
    return _child_peak_rss_mb(["-m", "cronstable", "--version"])


@bench(
    "mem.rss_daemon_import",
    "memory",
    detail="peak RSS of importing the full daemon graph",
    unit="MB",
    gate_pct=25.0,
    gate_floor=3.0,
    compare="median",
    repeats=(5, 2, 1),
)
def bench_rss_daemon():
    return _child_peak_rss_mb(["-c", "import cronstable.cron"])


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


def _cronstable_meta():
    meta = {"version": None, "orjson": False}
    try:
        from cronstable.version import version as ver

        meta["version"] = str(ver)
    except Exception:
        try:
            from importlib.metadata import version as md_version

            meta["version"] = md_version("cronstable")
        except Exception:
            pass
    try:
        import orjson  # noqa: F401

        meta["orjson"] = True
    except ImportError:
        pass
    return meta


def _run_one(spec):
    reps = _reps(spec["repeats"])
    values = []
    error = None
    for _ in range(reps):
        gc.collect()
        gc_was_enabled = gc.isenabled()
        gc.disable()
        try:
            values.append(float(spec["fn"]()))
        except Skip as exc:
            error = str(exc)
            break
        except Exception as exc:  # a broken benchmark must not kill the run
            error = "error: %r" % exc
            break
        finally:
            if gc_was_enabled:
                gc.enable()
    result = {
        "name": spec["name"],
        "group": spec["group"],
        "detail": spec["detail"],
        "unit": spec["unit"],
        "gate_pct": spec["gate_pct"],
        "gate_floor": spec["gate_floor"],
        "compare": spec["compare"],
        "info": spec["info"],
    }
    if not values:
        result.update({"skipped": True, "reason": error or "no data"})
        return result
    value = (
        min(values) if spec["compare"] == "min" else statistics.median(values)
    )
    result.update(
        {
            "skipped": False,
            "reason": None,
            "runs": len(values),
            "values": values,
            "value": value,
            "mean": statistics.fmean(values),
            "median": statistics.median(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
            "min": min(values),
            "max": max(values),
        }
    )
    return result


def _fmt(value, unit):
    if unit == "MB":
        return "%.2f MB" % value
    if value < 0.001:
        return "%.1f us" % (value * 1e6)
    if value < 1.0:
        return "%.2f ms" % (value * 1e3)
    return "%.3f s" % value


def main(argv=None):
    global _MODE
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", help="write results to this JSON file")
    parser.add_argument(
        "--quick", action="store_true", help="roughly 10x smaller workloads"
    )
    parser.add_argument(
        "--smoke", action="store_true", help="minimal workloads, for tests"
    )
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        help="run benchmarks whose name or group contains this substring",
    )
    parser.add_argument(
        "--list", action="store_true", help="list benchmarks and exit"
    )
    args = parser.parse_args(argv)
    _MODE = "smoke" if args.smoke else "quick" if args.quick else "full"
    _ensure_importable()

    if args.list:
        for spec in _BENCHMARKS:
            print(
                "%-28s %-12s %s"
                % (spec["name"], spec["group"], spec["detail"])
            )
        return 0

    selected = [
        spec
        for spec in _BENCHMARKS
        if not args.only
        or any(s in spec["name"] or s in spec["group"] for s in args.only)
    ]
    if not selected:
        print("no benchmark matches %r" % (args.only,), file=sys.stderr)
        return 2

    meta = _cronstable_meta()
    started = time.perf_counter()
    results = []
    for spec in selected:
        result = _run_one(spec)
        results.append(result)
        if result["skipped"]:
            line = "SKIP (%s)" % result["reason"]
        else:
            line = _fmt(result["value"], result["unit"])
        print("%-28s %s" % (result["name"], line), flush=True)

    import platform as _platform

    doc = {
        "schema": SCHEMA,
        "mode": _MODE,
        "cronstable_version": meta["version"],
        "orjson": meta["orjson"],
        "python": _platform.python_version(),
        "implementation": _platform.python_implementation(),
        "platform": sys.platform,
        "machine": _platform.machine(),
        "cpu_count": os.cpu_count(),
        "suite_seconds": round(time.perf_counter() - started, 3),
        "results": results,
    }
    if args.json:
        out_dir = os.path.dirname(os.path.abspath(args.json))
        os.makedirs(out_dir, exist_ok=True)
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1, sort_keys=True)
            f.write("\n")
        print("wrote %s" % args.json)
    ran = sum(1 for r in results if not r["skipped"])
    print(
        "%d benchmarks, %d skipped, %.1fs total (%s mode, cronstable %s)"
        % (
            ran,
            len(results) - ran,
            doc["suite_seconds"],
            _MODE,
            meta["version"],
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
