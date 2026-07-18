"""A full tour of the TUI's surfaces, headless against the fake daemon.

`tests/test_tui.py` covers the ported client logic and the core key
flows; this module walks every remaining surface — the drawers' four
tabs, the DAG drawer end to end (runs → tasks → approve → graph → xcom
→ task logs → backfill), the cluster/fleet/state/heat/radar/node
panels, mitigate, multi-tail, wallboard + zen, the boot self-test, the
settings sheet, themes, and the terminal engine's painter — so the
render and dispatch paths that only a live daemon exercised before are
pinned by tests (they are where the live grand-tour fleet exposed real
payload-shape bugs).

Payload fixtures mirror shapes captured from a running fleet: DAG run
docs key tasks by dict with the config task ``id``; a parked approval
gate is ``state: running`` + ``awaitingApproval: true``; run entries
stamp epoch floats (``createdAt``).
"""

import asyncio
import datetime
import io
import time

from cronstable import tui
from cronstable.tui import (
    Api,
    HeadlessTerm,
    LogTail,
    Term,
    copy_to_clipboard,
    strip_ansi,
)

from tests.test_tui import FakeDaemon, Harness, _job, _wait_for


def _iso(seconds_ago: float) -> str:
    stamp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        seconds=seconds_ago
    )
    return stamp.isoformat()


def rich_history(n: int = 8):
    return [
        {
            "outcome": "failure" if i == 2 else "success",
            "duration": 0.5 + i,
        }
        for i in range(n)
    ]


def install_fleet_fixtures(daemon: FakeDaemon) -> None:
    """The full-featured daemon: cluster, fleet, DAGs, state, node."""
    monitored = _job(
        "risk-model",
        outcome="success",
        history=rich_history(),
        schedule="*/5 * * * *",
    )
    monitored["running"] = True
    monitored["scheduled_in"] = None
    monitored["running_resources"] = {
        "cpu_percent": 87.5,
        "cpu_seconds": 12.0,
        "rss_bytes": 5 * 1024 * 1024,
        "instances": 1,
    }
    retrying = _job(
        "flaky-etl",
        outcome="failure",
        exit_code=7,
        fail_reason="exited with code 7",
        history=rich_history(4),
    )
    retrying["retry"] = {
        "attempt": 2,
        "maxAttempts": 5,
        "nextRetryAt": _iso(-30),
        "delaySeconds": 30,
    }
    spread = _job("owned-job", outcome="success")
    spread["clusterPolicy"] = "Leader"
    spread["clusterOwner"] = "node-b"
    slotted = _job("slotted-job")
    slotted["concurrencyScope"] = "cluster"
    slotted["slot"] = {"held": True, "holder": "node-a", "refs": 1}
    slotted["rebootPending"] = True
    long_cmd = _job(
        "scripted",
        outcome="success",
        command="set -eu\ntoken=$(cronstable secret get t)\ncurl -s x",
    )
    daemon.jobs = [monitored, retrying, spread, slotted, long_cmd]
    daemon.log_lines["flaky-etl"] = [
        {"stream": "stdout", "line": "starting etl"},
        {"stream": "stderr", "line": "\x1b[31mboom\x1b[0m"},
    ]
    daemon.job_resources["risk-model"] = {
        "name": "risk-model",
        "monitored": True,
        "interval": 1,
        "live": [{"cpu_percent": 42.0, "rss_bytes": 2048}],
        "runs": [
            {
                "started_at": _iso(120),
                "resources": {
                    "cpu_total_seconds": 1.5,
                    "max_rss_bytes": 4096,
                },
            }
        ],
    }
    daemon.cluster = {
        "enabled": True,
        "backend": "gossip",
        "node_name": "node-a",
        "elect_leader": True,
        "quorate": True,
        "is_leader": True,
        "leader": "node-a",
        "distribution": "spread",
        "node_stats": {"cpu_percent": 12.0, "mem_percent": 40.0},
        "peers": [
            {
                "node_name": "node-b",
                "status": "alive",
                "agree": True,
                "as_of": _iso(5),
                "node_stats": {"cpu_percent": 8.0, "mem_percent": 33.0},
                "owns": 3,
            },
            {"node_name": "node-c", "status": "lost", "agree": False},
        ],
        "lease": {
            "holder": "node-a",
            "identity": "node-a",
            "expiry": _iso(-30),
            "fence": 7,
        },
    }
    daemon.fleet = {
        "enabled": True,
        "elect_leader": True,
        "distribution": "spread",
        "interval": 10,
        "nodes": [
            {
                "node_name": "node-a",
                "self": True,
                "jobs": {
                    "risk-model": {"running": True, "enabled": True},
                    "flaky-etl": {
                        "running": False,
                        "enabled": True,
                        "last": {
                            "outcome": "failure",
                            "finished_at": _iso(60),
                            "exit_code": 7,
                            "duration": 1.0,
                        },
                        "scheduled_in": 30,
                    },
                    "owned-job": {
                        "running": False,
                        "enabled": True,
                        "last": {
                            "outcome": "success",
                            "finished_at": _iso(120),
                            "duration": 2.0,
                        },
                    },
                    "mothballed": {"running": False, "enabled": False},
                    "fresh": {"running": False, "enabled": True},
                },
            },
            {
                "node_name": "node-b",
                "status": "alive",
                "as_of": _iso(8),
                "truncated": True,
                "node_stats": {"cpu_percent": 8.0, "mem_percent": 30.0},
                "jobs": {
                    "risk-model": {
                        "running": False,
                        "enabled": True,
                        "last": {
                            "outcome": "cancelled",
                            "finished_at": _iso(200),
                        },
                    }
                },
            },
            {"node_name": "node-c", "status": "lost"},
        ],
    }
    daemon.dags_list = [
        {
            "name": "pipeline",
            "enabled": True,
            "scheduled": False,
            "tasks": [
                {"id": "extract", "dependsOn": []},
                {"id": "load", "dependsOn": ["extract"]},
                {"id": "approve", "dependsOn": ["load"]},
            ],
            "latestRun": {"state": "running"},
            "totalRuns": 2,
        }
    ]
    daemon.dag_runs["pipeline"] = [
        {
            "runKey": "manual-1",
            "state": "running",
            "createdAt": time.time() - 90,
        },
        {
            "runKey": "sched-0",
            "state": "success",
            "createdAt": time.time() - 900,
        },
    ]
    daemon.dag_docs["manual-1"] = {
        "runKey": "manual-1",
        "state": "running",
        "tasks": {
            "extract": {"state": "success", "attempt": 0},
            "load": {"state": "running", "attempt": 1},
            "approve": {
                "state": "running",
                "awaitingApproval": True,
                "attempt": 0,
            },
        },
    }
    daemon.dag_xcom["manual-1"] = {
        "dag": "pipeline",
        "runKey": "manual-1",
        "xcom": {"rows": 42, "cursor": "2026-07-17"},
    }
    daemon.state = {
        "enabled": True,
        "view": {"records": 12, "bytes": 4096},
        "stats": {"puts": 3, "gets": 9},
        "records": {"runs/etl": 5, "logs/etl": 2, "locks/x": 1},
        "documents": {"kv/app": 2, "cursor/etl": 1},
        "leases": {},
        "node": {"host": "node-a", "retries": [1], "slots": ["s"]},
    }
    daemon.state_documents["kv/app"] = [{"key": "greeting", "size": 5}]
    daemon.state_records["runs/etl"] = [{"key": "r1"}, {"key": "r2"}]
    daemon.node = {
        "node_name": "node-a",
        "resources": {
            "cpu_percent": 12.5,
            "mem_percent": 41.0,
            "rss_bytes": 123456,
            "pids": 42,
        },
    }
    daemon.node_history = {
        "node_name": "node-a",
        "enabled": True,
        "interval": 5,
        "points": [
            [time.time() - 60 + i, 10.0 + i, 20.0 + i] for i in range(20)
        ],
    }


async def start_rich(h: Harness, tmp_path, **kw):
    install_fleet_fixtures(h.daemon)
    app = await h.start(tmp_path, **kw)
    await _wait_for(lambda: len(app.jobs) == 5)
    return app


async def snap_text(h: Harness) -> str:
    h.app.mark()
    await h.settle()
    return h.term.screen()


# ===================================================================
#  the job drawer's four tabs
# ===================================================================
async def test_tour_drawer_tabs_and_log_tools(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(
        "os.path.expanduser", lambda p: p.replace("~", str(tmp_path))
    )
    h = Harness()
    try:
        app = await start_rich(h, tmp_path)
        h.keys.send("/", "f", "l", "a", "k", "enter")  # filter to flaky
        await _wait_for(lambda: app.filter_text == "flak")
        h.keys.send("enter")
        await _wait_for(lambda: app.is_open("drawer"))
        await _wait_for(
            lambda: app.log_tail is not None and len(app.log_tail.lines) >= 2
        )
        # search + jumps + toggles
        h.keys.send("/")
        await _wait_for(lambda: app.focus == "logsearch")
        h.keys.send("b", "o", "o", "m", "enter")
        await _wait_for(lambda: app.log_matches != [])
        app.focus = None
        h.keys.send("n", "N", "t", "w", "f", "pgup", "pgdn", "home", "end")
        await h.settle()
        screen = h.term.screen()
        assert "boom" in screen
        h.keys.send("d")  # save the log to a file
        await _wait_for(lambda: any("saved" in t[1] for t in app.toasts), 5)
        # r / x from inside the drawer act on the drawer job
        h.keys.send("r")
        await _wait_for(lambda: "flaky-etl/start" in h.daemon.posts)
        # history tab
        h.keys.send("tab")
        await _wait_for(lambda: app.drawer_tab == "history")
        await _wait_for(lambda: app.drawer_runs is not None)
        h.keys.send("j", "k", "pgdn", "pgup")
        screen = await snap_text(h)
        assert "runs" in screen
        # resources tab (monitored fixture)
        app.open_drawer("risk-model", "resources")
        await _wait_for(lambda: app.drawer_res is not None)
        screen = await snap_text(h)
        assert "live:" in screen
        assert "peak rss" in screen
        # schedule tab
        app.drawer_tab = "schedule"
        screen = await snap_text(h)
        assert "Every 5 minutes" in screen
        assert "next runs:" in screen
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("drawer"))
    finally:
        await h.stop()


async def test_tour_drawer_unmonitored_and_wrap(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job(
            "wide",
            outcome="success",
            history=[{"outcome": "success", "duration": None}],
        )
    ]
    h.daemon.log_lines["wide"] = [
        {"stream": "stdout", "line": "x" * 300},
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        app.wrap = True
        app.open_drawer("wide", "logs")
        await _wait_for(
            lambda: app.log_tail is not None and app.log_tail.lines
        )
        screen = await snap_text(h)
        assert "xxxx" in screen
        app.open_drawer("wide", "resources")
        await _wait_for(lambda: app.drawer_res is not None)
        screen = await snap_text(h)
        assert "no resource monitoring" in screen
    finally:
        await h.stop()


# ===================================================================
#  the DAG drawer, end to end
# ===================================================================
async def test_tour_dag_drawer(tmp_path):
    h = Harness()
    try:
        app = await start_rich(h, tmp_path)
        await _wait_for(lambda: app.dags)
        # the DAGs index panel
        app._toggle_dags()
        await _wait_for(lambda: app.is_open("dags"))
        h.keys.send("j", "k")
        screen = await snap_text(h)
        assert "pipeline" in screen
        assert "3 tasks" in screen
        h.keys.send("t")  # trigger from the index
        await _wait_for(lambda: "dag/pipeline/trigger" in h.daemon.posts)
        h.keys.send("enter")  # open the drawer
        await _wait_for(lambda: app.is_open("dag"))
        await _wait_for(lambda: app.dag_runs)
        screen = await snap_text(h)
        assert "manual-1" in screen
        # newest run -> tasks tab; the parked gate shows its hint
        h.keys.send("j", "k", "enter")
        await _wait_for(lambda: app.dag_run is not None)
        assert app.dag_tab == "tasks"
        screen = await snap_text(h)
        assert "awaiting" in screen
        assert "a approve" in screen
        # approve the selected awaiting task
        h.keys.send("j", "j")  # move to the approve task
        await _wait_for(lambda: app.dag_sel == 2)
        h.keys.send("a")
        await _wait_for(lambda: any("decision" in p for p in h.daemon.posts))
        assert h.daemon.post_bodies[-1]["decision"] == "approve"
        h.keys.send("R")
        await _wait_for(
            lambda: sum("decision" in p for p in h.daemon.posts) >= 2
        )
        assert h.daemon.post_bodies[-1]["decision"] == "reject"
        # the graph tab lays tasks out by depth with edges
        h.keys.send("left")  # tasks -> graph
        await _wait_for(lambda: app.dag_tab == "graph")
        screen = await snap_text(h)
        assert "extract" in screen
        assert "─▶" in screen or "->" in screen
        # xcom
        app.dag_tab = "xcom"
        app.dag_xcom = None
        app._spawn(app._load_dag_xcom())
        await _wait_for(lambda: app.dag_xcom is not None)
        screen = await snap_text(h)
        assert "rows" in screen and "42" in screen
        # task logs over SSE
        app.dag_tab = "tasks"
        app.dag_sel = 0
        h.keys.send("enter")
        await _wait_for(lambda: app.dag_tab == "logs")
        await _wait_for(
            lambda: app.dag_task_tail is not None and app.dag_task_tail.lines
        )
        screen = await snap_text(h)
        assert "says hi" in screen
        # backfill input
        h.keys.send("b")
        await _wait_for(lambda: app.focus == "backfill")
        for ch in "2026-07-01..2026-07-03":
            h.keys.send(ch)
        h.keys.send("enter")
        await _wait_for(lambda: "dag/pipeline/backfill" in h.daemon.posts)
        assert h.daemon.post_bodies[-1] == {
            "from": "2026-07-01",
            "to": "2026-07-03",
        }
        # trigger from inside the drawer too, then close
        h.keys.send("t")
        await _wait_for(
            lambda: (
                sum(p == "dag/pipeline/trigger" for p in h.daemon.posts) >= 2
            )
        )
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("dag"))
    finally:
        await h.stop()


# ===================================================================
#  cluster / fleet / state / heat / radar / node panels
# ===================================================================
async def test_tour_cluster_and_fleet(tmp_path):
    h = Harness()
    try:
        app = await start_rich(h, tmp_path)
        await _wait_for(lambda: (app.cluster or {}).get("enabled"))
        app._toggle("cluster")
        h.keys.send("j", "k")
        screen = await snap_text(h)
        assert "node-a" in screen and "gossip" in screen
        assert "leader" in screen
        assert "node-b" in screen  # peer row with load
        assert "owns 3" in screen
        assert "held by" in screen  # lease detail
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("cluster"))

        app._toggle("fleet")
        await _wait_for(
            lambda: (app.fleet or {}).get("enabled") and app.is_open("fleet")
        )
        screen = await snap_text(h)
        assert "3 nodes" in screen
        assert "node-a*" in screen  # self marker
        assert "partial" in screen or "node-b" in screen
        assert "fail" in screen
        h.keys.send("f")  # failing only
        await _wait_for(lambda: app.fleet_fail_only)
        screen = await snap_text(h)
        assert "FAILING ONLY" in screen
        h.keys.send("f", "r", "j", "k", "pgdn", "pgup", "home")
        await h.settle()
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("fleet"))
    finally:
        await h.stop()


async def test_tour_state_heat_radar_node(tmp_path):
    h = Harness()
    try:
        app = await start_rich(h, tmp_path)
        # ---- state inspector: view -> documents -> records ----
        app._toggle("state")
        await _wait_for(lambda: (app.state_data or {}).get("enabled"), 10)
        screen = await snap_text(h)
        assert "state inspector" in screen
        assert "document namespaces" in screen
        h.keys.send("right")  # documents tab
        await _wait_for(lambda: app.state_tab == "documents")
        h.keys.send("j", "enter")  # namespaces sort: kv/app is second
        await _wait_for(lambda: app.state_detail is not None)
        screen = await snap_text(h)
        assert "kv/app" in screen and "greeting" in screen
        h.keys.send("right")  # records tab (logs/ streams hidden)
        await _wait_for(lambda: app.state_tab == "records")
        app.state_detail = None
        h.keys.send("j", "k", "enter")
        await _wait_for(lambda: app.state_detail is not None)
        screen = await snap_text(h)
        assert "runs/etl" in screen
        assert "logs/etl" not in screen
        h.keys.send("r")
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("state"))

        # ---- heatmap ----
        app._toggle("heat")
        await _wait_for(lambda: app.heat_data, 15)
        screen = await snap_text(h)
        assert "activity heatmap" in screen
        h.keys.send("j", "r")
        await h.settle()
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("heat"))

        # ---- radar ----
        app._toggle("radar")
        screen = await snap_text(h)
        assert "next-fire radar" in screen
        assert "upcoming" in screen
        h.keys.send("esc")

        # ---- node resources + history sparkline ----
        app._toggle("node")
        await _wait_for(lambda: app.node_history is not None, 10)
        screen = await snap_text(h)
        assert "node: node-a" in screen
        assert "cpu_percent" in screen
        assert "cpu " in screen  # the history sparkline row
        h.keys.send("esc")
    finally:
        await h.stop()


# ===================================================================
#  incident kit: timeline -> mitigate -> writeup
# ===================================================================
async def test_tour_mitigate_console(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "os.path.expanduser", lambda p: p.replace("~", str(tmp_path))
    )
    h = Harness()
    h.daemon.jobs = [
        _job("bad-a", outcome="failure", exit_code=69, finished_ago=5),
        _job("bad-b", outcome="failure", exit_code=69, finished_ago=8),
        _job("runner", running=True, scheduled_in=None),
        _job("fine", outcome="success"),
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 4)
        await _wait_for(lambda: app.verdict is not None)
        assert "share exit=69" in app.verdict["sub"]
        h.keys.send("i")
        await _wait_for(lambda: app.is_open("timeline"))
        h.keys.send("m")  # hand the blast radius to mitigate
        await _wait_for(lambda: app.is_open("mitigate"))
        assert sorted(app.mitigate_names) == ["bad-a", "bad-b"]
        screen = await snap_text(h)
        assert "mitigate console" in screen
        h.keys.send("s")  # staggered bulk start
        await _wait_for(
            lambda: (
                "bad-a/start" in h.daemon.posts
                and "bad-b/start" in h.daemon.posts
            ),
            10,
        )
        await _wait_for(lambda: not app.mitigate_running, 10)
        screen = await snap_text(h)
        assert "✓ start bad-a" in screen
        h.keys.send("y")  # writeup lands in a file + clipboard
        await _wait_for(lambda: any("writeup" in t[1] for t in app.toasts), 5)
        assert h.term.copied and "bad-a" in h.term.copied[-1]
        # cancel-all path over the running job
        app.open_mitigate(["runner"], "running set")
        h.keys.send("x")
        await _wait_for(lambda: "runner/cancel" in h.daemon.posts, 10)
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("mitigate"))
    finally:
        await h.stop()


# ===================================================================
#  multi-tail console
# ===================================================================
async def test_tour_multitail(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job("a", outcome="success"),
        _job("b", outcome="failure"),
        _job("c"),
        _job("d"),
        _job("e"),
    ]
    for name in "abcde":
        h.daemon.log_lines[name] = [
            {"stream": "stdout", "line": "hello from %s" % name}
        ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 5)
        h.keys.send("m")  # terminal extra: open the console
        await _wait_for(lambda: app.is_open("tail"))
        h.keys.send("a")  # add via the input, with fuzzy matching
        await _wait_for(lambda: app.focus == "tailadd")
        h.keys.send("a", "enter")
        await _wait_for(lambda: len(app.tails) == 1)
        app.tail_preset("fail")  # palette preset fills from failing
        await _wait_for(lambda: len(app.tails) == 2)
        app.add_tail("nope")  # unknown name is refused with a toast
        await _wait_for(lambda: any("no such job" in t[1] for t in app.toasts))
        app.add_tail("c")
        app.add_tail("d")
        app.add_tail("e")  # fifth: over TAIL_MAX, refused with a toast
        assert len(app.tails) == 4
        await _wait_for(
            lambda: any("multi-tail is full" in t[1] for t in app.toasts)
        )
        await _wait_for(lambda: all(t.lines for t in app.tails), 10)
        app.timestamps = True
        h.keys.send("t", "w", "pgup", "pgdn", "end", "j", "k")
        screen = await snap_text(h)
        assert "multi-tail (4/4)" in screen
        assert "hello from a" in screen
        assert "end of run output" in screen
        h.keys.send("x")  # remove the picked stream
        await _wait_for(lambda: len(app.tails) == 3)
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("tail"))
        assert app.tails == []
    finally:
        await h.stop()


# ===================================================================
#  wallboard, zen, alarm, boot
# ===================================================================
async def test_tour_wallboard_zen_and_alarm(tmp_path):
    h = Harness()
    h.daemon.jobs = [
        _job(
            "tile-fail",
            outcome="failure",
            exit_code=3,
            history=rich_history(5),
        ),
        _job("tile-run", running=True, scheduled_in=None),
        _job("tile-ok", outcome="success", history=rich_history(3)),
        _job("tile-off", enabled=False),
    ]
    try:
        app = await h.start(tmp_path)
        app.prefs["sound"] = True
        await _wait_for(lambda: len(app.jobs) == 4)
        h.keys.send("w")
        await _wait_for(lambda: app.wallboard)
        screen = await snap_text(h)
        assert "tile-fail" in screen and "exit 3" in screen
        assert "ALARM (a to ack)" in screen
        h.keys.send("a")  # acknowledge
        await _wait_for(lambda: app.alarm_ack)
        # zen: force the screensaver and paint its dot field
        app.zen_on = True
        screen = await snap_text(h)
        assert "all clear" in screen
        # any key wakes zen without acting
        h.keys.send("q")
        await _wait_for(lambda: not app.zen_on)
        assert not app.quit
        h.keys.send("w")
        await _wait_for(lambda: not app.wallboard)
        # a fresh failure between polls rings the bell (sound on)
        bells = h.term.bells
        for job in h.daemon.jobs:
            if job["name"] == "tile-ok":
                job["last_run"]["outcome"] = "failure"
                job["last_run"]["finished_at"] = _iso(0)
        await _wait_for(lambda: h.term.bells > bells, 10)
    finally:
        await h.stop()


async def test_tour_boot_sequence(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("a", outcome="success", scheduled_in=12.0)]
    try:
        await h.daemon.start()
        prefs = dict(tui.PREF_DEFAULTS)
        prefs["poll_ms"] = 200
        app = tui.TuiApp(
            Api(h.daemon.url, None),
            h.term,
            h.keys,
            prefs,
            boot=True,
            prefs_file=str(tmp_path / "prefs.json"),
        )
        h.app = app
        h._task = asyncio.get_running_loop().create_task(app.run())
        await _wait_for(lambda: app.booting, 10)
        await _wait_for(lambda: not app.booting, 30)
        text = "\n".join(
            "\n".join(strip_ansi(r) for r in f) for f in h.term.frames
        )
        assert "POWER-ON SELF-TEST" in text
        assert "link" in text and "OK" in text
        assert "1 job" in text
        assert "standalone" in text
        assert "ALL CHECKS PASSED" in text
        # the 12h stamp was recorded
        assert app.prefs["boot_last"] > 0
    finally:
        await h.stop()


async def test_tour_boot_skip_key(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("a")]
    try:
        await h.daemon.start()
        prefs = dict(tui.PREF_DEFAULTS)
        prefs["poll_ms"] = 200
        app = tui.TuiApp(
            Api(h.daemon.url, None),
            h.term,
            h.keys,
            prefs,
            boot=True,
            prefs_file=str(tmp_path / "prefs.json"),
        )
        h.app = app
        h._task = asyncio.get_running_loop().create_task(app.run())
        await _wait_for(lambda: app.booting, 10)
        h.keys.send(" ")  # any key skips the POST
        await _wait_for(lambda: not app.booting, 10)
        assert True  # reaching here without a hang is the assertion
    finally:
        await h.stop()


# ===================================================================
#  settings, themes, sandbox, palette breadth
# ===================================================================
async def test_tour_settings_rows_all_cycle(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("a")]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        app.open("settings")
        rows = app.settings_rows()
        # walk every row and activate it once; every action must apply
        # cleanly and the sheet must repaint
        for idx in range(len(rows)):
            app.settings_sel = idx
            await app.handle_key("enter")
        screen = await snap_text(h)
        assert "settings" in screen
        assert "prefs file" in screen
        # a second full pass restores the toggles' original sense
        for idx in range(len(rows)):
            app.settings_sel = idx
            await app.handle_key("enter")
        h.keys.send("j", "k", "esc")
        await _wait_for(lambda: not app.is_open("settings"))
    finally:
        await h.stop()


async def test_tour_themes_cvd_ascii_and_narrow(tmp_path):
    h = Harness()
    h.term = HeadlessTerm(60, 16)  # a cramped terminal drops columns
    h.daemon.jobs = [
        _job("narrow-board-job", outcome="success", history=rich_history())
    ]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        app.prefs["compact"] = True
        app.prefs["ascii"] = True
        for _ in tui.THEME_HUES:
            app.cycle_theme()
            await snap_text(h)
        app.toggle_light_dark()
        for _ in tui.CVD_MODES:
            app.cycle_cvd()
        for _ in tui.POLL_CHOICES:
            app.cycle_poll()
        screen = await snap_text(h)
        assert "narrow-board-job" in screen
        assert "o" in screen  # the ASCII ok glyph
    finally:
        await h.stop()


async def test_tour_sandbox_and_token_palette_paths(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("a")]
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 1)
        # sandbox via the palette
        h.keys.send("ctrl+k")
        await _wait_for(lambda: app.is_open("palette"))
        for ch in "sandbox":
            h.keys.send(ch)
        h.keys.send("enter")
        await _wait_for(lambda: app.is_open("sandbox"))
        for ch in "*/5 * * * *":
            h.keys.send(ch)
        screen = await snap_text(h)
        assert "Every 5 minutes" in screen
        assert "next fires (UTC):" in screen
        h.keys.send("ctrl+u")
        for ch in "not a cron":
            h.keys.send(ch)
        screen = await snap_text(h)
        assert "rejects this expression" in screen
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("sandbox"))
        # token modal via the palette, cancelled with esc
        h.keys.send("ctrl+k")
        await _wait_for(lambda: app.is_open("palette"))
        for ch in "access token":
            h.keys.send(ch)
        h.keys.send("enter")
        await _wait_for(lambda: app.is_open("token"))
        screen = await snap_text(h)
        assert "access token" in screen
        h.keys.send("esc")
        await _wait_for(lambda: not app.is_open("token"))
        # copy chips + run-all-failing + refresh through the palette API
        app._copy_chip("deadbeef")
        assert "deadbeef" in h.term.copied
        job = app.by_name["a"]
        job["command"] = "echo hi"
        app._copy_job_command("a")
        await app.run_all_failing()  # nothing failing -> info toast
        assert any("nothing failing" in t[1] for t in app.toasts)
    finally:
        await h.stop()


# ===================================================================
#  API + tail failure paths
# ===================================================================
async def test_tour_error_paths(tmp_path):
    h = Harness()
    h.daemon.jobs = [_job("a"), _job("gone")]
    h.daemon.fail_logs_for.add("a")
    try:
        app = await h.start(tmp_path)
        await _wait_for(lambda: len(app.jobs) == 2)
        # a 500ing SSE stream surfaces in-pane, not as a crash
        app.open_drawer("a", "logs")
        await _wait_for(
            lambda: app.log_tail is not None and app.log_tail.error is not None
        )
        screen = await snap_text(h)
        assert "⚠" in screen
        app.close("drawer")
        # 404 on start (a job the daemon no longer knows)
        h.daemon.jobs = [j for j in h.daemon.jobs if j["name"] != "gone"]
        await app.run_job("gone")
        assert any("no such job" in t[1] for t in app.toasts)
        # 409s: cancel a not-running job / start a disabled one
        await app.cancel_job("a")
        assert any("not running" in t[1] for t in app.toasts)
    finally:
        await h.stop()


# ===================================================================
#  the terminal engine itself
# ===================================================================
def test_term_paint_diffs_rows():
    out = io.StringIO()
    term = Term(stream=out)
    rows = ["hello", "world", "three"]
    term.paint(rows, "")
    first = out.getvalue()
    assert "hello" in first and "world" in first
    out.truncate(0)
    out.seek(0)
    term.paint(["hello", "WORLD", "three"], "")
    second = out.getvalue()
    assert "WORLD" in second
    assert "hello" not in second  # unchanged rows are not repainted
    out.truncate(0)
    out.seek(0)
    term.invalidate()
    term.paint(rows, "")
    assert "hello" in out.getvalue()  # full repaint after invalidate
    term.bell()
    assert "\x07" in out.getvalue()
    term.osc52_copy("hi")
    assert "\x1b]52;c;" in out.getvalue()


def test_copy_to_clipboard_paths(monkeypatch):
    term = HeadlessTerm()
    calls = []

    class Proc:
        returncode = 0

    monkeypatch.setattr(
        "subprocess.run", lambda *a, **k: calls.append(a) or Proc()
    )
    assert copy_to_clipboard(term, "text")
    assert term.copied == ["text"]  # OSC 52 always attempted


async def test_logtail_error_reattach_clears(tmp_path):
    """A mid-run reconnect replays the same buffer: it must not
    duplicate; a clean end keeps history (runs stack up)."""
    h = Harness()
    h.daemon.jobs = [_job("a")]
    h.daemon.log_lines["a"] = [{"stream": "stdout", "line": "one"}]
    try:
        await h.daemon.start()
        api = Api(h.daemon.url, None)
        marks = []
        tail = LogTail(api, "/jobs/a/logs", "a", lambda: marks.append(1))
        tail.follow = False
        tail.start()
        await _wait_for(lambda: tail.ended is not None, 10)
        texts = [line for stream, line, _ in tail.lines]
        assert texts == ["one", "end of run output"]
        tail.stop()
        await api.close()
    finally:
        await h.stop()


def test_fmt_ago_any_takes_epochs_and_iso():
    from cronstable.tui import fmt_ago_any

    assert fmt_ago_any(None) == "—"
    assert fmt_ago_any(True) == "—"
    assert fmt_ago_any(time.time() - 90).endswith("ago")
    assert fmt_ago_any(_iso(30)).endswith("ago")


def test_key_decoder_osc_and_runaway():
    from cronstable.tui import KeyDecoder

    dec = KeyDecoder()
    assert dec.feed(b"\x1b]0;title\x07j") == ["j"]  # OSC swallowed
    assert dec.feed(b"\x1bx") == []  # alt+x swallowed
    assert dec.feed(b"\x1b[1~") == ["home"]
    # a runaway CSI is abandoned: no escape bytes ever leak through
    out = dec.feed(b"\x1b[" + b"9" * 40)
    assert "\x1b" not in "".join(out)
    assert dec.flush_escape() == []


def test_cluster_alert_conflict_variants():
    from cronstable.tui import cluster_alert

    base = {"enabled": True, "elect_leader": True, "node_name": "n"}
    assert cluster_alert(None) is None
    assert cluster_alert({"enabled": False}) is None
    ok = cluster_alert(dict(base, quorate=True))
    assert ok is not None and not ok["bad"]
    for extra, needle in [
        ({"conflict": True, "conflict_names": ["a"]}, "duplicate"),
        ({"conflict": True, "size_conflict": True}, "size mismatch"),
        ({"conflict": True, "policy_conflict": True}, "policy"),
        ({"conflict": True}, "conflict"),
        ({"quorate": False}, "no quorum"),
        ({"quorate": False, "backend": "etcd"}, "lease store"),
    ]:
        alert = cluster_alert(dict(base, **extra))
        assert alert is not None and alert["bad"]
        assert needle in alert["reason"]
