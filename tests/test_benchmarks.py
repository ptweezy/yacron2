"""Smoke tests for the performance benchmark tooling in benchmarks/.

The CI perf gate runs benchmarks/bench.py against both the current commit and
the previous release and diffs the two with benchmarks/compare.py, so a
harness that crashes (or silently skips everything) would take the release
gate down with it.  These tests run the suite in its minimal --smoke mode and
exercise compare.py's merge, chart, and gate logic on synthetic inputs.
"""

import json
import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BENCH = os.path.join(REPO_ROOT, "benchmarks", "bench.py")
COMPARE = os.path.join(REPO_ROOT, "benchmarks", "compare.py")


def _run(args, **kwargs):
    return subprocess.run(
        [sys.executable] + args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        **kwargs,
    )


def test_bench_smoke_produces_results(tmp_path):
    out = tmp_path / "smoke.json"
    proc = _run([BENCH, "--smoke", "--json", str(out)])
    assert proc.returncode == 0, proc.stdout
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert doc["schema"] == 1
    assert doc["mode"] == "smoke"
    results = {r["name"]: r for r in doc["results"]}
    ran = [r for r in results.values() if not r["skipped"]]
    # The suite is exhaustive; even smoke mode must exercise the bulk of it.
    # The only expected skips are the POSIX-only RSS metrics on Windows.
    assert len(ran) >= 30, sorted(
        (r["name"], r.get("reason")) for r in results.values() if r["skipped"]
    )
    for r in ran:
        assert r["value"] >= 0.0
        assert r["unit"] in ("s", "MB")
    # The headline metrics must never silently skip.
    for name in (
        "startup.version",
        "schedule.cold_build_100k",
        "config.parse_yaml_300",
        "state.append_1k",
    ):
        assert not results[name]["skipped"], results[name]


def test_bench_only_filter(tmp_path):
    out = tmp_path / "only.json"
    proc = _run([BENCH, "--smoke", "--only", "redact", "--json", str(out)])
    assert proc.returncode == 0, proc.stdout
    doc = json.loads(out.read_text(encoding="utf-8"))
    names = [r["name"] for r in doc["results"]]
    assert names and all(n.startswith("redact.") for n in names)


def _entry(name, value, gate_pct=25.0, gate_floor=0.010, unit="s"):
    return {
        "name": name,
        "group": name.split(".")[0],
        "detail": "",
        "unit": unit,
        "gate_pct": gate_pct,
        "gate_floor": gate_floor,
        "compare": "min",
        "info": False,
        "skipped": False,
        "reason": None,
        "runs": 1,
        "values": [value],
        "value": value,
        "mean": value,
        "median": value,
        "stdev": 0.0,
        "min": value,
        "max": value,
    }


def _doc(entries, version="1.0.0"):
    return {
        "schema": 1,
        "mode": "smoke",
        "cronstable_version": version,
        "results": entries,
    }


def _write(path, doc):
    path.write_text(json.dumps(doc), encoding="utf-8")
    return str(path)


def test_compare_gates_regression_and_honors_floor(tmp_path):
    base = _write(
        tmp_path / "base.json",
        _doc(
            [
                _entry("startup.version", 0.100),
                # Big relative but sub-floor absolute change: must not gate.
                _entry("micro.jitter", 0.0001),
            ]
        ),
    )
    cur = _write(
        tmp_path / "cur.json",
        _doc(
            [
                _entry("startup.version", 0.200),  # +100%: gates
                _entry("micro.jitter", 0.0002),  # +100% but +0.1ms: floor
            ],
            version="1.1.0",
        ),
    )
    md = tmp_path / "out.md"
    svg = tmp_path / "out.svg"
    proc = _run(
        [
            COMPARE,
            "--baseline",
            base,
            "--current",
            cur,
            "--md",
            str(md),
            "--svg",
            str(svg),
        ]
    )
    assert proc.returncode == 1, proc.stdout
    assert "startup.version" in proc.stdout
    assert "micro.jitter" not in proc.stdout.split("gate:")[-1]
    text = md.read_text(encoding="utf-8")
    assert "Gate: FAILED" in text
    assert svg.read_text(encoding="utf-8").startswith("<svg")

    # --warn-only and --accept both downgrade the failure to exit 0.
    for flag in ("--warn-only", "--accept"):
        proc = _run([COMPARE, "--baseline", base, "--current", cur, flag])
        assert proc.returncode == 0, (flag, proc.stdout)
        assert "::warning::" in proc.stdout


def test_compare_identical_passes_and_merges_rounds(tmp_path):
    r1 = _write(tmp_path / "r1.json", _doc([_entry("startup.version", 0.120)]))
    r2 = _write(tmp_path / "r2.json", _doc([_entry("startup.version", 0.100)]))
    md = tmp_path / "out.md"
    merged = tmp_path / "merged.json"
    proc = _run(
        [
            COMPARE,
            "--baseline",
            r1,
            "--baseline-label",
            "prev",
            "--current",
            r1,
            r2,
            "--md",
            str(md),
            "--merged-out",
            str(merged),
        ]
    )
    assert proc.returncode == 0, proc.stdout
    assert "Gate: passed" in md.read_text(encoding="utf-8")
    merged_doc = json.loads(merged.read_text(encoding="utf-8"))
    # Two rounds with compare="min" merge to the faster round.
    assert merged_doc["results"][0]["value"] == 0.100


def test_compare_without_baseline_records_first_release(tmp_path):
    cur = _write(tmp_path / "cur.json", _doc([_entry("startup.version", 0.1)]))
    md = tmp_path / "out.md"
    proc = _run([COMPARE, "--current", cur, "--md", str(md)])
    assert proc.returncode == 0, proc.stdout
    text = md.read_text(encoding="utf-8")
    assert "No previous release baseline" in text
    assert "startup.version" in text
