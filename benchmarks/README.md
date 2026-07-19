# Performance benchmarks

This directory holds the performance regression harness that CI runs on every
commit and enforces on every release. It exists to keep cronstable fast and
small enough for old machines: startup cost, schedule math at 100k-job scale,
config parsing, DAG planning, durable-state I/O, and memory footprint are all
measured, and a release that regresses past a metric's limit does not ship.

## The two tools

- `bench.py` runs the suite and writes one JSON document. The harness is
  stdlib-only and benchmarks whatever cronstable the invoking interpreter can
  import, so the same script can measure an older installed release. A
  benchmark whose API the measured version lacks is recorded as skipped,
  never failed.
- `compare.py` takes baseline and current JSON files (several rounds per
  side), merges the rounds, renders a markdown summary and an SVG diverging
  bar chart of the largest changes, and exits nonzero when a gated metric
  regressed.

## Running locally

```sh
python benchmarks/bench.py --quick --json before.json
# ...make your change, then...
python benchmarks/bench.py --quick --json after.json
python benchmarks/compare.py --baseline before.json --current after.json \
    --md diff.md --svg diff.svg
```

`--quick` cuts workloads to roughly a tenth for a fast local loop; CI runs
the full suite. `--only <substring>` selects benchmarks by name or group
(for example `--only cronexpr`), `--list` prints the inventory, and
`--smoke` is the minimal mode the unit tests use. If cronstable is not
installed in the interpreter, the harness falls back to the source tree it
lives in and says so on stderr.

Local numbers are only comparable to other runs on the same machine in the
same session. The CI comparison is paired for exactly that reason: both
versions run interleaved on one runner, in the same weather.

## What CI does with this

The `perf` job in `.github/workflows/release.yml` runs on every push and PR,
in parallel with the build matrix:

1. installs the current commit into one venv and the latest release tag into
   another;
2. runs `bench.py` against both, interleaved, for two rounds (the harness
   always comes from the current checkout, so both sides run identical
   measurement code);
3. runs `compare.py` over the four result files.

Per metric, rounds merge with the metric's estimator: best-of-rounds for
time (the minimum is the least noisy statistic of a fixed workload) and
median for memory. A metric fails its gate only when it slows down by more
than its declared percentage limit AND by more than its absolute floor, so
microsecond jitter on a sub-millisecond metric can never gate.

On an ordinary commit the comparison prints warnings only. On a release the
gate is enforced: the `release` job requires `perf`, so a gated regression
blocks publishing. The release then embeds the comparison in its notes,
attaches `perf-chart.svg` (the diff chart), `perf-summary.md` (the full
table), and `perf-results.json` (the merged raw numbers).

To ship an intentional regression, start a pushed commit's subject with
`[perf:accept]`. The regression is still measured and reported in the
release notes, but it does not gate. Only subject lines are scanned, same as
the `[release]` marker.

## Adding a benchmark

Register a function in `bench.py` with the `@bench(...)` decorator:

```python
@bench(
    "group.short_name",       # stable metric id; renaming loses history
    "group",
    detail="one line of what the workload is",
    repeats=(5, 2, 1),        # full / quick / smoke repeats
    gate_pct=25.0,            # regression limit, percent
    gate_floor=0.010,         # and the absolute floor, in the metric's unit
)
def bench_thing():
    ...setup (untimed)...
    t0 = time.perf_counter()
    ...the workload...
    return time.perf_counter() - t0
```

Ground rules:

- Time only the workload; do setup outside the timed region, and use
  `fixture(name, builder)` for expensive setup shared across repeats.
- Scale the workload with `_n(base)` so `--quick` and `--smoke` stay cheap.
- Import cronstable inside the function and raise `Skip` when an API is
  missing, so the harness still runs against older releases.
- Keep workloads deterministic: fixed datetimes, fixed inputs, no network.
- Memory metrics use `unit="MB"` and `compare="median"`.

The suite's own smoke test is `tests/test_benchmarks.py`; it fails if a
headline benchmark starts skipping, so a refactor that breaks a measured API
surfaces in the ordinary test run, not at release time.
