# Performance benchmarks

This directory holds the performance regression harness that CI runs on every
commit and enforces on every release. It exists to keep cronstable fast and
small enough for old machines: startup cost, schedule math at 100k-job scale,
config parsing, DAG planning, durable-state I/O, memory footprint, the terminal
dashboard's per-frame string work, and the web dashboard's render hot paths are
all measured, and a release that regresses past a metric's limit does not ship.

## The two tools

- `bench.py` runs the suite and writes one JSON document. The harness is
  stdlib-only and benchmarks whatever cronstable the invoking interpreter can
  import, so the same script can measure an older installed release. A
  benchmark whose API the measured version lacks is recorded as skipped,
  never failed. To keep the measurement honest it runs untimed warm-up passes
  before the timed repeats and (best-effort) pins itself to one CPU and raises
  its priority; benchmarks split into an in-process tier and a noisier
  subprocess tier (cold start, import, peak RSS), selectable with `--tier`.
- `compare.py` takes baseline and current JSON files (several rounds per
  side), merges the rounds, renders a markdown summary and an SVG diverging
  bar chart of every compared metric, and exits nonzero when a gated metric
  regressed. A regression gates only when it clears both its declared limit
  and a couple of its measured noise bands (the per-metric round-to-round
  scatter), so jitter alone can never fail the gate.

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
(for example `--only cronexpr`), `--tier inprocess` (or `subprocess`) selects
one tier, `--warmup N` overrides the warm-up passes, `--no-stabilize` skips
the CPU pin, `--list` prints the inventory, and `--smoke` is the minimal mode
the unit tests use. If cronstable is not installed in the interpreter, the
harness falls back to the source tree it lives in and says so on stderr.

Local numbers are only comparable to other runs on the same machine in the
same session. The CI comparison is paired for exactly that reason: both
versions run interleaved on one runner, in the same weather.

## What CI does with this

The `perf` job in `.github/workflows/release.yml` runs on every push and PR,
in parallel with the build matrix:

1. installs the current commit into one venv and the latest release tag into
   another;
2. runs `bench.py` against both, interleaved, per tier: five rounds of the
   in-process tier and two of the subprocess tier (the harness always comes
   from the current checkout, so both sides run identical measurement code);
3. runs `compare.py` over all the result files.

Per metric, rounds merge with the metric's estimator: best-of-rounds for
time (the minimum is the least noisy statistic of a fixed workload) and
median for memory. A metric fails its gate only when it slows down by more
than its declared percentage limit AND by more than its absolute floor AND by
more than a couple of its measured noise bands, where the noise band is the
two sides' round-to-round scatter combined in quadrature. So microsecond
jitter on a sub-millisecond metric can never gate, and neither can a metric's
own run-to-run wobble; a change that clears the raw limit but sits inside the
noise band is reported (not silently dropped) but does not fail the release.
More in-process rounds exist precisely to tighten that noise-band estimate.

Three refinements keep the gate both tight and honest:

- **Robust noise band.** From three rounds up, the round-to-round scatter is
  the median absolute deviation (scaled to a standard-deviation equivalent),
  not a plain standard deviation. One throttled or GC-stalled round can no
  longer inflate the band and hide a real regression behind it.
- **Interpreter-startup subtraction.** The `startup.*` metrics are dominated
  by Python's own process spawn and interpreter init, which cronstable cannot
  regress. Each side's `startup.python_baseline` is subtracted before the
  delta is computed, so the gate sees cronstable's OWN contribution and a real
  couple-of-ms import regression is not diluted below the limit by ~40ms of
  un-regressable overhead.
- **A tighter default limit.** The deterministic in-process compute metrics
  gate at 15%; the noisier tiers (subprocess process-spawn, real-disk state
  I/O, peak-RSS, browser render) set a looser limit of their own. The noise
  band above still protects every one of them from jitter.

On an ordinary commit the comparison prints warnings only. On a release the
gate is enforced: the `release` job requires `perf`, so a gated regression
blocks publishing. The release then embeds the comparison in its notes,
attaches `perf-chart.svg` (the diff chart), `perf-summary.md` (the full
table), and `perf-results.json` (the merged raw numbers).

To ship an intentional regression, start a pushed commit's subject with
`[perf:accept]`. The regression is still measured and reported in the
release notes, but it does not gate. Only subject lines are scanned, same as
the `[release]` marker.

## Terminal and web UI benchmarks

The dashboards have their own hot paths, and both are measured.

The **terminal UI** (`tui.*`) is pure Python, so it is benchmarked in process
like everything else: the log drawer re-measures, re-cuts and re-inks its whole
buffer each frame and the log search re-scans it, so `tui.log_restyle_5k` and
`tui.log_search_20k` drive `text_width` / `cut_to_width` / `rewrite_sgr` /
`strip_ansi` over a realistic buffer (coloured, plain, wide-glyph and
control-character lines). No terminal and no app loop.

The **web UI** (`webui.*`) is browser JavaScript, so it is timed inside a
headless Chromium via Playwright. The page exposes a `window.__perf` hook ONLY
under the `?perf=1` query string (it is entirely inert otherwise — no global is
defined), giving the harness seed helpers and the real render functions;
`bench.py` seeds synthetic jobs / fleet / log data and times `renderRows`,
`renderFleet` and `updateLogCount` with the page's own `performance.now()`
(batched, because Chromium clamps that clock to ~100us). The whole `webui`
group **skips cleanly** when Playwright or its Chromium build is absent, when
the page predates the `?perf=1` hook (an older release), and in `--smoke`
(the unit test must not launch a browser). The CI `perf` job installs
Playwright + Chromium into the current-side venv (best-effort) so `webui.*`
runs there; to run them locally:

```sh
pip install playwright && playwright install chromium
python benchmarks/bench.py --quick --only webui
```

Because an older release's page carries no `?perf=1` hook, the `webui` metrics
compare new-against-new (a forward-looking gate and a recorded number), not an
old-vs-new delta; the `tui.*` and backend metrics do diff across releases.

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
- A benchmark that measures a child process (cold start, import, peak RSS)
  passes `subprocess=True` so it lands in the subprocess tier.
- Size the timed region so it runs long enough (roughly 50ms+) that
  scheduler and GC jitter are a small fraction; a sub-10ms metric is
  dominated by noise. Rescaling an existing benchmark is safe for the gate
  (the comparison re-measures BOTH sides with the current definition, so it
  never diffs a new workload against a stored old number), but bump the metric
  id anyway so the name keeps meaning one fixed workload across releases and a
  release-notes trend is never silently redefined. `cronexpr.test_match_200k`,
  `schedule.duplicates_20k` and `dag.plan_claim_10k` are such rescales: the id
  suffix carries the new scale, and the old ids drop out.

The suite's own smoke test is `tests/test_benchmarks.py`; it fails if a
headline benchmark starts skipping, so a refactor that breaks a measured API
surfaces in the ordinary test run, not at release time.
