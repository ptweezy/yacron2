# Performance Benchmarks

Every commit is benchmarked, and every release is gated on the result.
cronstable is meant to run comfortably on old and small machines, so the CI
pipeline measures the paths that determine that (process startup, schedule
math at 100k-job scale, config parsing, DAG planning, durable-state I/O,
JSON, redaction, calendar rendering, and memory footprint) and refuses to
publish a release that regressed past a metric's declared limit.

## What is measured

The suite lives in [`benchmarks/`](https://github.com/ptweezy/cronstable/blob/develop/benchmarks)
and currently covers about 37 metrics across these groups:

| Group | Examples |
|---|---|
| `startup` | wall clock of `cronstable --version`, importing the scheduling engine, importing the full daemon graph, `--validate-config` over a 100-job file |
| `cronexpr` | parsing plain and extended expressions (ranges, steps, `L`, `W`, `#`, `H`, seconds), `next()` search, enumerating occurrences, instant matching |
| `config` | YAML parsing of a 300-job config, per-job `JobConfig` construction, classic crontab parsing |
| `schedule` | building the fire schedule for 100,000 jobs from cold, reseeding it pre-parsed, schedule pressure over 24 hours, duplicate detection, slot suggestion |
| `dag` | building and validating 10k-task graphs, the plan-and-claim transform over a 2k-task run |
| `state` | appending durable records, cold and memoized `derive_max`, listing records, job KV round trips |
| `json`, `fingerprint`, `redact`, `ical` | serialization round trips, job-set fingerprinting at 10k jobs, log redaction with and without secrets, iCal rendering |
| `memory` | traced bytes held by parsed schedules and job configs, peak RSS of a real `--version` process and of the daemon import |

Time metrics report the wall clock of a fixed workload; memory metrics report
MB. Lower is always better.

## How the comparison works

Runner hardware in CI is noisy, so absolute times from different runs are not
compared. Instead the `perf` job makes a paired measurement on one runner:

1. The current commit is installed into one virtualenv, and the latest
   release tag into another.
2. The suite runs against both, interleaved, for two rounds. The harness
   itself always comes from the current checkout, so both sides execute
   identical measurement code; a benchmark whose API the old release lacks
   is recorded as skipped for that side.
3. `benchmarks/compare.py` merges each side's rounds (best-of-rounds for
   time, median for memory) and diffs the two.

A metric fails its gate only when it slows down by more than its declared
percentage limit (25% for most timing metrics, 15% for traced memory) AND by
more than its absolute floor, so microsecond jitter on a tiny metric can
never gate. On an ordinary commit or pull request the comparison only warns.
On a release the gate is enforced: the publish jobs require `perf`, so a
gated regression stops the release before anything ships.

## The release chart

Each GitHub Release carries the comparison against the previous release:

- a diverging bar chart (`perf-chart.svg`) with a row for every compared
  metric, embedded at the top of the performance section of the release
  notes;
- the full metric table in a collapsed details block;
- `perf-results.json`, the merged raw numbers for that release, attached as
  an asset.

The first release after the suite was introduced records numbers without a
comparison; every release after that diffs against the one before it.

## Accepting an intentional regression

A feature can be worth a measured cost. To ship one, start a pushed commit's
subject line with `[perf:accept]`. The regression is still measured and
listed in the release notes, but it does not fail the gate. Only commit
subjects are scanned, exactly like the `[release]` marker described in
[Contributing and Releasing](Contributing-and-Releasing).

## Running the suite yourself

```sh
python benchmarks/bench.py --quick --json before.json
# make a change
python benchmarks/bench.py --quick --json after.json
python benchmarks/compare.py --baseline before.json --current after.json --md diff.md
```

`--quick` trims workloads to roughly a tenth for a fast local loop, and
`--only <substring>` runs one group (for example `--only cronexpr`). Compare
only runs made on the same machine. The full harness reference, including how
to add a benchmark, is in
[`benchmarks/README.md`](https://github.com/ptweezy/cronstable/blob/develop/benchmarks/README.md).

## Related pages

- [Contributing and Releasing](Contributing-and-Releasing): the release
  pipeline this gate is part of, and the `[release]` marker syntax.
- [Architecture and Internals](Architecture-and-Internals): the components
  the benchmark groups map onto.
- [Schedule Pressure](Schedule-Pressure), [Duplicate Schedule Detection](Duplicate-Schedule-Detection),
  [Suggest a Slot](Suggest-a-Slot): the fleet analyzers several `schedule`
  metrics exercise.
