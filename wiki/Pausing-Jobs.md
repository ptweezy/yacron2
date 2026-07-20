# Pausing Jobs

cronstable can pause a job at runtime: its scheduled fires are skipped until the pause expires or someone resumes it, without touching the configuration. A pause is a bounded window, never a permanent state; every pause carries an `until` deadline (one hour by default, thirty days at most), so a job silenced during an incident always comes back on its own. For a stop that should survive indefinitely, edit the config (`enabled: false`) instead.

The mechanics live in `cronstable/cron.py` (`Cron.pause_job_by_name` / `Cron.resume_job_by_name`), one code path shared by every surface: the [HTTP API](HTTP-API#post-jobsnamepause) (`POST /jobs/{name}/pause` and `/resume`), the [web dashboard](Web-Dashboard) and [terminal dashboard](Terminal-Dashboard) (the `p` key and the drawer button), and the `cron_pause_job` / `cron_resume_job` [MCP tools](MCP).

## Pausing and resuming

`POST /jobs/{name}/pause` takes an optional JSON body:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `durationSeconds` | int | `3600` (one hour) | How long the pause lasts, `1` to `2592000` (thirty days). Exclusive with `until`. |
| `until` | ISO-8601 string | (none) | Absolute expiry instant. Must be in the future and at most thirty days away; a timestamp without a UTC offset is read as UTC. Exclusive with `durationSeconds`. |
| `note` | string | `""` | Free-text audit note, up to 500 characters, shown wherever the pause shows. |
| `by` | string | `"api"` | Who paused it, up to 100 characters. |

An empty (or absent) body pauses for the default hour. The response is `200` with the pause record:

```shell
$ http post http://127.0.0.1:8080/jobs/nightly-etl/pause durationSeconds:=7200 note="upstream DB migration" by=parker
{
    "paused": {
        "since": "2026-07-19T14:00:00+00:00",
        "until": "2026-07-19T16:00:00+00:00",
        "note": "upstream DB migration",
        "by": "parker",
        "channel": "api"
    }
}
```

Pausing an already-paused job overwrites the window (idempotent, and how you extend a pause). An unknown job is a `404`; both keys at once, a past or over-cap `until`, an out-of-range duration, a wrong type, or an oversized `note`/`by` is a `400`. `channel` records which surface acted (`api`, `mcp`).

`POST /jobs/{name}/resume` (optional body: `by`) ends the pause immediately and returns `{"paused": null}`. Resuming a job that is not paused is a no-op with the same response. Both routes are mutating: they sit behind [`web.authToken`](HTTP-API#authentication) and the [cross-site request defense](HTTP-API#cross-site-request-defense) like `start` and `cancel`.

## What a pause does

- **Scheduled fires are skipped, visibly.** Each due slot writes a synthetic row to the run ledger with outcome `skipped` and `skip_reason: "paused"` (no `started_at`, no `exit_code`), so the history says "a fire was due here and was deliberately not run" instead of showing a silent gap. The dashboards paint these rows neutrally, and they stamp no success or failure state.
- **Pending retries defer.** An armed [retry ladder](Failure-Detection-and-Retries) is neither consumed nor cancelled: the attempt waits and fires after the resume. A pause is "hold my fires", not a verdict on the ladder.
- **Catch-up owes nothing for the window.** [Missed-run catch-up](Durable-State) never backfills slots that fell inside a pause window, including slots the daemon slept through while it was down (the durable pause record excuses them). Slots after the expiry are owed as normal.
- **Manual start still works.** `POST /jobs/{name}/start` launches a paused job (unlike a disabled one, which is refused with `409`): the operator asking by name outranks the standing "skip the schedule" instruction. Cancel is likewise unaffected.
- **Running instances are unaffected.** Pausing stops future fires; it never touches a run already in flight.
- **The pause sticks to the name.** Config reloads and edits to the job leave an active pause in place; only removing the job from the config drops it.

While paused, the job's [SLA checks](Late-Run-Detection) are suppressed: a deliberately held job must not page as overdue, and pause-skipped slots are never counted as late.

## Expiry

Expiry needs no timer and no write: a pause window whose `until` has passed reads as absent at every consumer at once, and the housekeeping pass (once per wall-clock minute) sweeps the stale entry and logs the auto-resume. There is nothing to clean up if the daemon restarts across the deadline.

## Durability and clusters

Without a [`state:` store](Durable-State) a pause is in-memory: a daemon restart forgets it and the schedule resumes. With one, each pause and resume appends a record to the job's durable `paused/<job>` stream (newest record wins), so:

- a pause **survives restarts**: boot rehydrates the active windows before the first fire;
- a pause is **fleet-wide**: every node sharing the store honors it, whichever node accepted the request. The record's `host` field is audit information only. Peers pick a pause or resume up on their housekeeping pass, so cross-node propagation takes up to about a minute; the node that handled the request applies it immediately.

The scheduling hot path never reads the store; fire-time checks are memory-only. When the store cannot be read, each node keeps its last known in-memory pause state and logs a warning, under either `onStoreUnavailable` policy: a pause is an operator convenience, not a correctness fence, so an unreadable store neither resurrects nor drops pauses, and never blocks firing.

## Where a pause shows

| Surface | What appears |
| --- | --- |
| [HTTP API](HTTP-API#get-jobs) | `GET /jobs` always carries a `paused` field: `null`, or `{since, until, note, by, channel}`. Skipped slots appear in `GET /jobs/{name}/runs` as `outcome: "skipped"` rows with `skip_reason`. |
| [`GET /schedule/why`](Why-No-Run) | A probe against a paused job answers with a `paused` note naming the expiry, the actor, and the note, so "the schedule matched but nothing ran" explains itself. |
| [Web dashboard](Web-Dashboard) | A **Paused** status with a `⏸` chip carrying the expiry and note, a paused summary pill and wallboard tile, and one-click pause/resume (row button, drawer button, palette, the `p` key). |
| [Terminal dashboard](Terminal-Dashboard) | The same status, `⏸ til HH:MM` in the next-fire column, and the same `p` toggle. |
| [Prometheus](Metrics-with-Prometheus#per-job) | `cronstable_job_paused{job_name}` is `1` while the job is paused; `cronstable_job_runs_total` counts the skipped slots under `status="skipped"`. |
| [MCP](MCP) | The observe tools report the same `paused` object; `cron_pause_job` / `cron_resume_job` act on it. |

## See also

- [Late-Run Detection](Late-Run-Detection): the SLA checks a pause suppresses.
- [HTTP Control API](HTTP-API): the endpoint reference, authentication, and error shapes.
- [Durable State](Durable-State): the store behind restart survival and fleet-wide pauses.
- [Failure Detection and Retries](Failure-Detection-and-Retries): the retry ladder that defers across a pause.
- [Why Didn't It Run?](Why-No-Run): probing one timestamp, pause note included.
