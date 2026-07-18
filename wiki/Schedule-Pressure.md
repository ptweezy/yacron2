# Schedule Pressure

The collision heatmap for your whole fleet: cronstable enumerates every enabled schedule's fires over the next 24 hours with the scheduler's own engine and buckets them into an hour-by-minute grid. It answers, with data, the questions a growing job set eventually raises: how many jobs fire at `:00`? Which minutes are empty? Is the 03:30 batch window actually quiet?

The enumeration runs through the same `CronTab.occurrences()` the scheduler fires from, so it is timezone-exact per job and DST-exact across transitions: a fall-back hour's doubled fires genuinely count twice. Disabled jobs and `@reboot` jobs are excluded (neither fires on a schedule, so neither can collide) and reported in an `excluded` count. [DAG](Orchestration-and-DAGs) schedules ride along as their synthetic `dag:<name>` job. Sub-minute (7-field) schedules weigh each matched minute by how many seconds they fire on.

## The endpoint

```
GET /schedule/pressure?hours=24&tz=Europe/London
```

Both parameters are optional: `hours` is 1 to 168 (default 24) and `tz` picks the display zone the grid's civil labels use (default UTC; each job still fires in its own configured zone). The payload is the full picture:

| Field | Meaning |
|-------|---------|
| `grid` | 24 rows (hour of day) of 60 fire counts (minute of hour). |
| `by_minute_fires` / `by_minute_jobs` | The 60-bin histogram: total fires and distinct jobs at each minute of the hour, across the whole window. |
| `by_hour` | Total fires per hour row. |
| `busiest_minute` | `{minute, jobs, fires}`: the "37 jobs fire at :00" headline. |
| `empty_minutes` | The minutes of the hour nothing fires on. |
| `top_cells` | The heaviest grid cells, each naming up to ten of its jobs. |
| `jobs`, `total_fires`, `excluded` | Fleet totals, plus how many jobs were excluded as disabled or `@reboot`. |

See [HTTP API](HTTP-API) for the route table. The same analyzer backs the `cron_schedule_pressure` [MCP tool](MCP), so an AI agent can read the fleet's pressure without screen-scraping the dashboard.

## In the dashboards

- The [web dashboard](Web-Dashboard) grows a **schedule pressure** card (the `▥ pressure` toolbar toggle): the 24x60 grid with hot cells highlighted, the minute-of-hour histogram, the [duplicate-schedule groups](Duplicate-Schedule-Detection), and the [suggest-a-slot](Suggest-a-Slot) buttons, with a UTC/local display-zone switch. The wallboard (TV mode) shows a compact pressure strip above the tile grid whenever the panel is enabled, so the room sees the next stampede coming.
- The [terminal dashboard](Terminal-Dashboard) has the same panel as an overlay (command palette: "Toggle schedule pressure"), computed locally from its `/jobs` snapshot with the identical shared analyzer, so it works against older daemons too.

Both refresh about once a minute; the forecast only moves minute by minute, so there is nothing to gain from polling faster.

## Reading it

A healthy fleet's grid is boring: load spread wide, few hot columns. The patterns worth acting on:

- **A hot `:00` column** is the classic thundering herd: cron's default minute. Spread it with [`H` hashed schedules](Hashed-Schedules), or move individual jobs to minutes [suggested from real load](Suggest-a-Slot).
- **Hot columns at `:00/:15/:30/:45`** mean everyone picked the same "nice" step phases; `H/15` keeps the cadence and spreads the phase.
- **A solid row** is an hourly window where many daily jobs pile up (backup hour, report hour). Check the row's cells before adding another job there.
- **Many identical rows** usually mean [duplicate schedules](Duplicate-Schedule-Detection): the same expression pasted across jobs.
