# Web Dashboard

yacron2 includes a **built-in web dashboard**: a single, self-contained HTML page
(one inline `<script>`, inline styles, no external assets, no build step, and no
database) served by the optional [HTTP Control API](HTTP-API). It turns the daemon
into a live, keyboard-driven control room. Watch every job's status, tail its
output as it runs, review run history, and preview upcoming schedules, all from a
browser.

[![The yacron2 web dashboard, a live overview of every job, showing status, schedule, last run, next-run countdown, and a run-trend sparkline](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-overview.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-overview.png)

## Enabling and opening it

The dashboard is part of the HTTP interface, so it appears as soon as you add a
`web` section with at least one `http://` listener (see
[HTTP Control API](HTTP-API) for the full configuration reference):

```yaml
web:
  listen:
    - http://127.0.0.1:8080
```

Open the listener's root path in a browser, <http://127.0.0.1:8080/> for the
example above. The page is served at `/` on every `http://` listener and is
self-contained, so nothing else needs to be installed or hosted.

The HTML document is returned with defense-in-depth security headers, including a
strict `Content-Security-Policy` (`default-src 'self'`, `connect-src 'self'`,
`frame-ancestors 'none'`, anti-clickjacking `X-Frame-Options: DENY`, and
`X-Content-Type-Options: nosniff`). Any header you set under `web.headers` is
merged on top of these defaults, so you can relax or extend them deliberately.

To expose only the REST API and **not** the dashboard, set `ui: false`:

```yaml
web:
  listen:
    - http://127.0.0.1:8080
  ui: false
```

## The job overview

The landing page is a single sortable, filterable table of every configured job.

The header carries a live UTC clock, a **connection indicator** (`live` when the
server is responding, `no signal` when polls are failing; hover it to see how
long ago the last successful response arrived), and **summary pills** counting the
total jobs and how many are running, failing, and OK.

Each row shows:

| Column | What it shows |
| --- | --- |
| **Status** | The job's current health: one of **Running**, **Failed**, **OK**, **Pending** (enabled but never run yet), **Cancelled**, or **Disabled** (`enabled: false`), each with a color and glyph. |
| **Job** | The job `name` and its command. |
| **Owner** | *(cluster only, under [`distribution: spread`](Clustering-and-Leader-Election#distribution-one-leader-or-spread-the-load))* the node that currently **owns** the job. The jobs owned by the node you're viewing are highlighted in the accent color, so you can see at a glance which work lands here; `EveryNode` jobs read **all nodes**, and a `Leader` job with no quorum reads **no quorum**. The column is hidden entirely outside spread mode. Sortable, so you can group jobs by node. |
| **Schedule** | The raw schedule string; hover it for a plain-English reading. |
| **Last run** | How long ago the last run finished (kept fresh every second) and an exit-code badge. |
| **Took** | The last run's duration. |
| **Next** | A live countdown to the next scheduled run (`—` while running or disabled). |
| **Trend** | A **sparkline** of recent runs: one bar per run, height by duration, colored by outcome. |
| **Actions** | One-click **Run** (or **Stop**, for a running job) and **Logs**. |

The toolbar above the table lets you:

- **filter** by typing in the search box (matches name or command; press `/` to focus it);
- narrow by status with the **all / ok / fail / run / off** segmented control;
- **sort** by name, status, last run, next run, or duration (from the dropdown, or by clicking a column header, clicking again to reverse);
- **run every failing job at once** with the **run failing** button.

## The job drawer

Clicking a job (or pressing `Enter` on the selected row) opens a detail drawer
with three tabs: **Logs**, **History**, and **Schedule**. The drawer header
repeats the job's status, schedule (with its plain-English reading), the running
PID(s), and a one-click button to copy the command. When
[leader election](Clustering-and-Leader-Election#per-job-policy) is enabled, the
header also shows the job's active **`clusterPolicy`** (`Leader`,
`PreferLeader`, or `EveryNode`), and under
[`distribution: spread`](Clustering-and-Leader-Election#distribution-one-leader-or-spread-the-load)
the node that currently **owns** the job (e.g. `cluster: Leader → yacron-c`).
Jobs are **deep-linkable**:
opening a job updates the URL to `#job/<name>`, so you can bookmark or share a
direct link to it.

### Logs: live output, in your browser

[![Live log tailing in the drawer, with ANSI color, line numbers, and an in-log search highlighting every match](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-logs.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-logs.png)

The Logs tab streams a job's captured output over
[Server-Sent Events](https://developer.mozilla.org/docs/Web/API/Server-sent_events):
it replays the most recent buffered lines first, then appends new lines live as a
running job produces them. Features:

- **ANSI color** rendering (toggle off to see raw text), with `stderr` lines distinguished from `stdout`;
- absolute **line numbers** and optional per-line **timestamps**;
- in-log **search / grep**, plain-text or **regex**, with a live match count, `Enter` to jump between matches, and a **matches-only** mode that hides non-matching lines;
- **follow** (auto-scroll) and **line-wrap** toggles;
- one-click **download** of the buffered log, and a **clear** button;
- **Run** and **Cancel** buttons right above the output.

Output is only available for the streams a job captures, so enable
[`captureStdout` / `captureStderr`](Output-Capturing) on jobs whose output you
want to watch here. (If neither is enabled, the pane says so rather than sitting
empty.) The view is bounded to the most recent lines so a chatty job can't grow
the tab without limit.

### History: outcomes and durations over time

[![The history tab: a stats grid (runs, success rate, ok/fail, avg/min/max duration), a per-run duration bar chart, and a detailed run table](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-history.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-history.png)

The History tab summarizes the job's retained run history:

- a **stats grid**: total runs, **success rate**, OK / fail counts, and average / min / max duration. The success rate is computed over runs that ran to completion, so deliberate **cancellations are excluded**;
- a **duration bar chart**, one bar per run (newest on the right), colored by outcome;
- a **run table**: outcome, exit code, when it finished, how long it took, and a reason for any run that carries one (failed runs, and runs cancelled from the dashboard).

History is retained **in memory only**, up to the most recent 50 runs per job, and
resets when yacron2 restarts.

### Schedule: in plain English, in the right timezone

[![The schedule tab: a plain-English reading of the cron expression and a timezone-aware list of the next run times](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-schedule.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-schedule.png)

The Schedule tab turns the cron expression into something you can read at a glance:

- a **plain-English description** (e.g. *"At 19:27, on Monday and Friday"*), understanding ranges, steps, lists, names, and the `@daily`/`@hourly`/`@reboot` macros;
- a preview of the **next run times**, computed live in the browser and shown in the job's own [timezone](Schedules-and-Timezones) (UTC, server-local, or an arbitrary IANA zone such as `America/Los_Angeles`), each with a relative countdown;
- impossible schedules (such as the 31st of February) are detected and called out rather than described as if they will fire;
- a key/value summary of whether the job is enabled, its timezone frame, a concurrency note, and its command.

## Cluster panel

When a [`cluster`](Clustering-and-Leader-Election)
section is configured, the dashboard shows a **cluster panel** below the job
table (it stays hidden otherwise). The panel polls `GET /cluster` alongside the
job list and renders:

> **Try it:** the bundled Compose demos bring the dashboard up with one command.
> [`docker-compose-cluster.yml`](https://github.com/ptweezy/yacron2/blob/develop/docker-compose-cluster.yml)
> starts the three-node gossip cluster (`yacron-a` / `yacron-b` / `yacron-c`) so
> you can watch the peer table, roles, and leadership move live; stop a node to
> see the summary and dots react. For the ambient wallboard view (including the
> "zen" all-clear screensaver),
> [`docker-compose-zen.yml`](https://github.com/ptweezy/yacron2/blob/develop/docker-compose-zen.yml)
> runs a single deliberately calm node.

- a **summary line** with this node's name (e.g. `yacron-a`) and the agreement
  tally (e.g. `yacron-a · 2/2 agreed`); when
  [leader election](Clustering-and-Leader-Election#leader-election)
  is on, it also shows the live quorum count and this node's role: **leader**,
  **follower** (with the current leader's name), or **no quorum** when the node
  has stood down. Under [`distribution: spread`](Clustering-and-Leader-Election#distribution-one-leader-or-spread-the-load)
  there is no single leader, so the role reads **spread (per-job owner)** while
  quorate, or **spread (no quorum)** otherwise. Under spread the summary also
  reports how many jobs **this** node owns (e.g. `… · owns 3`). When the cluster
  is in **conflict** the role instead reads **standing down (conflict)**, because
  the conflict fails `Leader` jobs closed regardless of who holds the lead, and
  the summary appends a loud **`⚠ … — Leader jobs paused`** note naming the
  cause, one of three: a **duplicate `nodeName`** (two peers advertising the same
  name), a **cluster size mismatch** (peers declaring different cluster sizes,
  e.g. mid-resize), or a **coordination policy mismatch** (a peer running a
  different `distribution` / `elect_leader`). See
  [Clustering and Leader Election](Clustering-and-Leader-Election) for what each
  conflict means and how to clear it;
- a **per-peer table** with the on-screen headers **Peer** | **Node** | **Owns**
  | **Status** | **Job set**, listing each peer's address, reported node name,
  status, and the short form of its job-set id, with a coloured **status dot**:
  green for `agreed`, amber for `syncing`, red for `drifted` / `untrusted` /
  `conflict`, grey for `unreachable`, blue for `self`, and a faint-grey dot for a
  peer that is `unknown` (configured but not yet contacted, so no status has come
  back). The **Owns** column (counting how many jobs each node owns) is present
  only under
  [`distribution: spread`](Clustering-and-Leader-Election#distribution-one-leader-or-spread-the-load),
  so the whole job-to-node distribution is visible at a glance (it pairs with the
  per-job **Owner** column in the job table above); outside spread it reads `—`.

Peers the node has listed as its own address (`self`) are excluded from the
agreement tally. This makes it easy to watch a rolling deploy (`syncing` →
`agreed`), spot drift, or watch leadership move when a node goes down.

A **`▚ timeline`** button in the panel header toggles a per-peer **swimlane**: a
lane per peer, coloured by status over time, that accumulates in the browser
while the tab is open (so you can see a `syncing` → `agreed` convergence or a
flapping peer as a stripe rather than a single instant). It charts the gossip
peer set only, so it is hidden entirely for the lease backends.

Separately from the in-panel summary, a cluster incident also raises the
page-level **CLUSTER ALERT** bar at the top of the dashboard: a red incident
banner shown whenever this node reports a cluster conflict (duplicate `nodeName`,
size mismatch, or coordination policy mismatch) or has **lost quorum**, so the
alert is visible without scrolling down to the panel. It names the cause and,
under a lease backend, phrases quorum loss as the lease store being unreachable.

The bullets above describe the **gossip** backend. The
[lease backends](Clustering-and-Leader-Election#operating-the-lease-backends-kubernetes-and-etcd)
(`kubernetes` / `etcd`) have no peer set, so the panel renders a different shape:
a **role summary** (`node-name · backend · role`, where the role is **leader**,
**follower (leader: …)**, **follower**, or **no quorum (store unreachable)**)
followed by a **key/value lease-detail table**: no status dots, no agreement
tally, no quorum count. The table renders every non-null key the backend reports,
so beyond the lease/election name, the holder, the identity, and the expiry it
also surfaces the kubernetes `namespace` or the etcd `leaseId` when present. There
`no quorum` means *the lease store is unreachable from this node*, not "no
majority". See
[Clustering and Leader Election](Clustering-and-Leader-Election#observing-the-cluster)
for the full `GET /cluster` field semantics.

## Command palette

[![The command palette open, fuzzy-matching the query "run" to per-job run and log actions](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-palette.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-palette.png)

Press `Ctrl-K` (or `⌘K`, or `Ctrl-P`) to open a **fuzzy command palette**. It
searches both global actions (refresh, run all failing jobs, cycle theme, toggle
effects, open settings, set the access token…) and a per-job action for every job
(open its logs, run it, cancel it, copy its command, view its schedule). Type to
filter, arrow keys to move, `Enter` to run.

## Keyboard shortcuts

[![The keyboard shortcut reference overlay listing every shortcut](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-shortcuts.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-shortcuts.png)

The dashboard is keyboard-first. Press `?` at any time for this overlay.

| Key | Action |
| --- | --- |
| `Ctrl-K` / `⌘K` | Open the command palette |
| `/` | Focus the filter box |
| `j` / `↓` | Select the next job |
| `k` / `↑` | Select the previous job |
| `Enter` | Open the selected job |
| `r` | Run the selected job |
| `x` | Cancel the selected (running) job |
| `c` | Copy the selected job's command |
| `g` | Refresh now |
| `t` | Cycle the theme |
| `?` | Show the shortcut list |
| `Esc` | Close the open panel or drawer |

## Settings, themes, and notifications

[![The settings panel: theme, CRT effects, scanlines, compact density, desktop notifications, and refresh interval](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-settings.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-settings.png)

The settings panel (and the command palette) expose:

- **Three themes**: amber and green **phosphor CRT**, or a flat **modern** theme. Cycle them with `t`.
- **CRT effects** (phosphor glow, vignette, and a subtle flicker) and **scanlines**, each toggleable. They apply only to the CRT themes and automatically respect `prefers-reduced-motion`.
- **Compact density** for tighter rows.
- **Desktop notifications** that fire when a job fails (after you grant the browser permission).
- A **refresh interval** of 1s / 2s / 3s / 5s / 10s, or paused.

All preferences are remembered in the browser's `localStorage`, so the dashboard
comes back the way you left it.

| Green phosphor CRT | Flat modern theme |
| :---: | :---: |
| [![The dashboard in the green phosphor CRT theme](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-theme-green.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-theme-green.png) | [![The dashboard in the flat modern theme](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-theme-modern.png)](https://raw.githubusercontent.com/ptweezy/yacron2/develop/docs/img/dashboard-theme-modern.png) |

## Authentication

When [bearer-token authentication](HTTP-API#authentication) is enabled
(`web.authToken`), the dashboard **page itself** loads without a token: it
carries no data and no secrets. The first data request returns `401`, and the
dashboard then prompts you for the token, stores it **only in that browser tab**
(`sessionStorage`), and attaches it as `Authorization: Bearer …` on every
subsequent request. You can update or clear the stored token from the header's
token button at any time.

## What it polls, and the data model

The dashboard is a thin client over the [HTTP Control API](HTTP-API):

- it polls `GET /jobs` on the refresh interval for the overview (each job carries a compact tail of recent runs for the sparkline);
- it polls `GET /cluster` on the same interval for the [cluster panel](#cluster-panel) (the panel stays hidden unless a cluster section is configured);
- opening a job's **History** tab fetches `GET /jobs/{name}/runs` (full retained history plus aggregate stats);
- opening the **Logs** tab opens the `GET /jobs/{name}/logs` SSE stream;
- the **Run** / **Stop** buttons call `POST /jobs/{name}/start` and `POST /jobs/{name}/cancel`;
- the version in the header comes from `GET /version`.

All run history and captured output lives **in memory** in the running daemon
(the most recent 50 runs per job) and is never written to disk, so the dashboard
adds nothing to yacron2's read-only-root-filesystem deployment story and resets
cleanly on restart.

## See also

- [HTTP Control API](HTTP-API): the REST endpoints, configuration schema, authentication, and Unix-socket options the dashboard is built on.
- [Clustering and Leader Election](Clustering-and-Leader-Election): the cluster panel, per-job `clusterPolicy`, and the `GET /cluster` view it polls.
- [Output Capturing](Output-Capturing): `captureStdout` / `captureStderr`, which control what the Logs tab can show.
- [Schedules and Timezones](Schedules-and-Timezones): the schedule strings and timezones the Schedule tab explains and previews.
- [Production and Container Deployment](Production-Deployment): running the interface under a hardened, read-only-root-filesystem deployment.
