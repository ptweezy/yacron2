# Job-Set ID

The **job-set id** is a deterministic, order-independent fingerprint of the set
of jobs a cronstable instance is running: two instances produce the *same* id if
and only if they hold the same set of jobs. It exists so that several replicas
deployed from one configuration can confirm they are running the same thing, or
detect that one has drifted from the others; it is the agreement key for
[cluster peer attestation](Clustering-and-Leader-Election). It is useful
without clustering too: print it in a deploy script, compare it across a fleet
via metrics, or read it off the dashboard header. The fingerprint is computed
in `cronstable/fingerprint.py`.

An id is a `v1:` scheme prefix plus 64 hex characters:

```text
v1:b834d7565aee0da50cd017f666651a5ba3b2e6b161daf0cb6e430f23f51ce90b
```

**On this page:**
[What the id covers](#what-the-id-covers) ·
[No secret material](#no-secret-material) ·
[Scheme versioning and platform caveats](#scheme-versioning-and-platform-caveats) ·
[Where it surfaces](#where-it-surfaces) ·
[The per-job digest](#the-per-job-digest)

## What the id covers

The id is taken over the **effective (post-merge) configuration** of every job,
not the raw YAML text, which gives it these properties:

* **Independent of job order**, of how the jobs are split across
  [included files](Includes-and-Defaults), and of whether a setting was written
  inline on each job or hoisted into a `defaults` block: the fingerprint sees
  only the merged result. Numeric spellings are normalised too, so an inline
  `killTimeout: 30` and the same value inherited from defaults fingerprint
  identically even where one parses as a float.
* **Equivalent schedule spellings match.** The `minute:`/`hour:` object form
  collapses to the same crontab line as the equivalent string form, and runs of
  whitespace in a string schedule are collapsed, so a reformatted crontab line
  fingerprints the same (see
  [Schedules and Timezones](Schedules-and-Timezones)).
* **`user` / `group` are fingerprinted as configured** (e.g. `www-data`), not
  as the resolved numeric uid/gid, which can differ host to host.
* **It covers every behaviour-affecting field**, so any meaningful change to a
  job changes the id. Exactly: `name`; `command` (a shell string and an argv
  list are kept distinct); the normalised schedule; `shell`;
  `concurrencyPolicy`; `clusterPolicy`; `captureStdout` / `captureStderr`;
  `streamPrefix`; `saveLimit`; `maxLineLength`; the effective scheduling
  timezone (the resolved `utc` / `timezone` frame, so behaviourally identical
  spellings match); `enabled`; `onlyIfLastSucceeded`; `failsWhen`;
  `onFailure` / `onPermanentFailure` / `onSuccess` (including the retry and
  reporting policy, with secret values redacted); the sorted *names* of
  `environment` variables; `executionTimeout`; `killTimeout`; `statsd`;
  `user`; `group`; and `concurrencyScope` when set to `cluster`.

Deliberately **not** part of the identity: the catch-up trio (`onMissed`,
`startingDeadlineSeconds`, `catchupJitterSeconds`) and the archival pair
(`archiveOutput`, `redactArchivedSecrets`), which are restart-time or
observability-only, node-local behaviour; environment variable *values* and
inline secret values (next section); and everything outside the job
definitions, in particular the `cluster` section itself (the peer list,
`distribution`, and the rest of the coordination config never move the id).

Fields added after the `v1` scheme shipped enter the identity only when they
are set away from their default (`concurrencyScope` above is one), so
upgrading cronstable never changes the id of an existing configuration.

## No secret material

The id is logged at startup and served on a possibly-unauthenticated HTTP
endpoint, so it must never embed secret material:

* **Inline reporting secrets are redacted.** A Sentry DSN, mail password, or
  webhook URL or header value written as a literal `value:` is replaced by a
  placeholder before hashing. *Whether* and *how* a secret is configured
  (`value` / `fromFile` / `fromEnvVar`, and the file path or env-var name) is
  still part of the identity; the literal secret never is.
* **Only the names of `environment` variables are hashed**, never their
  values: env is a common place to carry secrets, and a per-host value (e.g.
  from `env_file`) would otherwise make identical configs fingerprint
  differently across hosts. The name set *does* include names contributed by
  `env_file`, so replicas must ship env files with the same variable names
  (only the values may differ per host).

Two consequences: the id is **safe to log and serve**, and **rotating a secret
or changing an env value does not change it**.

## Scheme versioning and platform caveats

The scheme is versioned: the `v1:` prefix is part of the emitted id and folded
into the hash, so ids are only ever comparable within one scheme version. A
cluster peer reporting a different scheme is marked `drifted` immediately, with
no debounce, since such ids are not comparable (see
[per-peer status](Clustering-and-Leader-Election#per-peer-status)).

Because the fingerprint is over *effective* config, it also reflects
platform-dependent defaults: the default `shell` is `/bin/sh` on POSIX and
`cmd.exe` on Windows, so the same YAML fingerprints differently across
platforms unless `shell` is set explicitly. Compare instances running on the
same platform, which replicas are.

## Where it surfaces

* **CLI**: `cronstable --job-set-id` prints the id to stdout and exits, handy in
  scripts and deploy checks; see the
  [Command-Line Reference](CLI-Reference).

  ```shell
  $ cronstable -c /etc/cronstable.d --job-set-id
  v1:b834d7565aee0da50cd017f666651a5ba3b2e6b161daf0cb6e430f23f51ce90b
  ```

* **HTTP**: [`GET /job-set-id`](HTTP-API#get-job-set-id) returns it as
  `text/plain`, or as a JSON object that also carries the job count under
  `Accept: application/json`.
* **Logs**: `Job set id: v1:… (N jobs)` is logged at `INFO` once at startup,
  and again whenever a config reload changes the id.
* **Web dashboard**: the header shows a chip with the first 12 hex characters
  (`#b834d7565aee`); its tooltip carries the full id, and a click copies it
  (see [Web Dashboard](Web-Dashboard)).
* **Terminal dashboard**: the header bar shows the first characters of the id,
  and the command palette has a "Copy job set id" action (see
  [Terminal Dashboard](Terminal-Dashboard)).
* **Metrics**: the `cronstable_job_set_info{job_set_id}` info gauge; compare it
  across a fleet to spot config drift (see
  [Metrics with Prometheus](Metrics-with-Prometheus)).
* **Cluster**: the id is the `job_set_id` field on `GET /cluster` and the
  agreement key exchanged on the mTLS `/peer` endpoint; on the lease backends
  the persisted `@reboot` "already ran" records are scoped to the current id,
  so a changed job set re-arms leader-gated `@reboot` one-shots. The full
  treatment of what the cluster does with the id is in
  [Clustering and Leader Election](Clustering-and-Leader-Election#the-job-set-id-foundation).

## The per-job digest

The set id is built from **per-job digests**: each job's canonical identity is
serialised to a canonical JSON form and SHA-256 hashed, the per-job digests
are sorted (neutralising order), and the sorted list is hashed together with
the scheme version to yield the final `v1:<64 hex>` id. An empty job set
yields a stable, well-defined id.

The per-job digest is also used on its own: [durable state](Durable-State)
records that must not outlive a job's definition (restart-surviving retry
ladders, `@reboot` markers, in-flight run records) are stamped with the owning
job's digest and invalidated when *that job's* behaviour-affecting config
changes. That is stricter than whole-set invalidation, which would drop every
pending record whenever any job in the set changed.

## See also

- [Clustering and Leader Election](Clustering-and-Leader-Election): what the cluster does with the id (attestation, drift, election).
- [HTTP Control API](HTTP-API): the `GET /job-set-id` endpoint.
- [Command-Line Reference](CLI-Reference): the `--job-set-id` flag.
- [Metrics with Prometheus](Metrics-with-Prometheus): the `cronstable_job_set_info` gauge.
- [Includes and Defaults](Includes-and-Defaults): the merge that produces the effective config the id is taken over.
