# Clustering and Leader Election

By default yacron2 holds its schedule in-process and keeps no shared state, so
two instances started from the same configuration each run **every** job
independently. That is the safe single-instance model, but it means you cannot
simply scale to two replicas for availability without double-running every job.

The optional **`cluster`** section closes that gap. It lets a static set of
instances attest, over mutual TLS, that they are running the *same* job set, and,
when you opt in, turns that attestation into a **quorum-gated leader
election** so that several replicas deployed from one config run with only the
elected leader firing scheduled jobs. It builds directly on the
[job-set id](#the-job-set-id-foundation) and is implemented in
`yacron2/cluster.py` (the `ClusterManager`, `ClusterView`, and the pure
`elect_leader`/`quorum_size` functions).

*New in version 1.2.0.*

> **This is best-effort coordination, not fenced exactly-once.** It keeps no
> shared state, so it is simple to operate and cannot wedge on a missing
> consensus store. The trade-off is that there are narrow windows where a
> firing may be skipped or (under some policies) double-run. If you need a hard
> exactly-once guarantee, use a lease/consensus store (etcd, a Kubernetes
> `Lease`) instead. See [Guarantees and trade-offs](#guarantees-and-trade-offs).

## At a glance

| | Single instance (default) | `cluster` only | `cluster` + `electLeader` |
| --- | --- | --- | --- |
| Replicas | 1 | many (each runs everything) | many (leader runs scheduled jobs) |
| Coordination | none | observe-only attestation | quorum-gated election |
| mTLS identity required | no | yes | yes |
| Endpoint | none | `GET /cluster`, `GET /peer` | `GET /cluster`, `GET /peer` |
| Double-running | n/a | yes (by design) | no, for `Leader` jobs in a quorum |

## The job-set id foundation

A **job-set id** is an order-independent fingerprint of the set of jobs an
instance is running: two instances produce the *same* id if and only if they
hold the same set of jobs. It is taken over the *effective* (post-merge)
configuration of every job, normalises equivalent schedule spellings,
fingerprints `user`/`group` as configured (not as a resolved uid/gid), and
embeds no secret material. The scheme is versioned with a `v1:` prefix so ids
are only ever compared within one scheme.

The id is what the cluster compares: agreement means "we are running the same
jobs". It is available on the standalone [`GET /job-set-id`](HTTP-API) endpoint,
in the dashboard header, and is logged at startup and whenever a reload changes
it. `clusterPolicy` (below) is part of the id, so two replicas that disagree on
a job's policy show up as drift rather than silently coordinating differently.
The full treatment of the job-set id lives in the
[project README](https://github.com/ptweezy/yacron2#job-set-id).

## Cluster peer attestation

With a `cluster` section but **without** `electLeader`, the cluster is
*observe-only*: every instance still runs every job, and attestation just tells
you whether the peers agree. Each node serves a tiny `GET /peer` endpoint on a
dedicated mTLS listener and periodically polls every configured peer, comparing
job-set ids.

```yaml
cluster:
  listen: "0.0.0.0:8443"                  # the mTLS listener for this node
  tls:
    ca:   /etc/yacron2/cluster-ca.pem     # trust anchor for peer certificates
    cert: /etc/yacron2/this-node.pem      # this node's certificate
    key:  /etc/yacron2/this-node.key
  peers:
    - host: yacron-b.internal:8443
    - host: yacron-c.internal:8443
  nodeName: node-a                        # optional; defaults to the system hostname
  interval: 30                            # optional; seconds per round (default 30)
  driftAfter: 3                           # optional; rounds before "drifted" (default 3)
  connectTimeout: 10                      # optional; seconds per peer request (default 10)
  electLeader: false                      # optional; run jobs on the leader only (see below)
```

The trust model is deliberately small and keeps no shared state:

* **mTLS is the membership boundary.** A peer's certificate must chain to the
  configured `ca`, and (client side) match the host it was reached at, so only
  nodes the CA vouches for are ever attested. Standard TLS hostname verification
  provides that SAN pinning: the cert presented by `yacron-b.internal:8443`
  must carry `yacron-b.internal` as a Subject Alternative Name. Provision the
  certificates with your own PKI (cert-manager, a service mesh, an internal CA);
  yacron2 only consumes them. The same per-node cert/key is used both to serve
  `/peer` and to authenticate as a client when polling peers.
* **Each node keeps its own view.** No node is authoritative: two healthy nodes
  converge to the same picture, and any disagreement is itself the signal.
* **Drift is debounced.** A reachable peer whose id differs is only reported as
  `drifted` after `driftAfter` consecutive rounds, so a rolling deploy (a brief,
  legitimate mismatch) does not raise a false alarm.

### Per-peer status

Every peer in the [`GET /cluster`](#observing-the-cluster) view and the
dashboard panel carries one of these statuses (the constants live in
`yacron2/cluster.py`):

| Status | Meaning |
| --- | --- |
| `agreed` | Reachable over mTLS and reporting the same job-set id. |
| `syncing` | Reachable, but its id differs and the mismatch has not yet persisted for `driftAfter` rounds (a transient/rolling-deploy mismatch). |
| `drifted` | Reachable, but its id has differed for `driftAfter` consecutive rounds (an actual disagreement). Also used immediately (no debounce) when the peer reports a different fingerprint **scheme** (`v1:` vs another), since such ids are not comparable. |
| `unreachable` | Connect/timeout/`OSError`: the peer could not be contacted this round. |
| `untrusted` | TLS/certificate verification failed: the peer is not (or not provably) a cluster member. |
| `self` | The peer reported *this* node's own `nodeName` (an operator listed this node's own address in its peer list). It never counts toward agreement. |
| `unknown` | Not yet contacted (the initial state before the first poll). |

A peer reported as `unreachable` or `untrusted` resets its drift streak, because
the streak only counts *reachable-but-mismatched* rounds.

The `/peer` endpoint is served **only** on the separate mTLS `listen` address,
never on the public [web API](HTTP-API). It returns a small JSON document:
`{"node_name": ..., "job_set_id": ..., "scheme_version": "v1"}`.

## Leader election

Setting `electLeader: true` turns the same attestation into a **quorum-gated
leader election**, so you can run more than one replica from the same config
without double-running jobs:

```yaml
cluster:
  listen: "0.0.0.0:8443"
  tls:
    ca:   /etc/yacron2/cluster-ca.pem
    cert: /etc/yacron2/this-node.pem
    key:  /etc/yacron2/this-node.key
  peers:
    - host: yacron-b.internal:8443
    - host: yacron-c.internal:8443
  electLeader: true
```

Each node independently elects, as leader, the **lowest `nodeName`** among the
members it currently sees *agreeing* on the job-set id, but **only if that set
is a quorum** (a strict majority) of the cluster. **Only the leader runs
*scheduled* jobs.** Manual runs via the API (`POST /jobs/{name}/start`) and
automatic retries are deliberately *not* gated, so you can still trigger a job
on any node.

### Cluster size and quorum

* **List every *other* member in `peers`**, not this node itself. The cluster
  size is therefore `len(peers) + 1`, and the quorum is `⌊size / 2⌋ + 1`. The
  peer lists must be consistent across nodes for every node to compute the same
  size and quorum.
* If you accidentally list a node's own address in its own peer list, that entry
  is reported as `self` and never counts toward agreement, so it can only make
  quorum *harder* to reach, never easier.
* The computed size, quorum, elected leader, and whether this node is the leader
  are all shown at `GET /cluster` and in the dashboard panel.

### Why the quorum gate is safe

The quorum gate is what makes this safe with **no shared state**. Two strict
majorities of `N` cannot be disjoint, so under a clean network partition at most
one side is quorate, and therefore **at most one leader exists**. The price is
liveness: a node that cannot see a majority deliberately **stands down** (runs
nothing) rather than risk a second leader. A `Leader` job therefore runs on a
given firing only while a majority of the cluster is up and mutually reachable.

### Sizing the cluster

A `Leader` job fires successfully only while a quorum is up and mutually
reachable. If each node is independently up with probability `p`, and the quorum
is `q = ⌊N/2⌋ + 1`, then the chance a given firing runs is the probability that
**at least `q` of `N` nodes are up**, which is a binomial tail:

```text
P(runs) = Σ (from k=q to N)  C(N, k) · p^k · (1 − p)^(N − k)
```

The table below evaluates that for a few realistic per-node availabilities, as a
fraction and as "nines" (`−log₁₀(1 − P)`). "Tol." is how many simultaneous node
failures the size survives (`N − q`).

| N | Quorum | Tol. | P(runs), p=0.9 | p=0.99 | p=0.999 |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 | 0 | 0.9000 (1.0 nines) | 0.9900 (2.0) | 0.99900 (3.0) |
| 2 | 2 | 0 | 0.8100 (0.7) | 0.9801 (1.7) | 0.99800 (2.7) |
| **3** | 2 | **1** | 0.9720 (1.6) | 0.99970 (3.5) | 0.9999970 (5.5) |
| 4 | 3 | 1 | 0.9477 (1.3) | 0.99941 (3.2) | 0.9999940 (5.2) |
| **5** | 3 | **2** | 0.9914 (2.1) | 0.999990 (5.0) | ≈1 (8.0) |
| 7 | 4 | 3 | 0.9973 (2.6) | ≈1 (6.5) | ≈1 (10.5) |

How to read it:

* **Odd sizes are the sweet spot.** Each odd size adds one failure of headroom
  over the previous odd size: 3 tolerates 1, 5 tolerates 2, 7 tolerates 3.
* **Even sizes are equal-or-worse, never better.** N=4 still needs a quorum of
  3, so it tolerates the same single failure as N=3, but it has an extra node
  that can fail, so its P(runs) is actually slightly *lower* (0.99941 vs 0.99970
  at p=0.99). yacron2 warns on even sizes for exactly this reason.
* **2 is worse than 1.** A 2-node quorum is 2, so both must be up: P = p²
  (0.9801 at p=0.99), below a single node's `p` (0.99), with no failover upside.
  yacron2 **refuses to start** with `electLeader` and a 2-node cluster, raising a
  `ConfigError` ("...strictly worse than a single replica..."). The same 2-node
  cluster is fine for attestation-only (without `electLeader`).

The same numbers as expected **skipped firings** for an hourly `Leader` job
(8760 firings/year), which is often the more intuitive framing:

| N | p=0.99 | p=0.999 |
| --- | --- | --- |
| 1 | ≈88 skips/yr | ≈8.8 skips/yr |
| 3 | ≈2.6 skips/yr | ≈0.03 skips/yr |
| 5 | ≈0.09 skips/yr | negligible |

Caveats on the math:

* It assumes **independent** failures. Correlated failures (a bad config push, a
  shared host, zone, or power domain) break that assumption, and then more nodes
  can even hurt. Spread the nodes across independent failure domains; `p` should
  be realistic uptime *including* deploys and restarts, not raw hardware MTBF.
* It only models "is a quorum up". It does *not* capture the narrow
  membership-change windows in [Guarantees and trade-offs](#guarantees-and-trade-offs)
  (a firing may still slip through them), nor `PreferLeader` duplication, which
  is about partitions rather than node-up probability.

### Failure handling

If `electLeader` is configured but the cluster listener fails to start (bad cert
files, a bad listen address, a port already in use), the node **fails closed**:
it logs the error, keeps running, but its `Leader`/`PreferLeader` jobs stay idle
rather than falling back to running everything on every replica. (`EveryNode`
jobs are unaffected; see below.) Leadership transitions are logged each time
the node acquires or loses scheduled-job leadership.

## Per-job policy

The cluster-wide `electLeader` switch sets the *default* behaviour, but each job
can override it with **`clusterPolicy`** to pick its own point on the
liveness-vs-duplication trade-off. **No option is true exactly-once**: each
gives up one side. `Leader` may *skip*, `PreferLeader` may *double-run*.

| `clusterPolicy` | Healthy (quorate) | Partitioned / sub-quorum | Use for |
| --- | --- | --- | --- |
| `Leader` *(default)* | leader runs once | **nobody** runs (skips) | non-idempotent jobs where a duplicate is harmful and an occasional skip is OK (billing, outbound email) |
| `PreferLeader` | lowest node runs once | each side's lowest node runs (**may double-run**) | important **and** idempotent jobs that should never skip |
| `EveryNode` | every node runs | every reachable node runs | genuinely per-node work (local log rotation), or fully idempotent jobs |

```yaml
jobs:
  - name: charge-cards          # must not double-charge; skip-tolerant
    command: ./charge.sh
    schedule: "0 * * * *"
    clusterPolicy: Leader       # the default; can be omitted

  - name: refresh-cache         # idempotent, but must not be skipped
    command: ./refresh.sh
    schedule: "*/5 * * * *"
    clusterPolicy: PreferLeader

  - name: rotate-local-logs     # inherently per-node
    command: ./rotate.sh
    schedule: "@daily"
    clusterPolicy: EveryNode
```

Notes:

* `clusterPolicy` is **inert unless `cluster.electLeader` is on**. Without
  election, every job runs on every instance regardless of its policy.
* `Leader` and `PreferLeader` jobs **fail closed** when election is configured
  but no manager is running (e.g. the listener failed to start). `EveryNode`
  jobs are independent of cluster health, so they keep firing regardless.
* The active policy for each job (when election is on) is shown in the
  dashboard's job drawer and included in the `GET /jobs` payload. To keep the
  per-poll payload lean for the common single-instance case, `clusterPolicy` is
  **omitted** from `GET /jobs` when election is not configured.
* `clusterPolicy` is part of the [job-set id](#the-job-set-id-foundation), so
  replicas that disagree on a job's policy surface as drift.

The decision for one node, one firing, is exactly:

```text
election off  -> run (every node runs everything)
EveryNode     -> run (always, even if the manager failed to start)
no manager    -> skip (fail closed)
PreferLeader  -> run only if this node is the lowest reachable agreeing node
Leader        -> run only if this node is the quorum-gated elected leader
```

(Under `distribution: spread`, described next, the last two lines become "the
*per-job owner* among the reachable agreeing nodes" and "the quorum-gated
*per-job owner*" respectively.)

## Distribution: one leader, or spread the load

By default (`distribution: single-leader`) the single elected leader runs
**every** `Leader` job, so the other replicas are pure standby for scheduled
work. That is the simplest model, but on a busy cluster it makes the leader a
hotspot while the rest idle.

Setting `distribution: spread` keeps the same quorum gate but replaces the one
leader with **per-job ownership**: each leader-gated job is assigned to a single
node by *rendezvous (highest-random-weight) hashing* of the job name against the
agreeing members. Different jobs hash to different nodes, so the scheduled
workload fans out roughly evenly across the cluster.

```yaml
cluster:
  listen: "0.0.0.0:8443"
  tls: { ca: /etc/yacron2/cluster-ca.pem, cert: /etc/yacron2/this-node.pem, key: /etc/yacron2/this-node.key }
  peers:
    - host: yacron-b.internal:8443
    - host: yacron-c.internal:8443
  electLeader: true
  distribution: spread      # default is single-leader
```

What to know:

* **Same safety, not more.** Under a clean partition every quorate node sees the
  same member set and computes the same owner for each job, so still at most one
  node runs it. This is a *load* optimization only; it does not change the
  best-effort guarantee. `Leader` jobs still skip without quorum; `PreferLeader`
  still ignores quorum (its owner is computed over the reachable set, so an
  isolated node owns and runs all of its jobs).
* **Rendezvous hashing, not modulo.** When a node leaves or joins, only *its*
  share of jobs is reassigned (to the next-highest-weight node); the rest stay
  put. A membership change is therefore minimally disruptive, unlike
  `hash % N`, which would reshuffle everything.
* **Best with many or heavy jobs.** Hashing is only *roughly* even, so with a
  handful of jobs the split is lumpy (several can land on one node). It pays off
  when a single node cannot comfortably carry all the scheduled work; for light
  workloads the default single leader is simpler and equally correct.
* **Keep it consistent.** Every node must agree on `distribution` (just like the
  peer list and `electLeader`). A node left on `single-leader` while the others
  run `spread` would run every job itself. `distribution` is *not* part of the
  job-set id (it is cluster config, not a job property), so a mismatch does not
  show up as drift; treat it like `electLeader` and roll it out uniformly. It is
  inert without `electLeader` (and yacron2 warns if you set it anyway).

### Worked example

The bundled [three-node demo](#trying-it-locally) names its two scheduled
leader-gated jobs `tick-leader-only` (`Leader`) and `tick-prefer-leader`
(`PreferLeader`). With all three nodes healthy:

| Job | `single-leader` (default) | `spread` |
| --- | --- | --- |
| `tick-leader-only` | runs on `yacron-a` (the leader) | runs on `yacron-c` |
| `tick-prefer-leader` | runs on `yacron-a` (the leader) | runs on `yacron-b` |

So flipping to `spread` moves the two jobs onto two *different* nodes instead of
piling both onto the leader. The owner is a deterministic function of the job
name and the live member set, so it stays put until membership changes (then
only the affected jobs move). You can confirm it live:

```shell
curl -s http://localhost:8080/jobs | python -m json.tool | grep -A1 clusterPolicy
# each leader-gated job shows a "clusterOwner" naming the node that runs it
```

## Observing the cluster

`GET /cluster` on the [web/HTTP interface](HTTP-API) returns the current view as
JSON. When no `cluster` section is configured it returns
`{"enabled": false, "peers": []}`; otherwise it returns the node's view:

```jsonc
{
  "enabled": true,
  "node_name": "node-a",
  "job_set_id": "v1:…",
  "cluster_size": 3,
  "quorum": 2,
  "elect_leader": true,
  "distribution": "single-leader", // or "spread"
  "quorate": true,                 // whether this node sees a quorum
  "leader": "node-a",              // null when not quorate, or always in spread mode
  "is_leader": true,               // always false in spread mode (no single leader)
  "peers": [
    {"host": "yacron-b.internal:8443", "status": "agreed",
     "node_name": "node-b", "job_set_id": "v1:…",
     "last_seen": "2026-06-23T19:00:00+00:00", "last_error": null,
     "mismatch_streak": 0},
    {"host": "yacron-c.internal:8443", "status": "unreachable",
     "node_name": null, "job_set_id": null,
     "last_seen": null, "last_error": "Cannot connect to host …",
     "mismatch_streak": 0}
  ]
}
```

In `spread` mode there is no single leader, so `leader` is `null` and
`is_leader` is `false`; use `quorate` to tell whether this node is running its
owned jobs. The per-job owners appear as a `clusterOwner` field on each
leader-gated job in [`GET /jobs`](HTTP-API).

The same view is rendered as a **cluster panel** in the
[Web Dashboard](Web-Dashboard): a status dot per peer, the agreement tally, and
(when election is on) the quorum count and this node's role (leader, follower,
"no quorum", or, in spread mode, "spread (per-job owner)").

## Guarantees and trade-offs

This design intentionally keeps **no shared state**, which is what makes it easy
to run, but it means the guarantee is *best-effort*, not fenced exactly-once.
Because each node acts on a view only as fresh as its last poll (`interval`),
there are narrow windows where behaviour degrades:

* **Just after a leader dies**, a `Leader` firing may be *skipped* until the
  survivors notice (up to one `interval`) and re-elect.
* **Asymmetric or flapping reachability** can briefly elect two leaders.
* A `PreferLeader` job **may double-run** across a partition (that is the point
  of the policy: it never skips).

If you need a hard exactly-once guarantee, you need a lease/consensus store
(etcd, a Kubernetes `Lease`), which this design deliberately avoids. If a job
must *never* be skipped or doubled, run a single replica (`replicas: 1`) or use
an external coordinator. Tuning the `interval` shorter narrows the degraded
windows at the cost of more polling traffic.

## Kubernetes

A StatefulSet pairs naturally with this feature: its stable ordinal hostnames
(`yacron2-0`, `yacron2-1`, …) make both the certificate SANs and the peer list
straightforward, and give each pod a stable `nodeName`. Use an **odd**
`replicas` count, spread pods across nodes/zones with
`topologySpreadConstraints`, and provision the per-pod certificates from your own
PKI (e.g. cert-manager). See
[Production and Container Deployment](Production-Deployment) for the deployment
walkthrough.

## Trying it locally

The repository ships a ready-to-run three-node cluster in
[`docker-compose-cluster.yml`](https://github.com/ptweezy/yacron2/blob/develop/docker-compose-cluster.yml).
It generates a throwaway cluster CA and per-node certificates, brings up three
mutually-attesting nodes with `electLeader: true` and one job of each
`clusterPolicy`, and publishes each node's dashboard on a separate port
(8080/8081/8082) so you can watch leadership move when you stop the leader:

```shell
docker compose -f docker-compose-cluster.yml up --build
# then open http://localhost:8080/ , :8081 , :8082 and watch the cluster panel
docker compose -f docker-compose-cluster.yml stop yacron-a   # watch leadership move to node-b
```

The compose file's header comments document the full set of things to try
(losing quorum, drift, the per-policy job behaviour).

### A larger, CPU-heavy cluster

To watch [`distribution: spread`](#distribution-one-leader-or-spread-the-load)
fan real load across the cluster, the repository also ships
[`docker-compose-cluster-large.yml`](https://github.com/ptweezy/yacron2/blob/develop/docker-compose-cluster-large.yml):
**ten** nodes (dashboards on ports 8080–8089) running a larger job set with
several CPU-heavy jobs, defaulting to `spread`. Each node's config is generated
from environment variables by a small entrypoint, so there are no per-node files
to maintain.

```shell
docker compose -f docker-compose-cluster-large.yml up --build
docker stats     # watch CPU spread across the nodes (a few cores busy on several nodes)

# contrast: pin everything to one leader and watch a single node light up
DISTRIBUTION=single-leader docker compose -f docker-compose-cluster-large.yml up -d
```

(Ten is an *even* size, so the nodes log the even-size warning; that is expected
and called out in the file. Quorum is 6, so it tolerates four failures.) Its
header comments list how to inspect per-job owners and fail nodes.

## See also

- [Configuration Reference](Configuration-Reference) — the `cluster` section and `clusterPolicy` option schema.
- [HTTP Control API](HTTP-API) — the `GET /cluster` endpoint.
- [Web Dashboard](Web-Dashboard) — the cluster panel and per-job policy display.
- [Production and Container Deployment](Production-Deployment) — running multiple replicas under Kubernetes.
- [Architecture and Internals](Architecture-and-Internals) — where `cluster.py` fits in the daemon.
