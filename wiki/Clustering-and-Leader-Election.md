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

**Use an odd cluster size.** With per-node availability `p`, the chance a firing
runs is the probability a majority is up: roughly `3p² − 2p³` for 3 nodes,
higher for 5.

| Size | Tolerates | Notes |
| --- | --- | --- |
| 1 | 0 failures | Degenerate "cluster"; always leads itself. Equivalent to a plain single instance. |
| **3** | **1 failure** | Recommended minimum for HA (~4 nines of "runs" at `p = 0.99`). |
| **5** | **2 failures** | More headroom (~5 nines). |
| 2 | n/a | **Rejected** with `electLeader` (see below). |
| 4, 6, … (even) | same as size−1 | Allowed but **warned**: an even size tolerates no more failures than the odd size below it; the extra node only adds something that can fail. |

* **2 nodes is worse than 1.** A quorum of 2 is 2, so *both* must be up for
  *either* to run: lower availability than a single replica, with no failover
  upside. yacron2 **refuses to start** with `electLeader` and a 2-node cluster,
  raising a `ConfigError` ("...strictly worse than a single replica..."). The
  same 2-node cluster is fine for attestation-only (without `electLeader`).
* Spread the nodes across **independent failure domains**. Correlated failures
  (a bad config push, a shared host/zone) defeat the quorum math regardless of
  `N`.

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
  "leader": "node-a",          // null when this node is not quorate
  "is_leader": true,
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

The same view is rendered as a **cluster panel** in the
[Web Dashboard](Web-Dashboard): a status dot per peer, the agreement tally, and
(when election is on) the quorum count and this node's role (leader, follower,
or "no quorum").

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

## See also

- [Configuration Reference](Configuration-Reference) — the `cluster` section and `clusterPolicy` option schema.
- [HTTP Control API](HTTP-API) — the `GET /cluster` endpoint.
- [Web Dashboard](Web-Dashboard) — the cluster panel and per-job policy display.
- [Production and Container Deployment](Production-Deployment) — running multiple replicas under Kubernetes.
- [Architecture and Internals](Architecture-and-Internals) — where `cluster.py` fits in the daemon.
