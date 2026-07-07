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

> **The default `gossip` backend is best-effort coordination, not fenced
> exactly-once.** It keeps no shared state, so it is simple to operate and
> cannot wedge on a missing consensus store. The trade-off is that there are
> narrow windows where a firing may be skipped or (under some policies)
> double-run. If you need a hard exactly-once guarantee **and** already run a
> coordination store, set `cluster.backend: kubernetes` or `etcd` (below) to
> elect through a `Lease` / a lease-bound key instead; if the nodes already
> share a POSIX mount, `cluster.backend: filesystem` elects through a fenced
> lease file on the mount itself (fenced under NTP-bounded clock skew), with
> no extra service at all. See
> [Choosing a backend](#choosing-a-backend) and
> [Guarantees and trade-offs](#guarantees-and-trade-offs).

**Terms used on this page.** A **job-set id** is an order-independent
fingerprint of the jobs a node runs (two nodes match iff they hold the same job
set). A **quorum** is a strict majority of the cluster, `⌊N / 2⌋ + 1` nodes. A
node is **quorate** when it currently sees a quorum of agreeing members.
**Fenced** means a shared store guarantees a single holder (the lease backends).
A **lease** is a short-lived, auto-expiring claim on that store that the holder
keeps renewing. A **bridge** is a set of members that two mutually-unreachable
nodes can both still reach (the sides see the bridge, not each other); a
**thin bridge** is one of fewer than `quorum - 1` shared members, too thin for
the two sides to confirm each other through it (see
[Guarantees and trade-offs](#guarantees-and-trade-offs)).

**On this page:**
[Quickstart](#quickstart-a-minimal-3-node-cluster) ·
[From one node to a cluster](#from-one-node-to-a-cluster) ·
[Choosing a backend](#choosing-a-backend) ·
[At a glance](#at-a-glance) ·
[The job-set id foundation](#the-job-set-id-foundation) ·
[Cluster peer attestation](#cluster-peer-attestation) ·
[Leader election](#leader-election) ·
[Per-job policy](#per-job-policy) ·
[Distribution](#distribution-one-leader-or-spread-the-load) ·
[Observing the cluster](#observing-the-cluster) ·
[Guarantees and trade-offs](#guarantees-and-trade-offs) ·
[Certificate rotation](#certificate-rotation) ·
[Operating the lease backends](#operating-the-lease-backends-kubernetes-and-etcd) ·
[Trying it locally](#trying-it-locally) ·
[Cluster sizing math](#appendix-cluster-sizing-math)

## Quickstart: a minimal 3-node cluster

The fastest way to see a leader-electing cluster is the bundled three-node demo,
which mints a throwaway CA and per-node certs for you:

```shell
docker compose -f docker-compose-cluster.yml up --build
# then open http://localhost:8080/ , :8081 , :8082 and watch the cluster panel
```

See [Trying it locally](#trying-it-locally) for the fuller walkthrough (failing
the leader, losing quorum, drift, per-policy behaviour).

To build one by hand, each node gets the same job set plus a per-node `cluster`
block. This is a complete leader-electing gossip node (`yacron-a` of three):

```yaml
cluster:
  listen: "0.0.0.0:8443"                  # this node's mTLS /peer listener
  tls:
    ca:   /etc/yacron2/cluster-ca.pem     # shared cluster CA (trust anchor)
    cert: /etc/yacron2/yacron-a.pem       # this node's cert (SAN = yacron-a)
    key:  /etc/yacron2/yacron-a.key
  peers:                                  # every OTHER member -> size = 3
    - host: yacron-b.internal:8443
    - host: yacron-c.internal:8443
  nodeName: yacron-a                      # unique, stable per node
  electLeader: true                       # only the elected leader runs jobs
```

(The certificate paths are yours to choose; the bundled compose demo, for
example, mounts its generated certs at `/certs/ca.pem` / `/certs/yacron-a.pem`.)

**The quorum rule in one paragraph.** List every *other* node in `peers` (never
this node itself), so the cluster size is `len(peers) + 1` and every node
computes the same `N`. A node acts as leader only while it sees a **quorum**, a
strict majority (`⌊N / 2⌋ + 1`), of agreeing members. So a 3-node cluster
tolerates one node down; give each node a distinct `nodeName` and a matching
peer list.

Most clusters want the **default single leader**: enable
[`distribution: spread`](#distribution-one-leader-or-spread-the-load) only when
one leader cannot carry all the scheduled work.

**On Kubernetes**, skip the certs and peer list entirely and set
`cluster.backend: kubernetes` so a `Lease` fences leadership instead; nodes
that already share a POSIX mount can likewise skip them with
`cluster.backend: filesystem` and elect through a lease file on the mount (see
[Choosing a backend](#choosing-a-backend)).

## From one node to a cluster

To grow a single instance into a cluster without a double-run flag day, add
attestation first, verify it is healthy, then turn on election:

1. **Pick a unique, stable `nodeName`** per replica (the orchestrator's stable
   hostname, a StatefulSet ordinal, or an explicit value). Reusing one across
   nodes silently double-runs; see [Unique node names](#unique-node-names).
2. **Provision the coordination material.** For `gossip`, issue per-node
   certs from a dedicated cluster CA (see
   [Cluster peer attestation](#cluster-peer-attestation)); for a lease backend,
   set up the `Lease` RBAC or etcd credentials, or point
   `cluster.filesystem.path` at the shared mount
   ([Operating the lease backends](#operating-the-lease-backends-kubernetes-and-etcd)).
3. **Add the `cluster` block with `electLeader: false` first** (attestation
   only). Every replica still runs every job, so nothing changes operationally,
   and you can confirm the peers reach `agreed` on `GET /cluster` before trusting
   the topology.
4. **Run `yacron2 --validate-config`** to catch a bad peer list, TLS paths, or
   lease ordering at rest; see the [Command-Line Reference](CLI-Reference).
5. **Roll the replicas one at a time**, letting each converge to `agreed` before
   the next (change membership incrementally so majorities always overlap; see
   [Consistent cluster size](#consistent-cluster-size)).
6. **Set `electLeader: true`.** Enabling election only once attestation is
   already healthy means the switch itself is a clean transition, not a flag day.

### Reverting to a single instance

To collapse back to one instance, scale to `replicas: 1`, or set
`electLeader: false` (keep attestation) or remove the `cluster` block entirely.
The `gossip` backend keeps no shared state, so there is nothing to clean up; a
lease backend releases its lease on a graceful stop, so a survivor (if any) takes
over at once.

## Choosing a backend

`cluster.backend` selects how leadership is decided. **The decision rule:** stay
on the default `gossip` when you want zero-dependency replicas and can tolerate
an occasional skip or double-run in narrow windows; pick `kubernetes` (already on
Kubernetes) or `etcd` (already run etcd) when you need a **fenced, exactly-once**
guarantee and already run that store; pick `filesystem` when the nodes already
share a POSIX mount (Amazon S3 Files / EFS / NFS) and you want fenced
leadership with zero extra services. All four present the same **per-job** seam
(`clusterPolicy`) to the scheduler, so switching backends does not change how
jobs are written; only the *coordination* underneath, and therefore how the
cluster is **observed**, differs.

| | `gossip` *(default)* | `kubernetes` | `etcd` | `filesystem` |
| --- | --- | --- | --- | --- |
| Coordination | embedded mTLS gossip, no shared state | a `coordination.k8s.io/v1` `Lease` | a lease-bound etcd key | a flock-guarded TTL lease file on a shared POSIX mount |
| Guarantee | best-effort (may skip or double-run in narrow windows) | **fenced, exactly-once** while the apiserver is reachable | **fenced, exactly-once** while etcd is reachable | **fenced** under NTP-bounded clock skew (~2 s budget) while the mount is reachable |
| Extra dependency | none | none (optional `yacron2[kubernetes]`) | none | none |
| Needs | per-node mTLS certs + a static peer list | in-cluster (or kubeconfig) apiserver access + a Lease RBAC | reachable etcd endpoint(s) | a shared POSIX mount (S3 Files / EFS / NFSv4) with real cross-host locks, NTP on every node |
| Best when | zero-dependency replicas, occasional skip/dup tolerable | already on Kubernetes and want a hard guarantee | already run etcd | you already have a shared mount and want fenced leadership with zero extra services |

The per-backend config keys (`cluster.kubernetes.*`, `cluster.etcd.*`,
`cluster.filesystem.*`) are in the
[Configuration Reference](Configuration-Reference#cluster); deployment, RBAC,
auth/TLS, failure modes, and monitoring are in
[Operating the lease backends](#operating-the-lease-backends-kubernetes-and-etcd).
Runnable samples are in
[`example/kubernetes/`](https://github.com/ptweezy/yacron2/tree/develop/example/kubernetes)
and [`example/etcd/`](https://github.com/ptweezy/yacron2/tree/develop/example/etcd).

The rest of this page documents the **`gossip`** backend (the default) in
depth; its trust model and quorum math are specific to it. The `clusterPolicy`
semantics in [Per-job policy](#per-job-policy), however, apply to every backend.

## At a glance

| | Single instance (default) | `cluster` only | `cluster` + `electLeader` |
| --- | --- | --- | --- |
| Replicas | 1 | many (each runs everything) | many (leader runs scheduled jobs) |
| Coordination | none | observe-only attestation | quorum-gated election (`gossip`) or a fenced lease (`kubernetes` / `etcd` / `filesystem`) |
| mTLS identity required | no | yes | yes on `gossip` (a lease backend needs none) |
| Endpoint | none | `GET /cluster`, `GET /peer` | `GET /cluster` (plus `GET /peer` on `gossip`) |
| Double-running | n/a | yes (by design) | no for `Leader` jobs in a converged, fully-connected quorum (best-effort: a thin bridge, a same-`N` membership change, or the ~one-`interval` window after a partition can still let two nodes both lead; see [Guarantees and trade-offs](#guarantees-and-trade-offs)) |

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
  nodeName: yacron-a                      # optional; defaults to the system hostname
  interval: 30                            # optional; seconds per round (default 30)
  driftAfter: 3                           # optional; rounds before "drifted" (default 3)
  connectTimeout: 10                      # optional; seconds per peer request (default 10)
  electLeader: false                      # optional; run jobs on the leader only (see below)
```

Run `yacron2 --validate-config` before deploying to catch a bad `cluster`
section (peer list, TLS paths, lease ordering) at rest rather than at startup;
see the [Command-Line Reference](CLI-Reference).

The trust model is deliberately small and keeps no shared state:

* **mTLS is the membership boundary.** A peer's certificate must chain to the
  configured `ca`, and (client side) match the host it was reached at, so only
  nodes the CA vouches for are ever attested. Standard TLS hostname verification
  provides that SAN pinning: the cert presented by `yacron-b.internal:8443`
  must carry `yacron-b.internal` as a Subject Alternative Name. The CA is the
  *whole* authentication boundary (yacron2 trusts any cert it signs to assert
  its identity and gossip state), so it **must** be a dedicated, closed CA
  issued only to yacron2 nodes, **not** a shared service-mesh or
  organisation-wide CA (any cert that CA admits can otherwise fabricate the
  `/peer` payload below: fake agreement, trip the conflict gate, or suppress an
  `@reboot` job). Provision the certificates from your own dedicated PKI (a
  private cert-manager issuer, an internal CA); yacron2 only consumes them. The
  same per-node cert/key is used both to serve `/peer` and to authenticate as a
  client when polling peers. An **in-place
  renewal** of these files (same paths, new bytes) is detected and applied
  automatically, with no restart (see [Certificate rotation](#certificate-rotation)).
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
| `self` | The peer reported *this* node's own `nodeName` **and** its own instance id (an operator listed this node's own address in its peer list). It never counts toward agreement. |
| `conflict` | The peer reported this node's `nodeName` but a *different* instance id: a **duplicate `nodeName`** (two nodes sharing a name). It never counts toward agreement, and while any conflict is visible `Leader` jobs fail closed. See [Unique node names](#unique-node-names). |
| `unknown` | Not yet contacted (the initial state before the first poll). |

A failed round (`unreachable` or `untrusted`) neither advances nor resets the
drift streak: the streak counts *reachable-but-mismatched* rounds, and only a
confirmed agreement (an `agreed` round, or the benign `self` case) clears it,
so a genuinely drifting peer cannot postpone its `drifted` label by flapping in
and out of reach.

The `/peer` endpoint is served **only** on the separate mTLS `listen` address,
never on the public [web API](HTTP-API). It returns a small JSON document with
everything a polling peer needs: the reporter's `node_name` and `job_set_id`
(the agreement key), a per-process `instance_id` (so a duplicate `nodeName` is
distinguishable from a self-listing), the declared `cluster_size` and the
`distribution` / `elect_leader` descriptors (the conflict gates), plus its own
`members` view, `mutual_agreeing` / `quorate_vouched` sets (bridge discovery and
`spread` owner deferral), and `ran_reboot_jobs` (deferred-`@reboot`
de-duplication).

The full annotated payload, the per-field safety role, and the trust-model notes
(any CA-admitted peer can read and could fabricate the whole member and
agreement graph, so the cluster CA must be a dedicated, closed boundary, and the
listener caps request size but not concurrent connections) are in
[Architecture and Internals](Architecture-and-Internals#the-peer-attestation-payload).

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
*scheduled* jobs.** Manual runs via the API (`POST /jobs/{name}/start`) are
deliberately *not* gated, so you can still trigger a job on any node. Automatic
*retries* re-check the gate before every relaunch: a transient fail-closed
denial (lost quorum, a detected conflict, a rebuilt manager's still-converging
view) merely defers the retry and re-checks it, while a *positively observed*
ownership move ends the local ladder so it cannot double-run against the new
owner. What happens to the pending attempt then depends on the state store: on
a **shared** [durable state](Durable-State#restart-surviving-retries) store
with leader election, the ladder is **handed off** rather than dropped -- the
old owner writes a durable `handoff` record instead of settling the ladder
dead (no `cancelled` run-history record: the attempt moves, it does not die)
and the new owner resumes the remaining attempts from it. Without a shared
store the retry is **abandoned** (a WARNING plus a `cancelled` run-history
record). `EveryNode` and `@reboot` ladders never move between nodes. The full
defer-vs-abandon-vs-handoff lifecycle is documented in
[Failure Detection and Retries](Failure-Detection-and-Retries#retry-lifecycle).

### Cluster size and quorum

* **List every *other* member in `peers`**, not this node itself. The cluster
  size is therefore `len(peers) + 1`, and the quorum is `⌊size / 2⌋ + 1`. The
  peer lists must be consistent across nodes for every node to compute the same
  size and quorum. This is [enforced at runtime](#consistent-cluster-size),
  not merely assumed.
* If you accidentally list a node's own address in its own peer list, an entry
  the config load can prove is local (a loopback address on this node's port
  under a matching listen) is rejected or warned about up front; anything else
  (e.g. the node's own routable IP) is recognised at runtime as `self` once its
  self-poll succeeds, never counts toward agreement, and is **excluded from the
  cluster size**. In a genuinely 3+-node cluster that is benign (logged once at
  INFO): it neither changes the effective `N`/quorum nor (since `N` stays equal
  to what other nodes declare) trips the size-consistency check below. It is
  **not** harmless at the boundary: a self-padded "3-node" config that is
  really 2 nodes sails past the `electLeader` 2-node refusal and runs as the
  degenerate quorum-2-of-2 cluster (both nodes must be up; any single failure
  stops all `Leader` jobs cluster-wide), which yacron2 flags at runtime with a
  prominent WARNING. Remove the self entry rather than relying on the
  exclusion.
* The computed size, quorum, elected leader, and whether this node is the leader
  are all shown at `GET /cluster` and in the dashboard panel.

### Why the quorum gate is safe

The quorum gate is what makes this safe with **no shared state**. Two strict
majorities of `N` cannot be disjoint, so under a clean network partition at most
one side is quorate, and therefore (within about one poll `interval`) **at
most one leader exists**. (That qualifier matters: a leader just cut off from
the majority keeps acting on its last, now-stale view until its *own* next poll,
so for up to one `interval` a clean partition can briefly **double-run** a
`Leader` firing rather than only skip one; the single-leader property reasserts
once the cut-off node re-polls and stands down. See
[Guarantees and trade-offs](#guarantees-and-trade-offs).) The price is
liveness: a node that cannot see a majority deliberately **stands down** (runs
nothing) rather than risk a second leader. A `Leader` job therefore runs on a
given firing only while a majority of the cluster is up and mutually reachable.

### Unique node names

The safety argument above assumes every node has a **distinct `nodeName`**. If
two nodes shared one, each would compute itself as the lowest name in its live
set and *both* would elect themselves: a silent double-run, exactly what
election is meant to prevent. So `nodeName` uniqueness is a correctness
requirement, not just a nicety.

yacron2 enforces it at runtime. Each process mints a random **instance id** at
startup and reports it on `/peer` alongside its `nodeName`. That lets a node
distinguish two cases that otherwise look identical:

* a **benign self-listing** (an operator put this node's own address in its
  own peer list), where the peer reports *this* node's name *and* its own
  instance id (status `self`); from
* a **duplicate `nodeName`** (a *different* process announcing this node's
  name), where the instance id differs (status `conflict`). A third node can
  likewise spot two distinct instances claiming one name.

> **This detection is best-effort.** It relies on some node being able to see
> both copies, directly, or transitively by unioning peers' reported member
> lists (one hop). Two copies of a duplicated `nodeName` that sit in **disjoint
> partitions** (no single node can observe both, even transitively) cannot be
> reconciled, so each side stays quorate and **both lead**, the same residual
> class as a same-`N` membership swap (see
> [Consistent cluster size](#consistent-cluster-size)). So treat unique
> `nodeName`s as something to **enforce** (distinct cert SANs, the orchestrator's
> stable hostnames), not merely to detect at runtime.

While any `conflict` is visible, this node's **`Leader` jobs fail closed**
(stand down) instead of risking a double-run, and the conflict is surfaced as a
`conflict` flag on [`GET /cluster`](#observing-the-cluster), a banner in the
dashboard cluster panel, and an `ERROR` log line. It clears automatically once
the duplicate is renamed: the gate is self-healing. `PreferLeader` is *not*
gated on conflicts: it already accepts double-runs as the price of never
skipping. The default `nodeName` (the system hostname) is already unique per
host; set an explicit, unique `nodeName` when several nodes might share a
hostname (e.g. identical container images without distinct hostnames).

### Consistent cluster size

The safety argument also assumes every node uses the **same cluster size `N`**.
"Two strict majorities of `N` cannot be disjoint" is only true for a *single*
`N`: two majorities of *different* sizes **can** be disjoint. But `N` is each
node's own `len(peers) + 1`, and the [job-set fingerprint](#the-job-set-id-foundation)
deliberately covers job *definitions* only, **not** the peer list. So two nodes
with divergent peer lists still see each other `agreed`, each reaches a quorum
under its *own* `N`, and **both** elect themselves: a silent double-run. An
ordinary cluster **resize** (say rolling 3 → 5 nodes) triggers exactly this:
mid-roll, the old nodes still carry `N = 3` (quorum 2) while the new ones carry
`N = 5` (quorum 3), so the old `{a, b}` and new `{c, d, e}` groups are each
quorate and each run the `Leader` jobs.

yacron2 closes this the same way it closes a duplicate `nodeName`. Each node
reports its declared `N` on `/peer`, and a peer that **agrees on the job set but
declares a different `N`** is treated as a first-class `conflict`: this node's
`Leader` jobs **fail closed** until the cluster reconverges on one `N`. Because a
resize leaves the job set unchanged, the divergent nodes *are* mutually `agreed`
and therefore each observe the mismatch: both sides stand down, so no firing
double-runs while the roll-out is in flight. The conflict is surfaced as the
`size_conflict` / `conflicting_sizes` fields on
[`GET /cluster`](#observing-the-cluster), a banner in the dashboard cluster
panel, and an `ERROR` log line, and clears automatically once every node's
`peers` agree on the member set. As with a `nodeName` conflict, `PreferLeader`
is *not* gated: it already accepts double-runs as the price of never skipping.

> **Note:** the check compares the declared size `N`, which catches every
> *resize* (the documented failure above). It does not detect a same-`N` but
> different-*membership* divergence (e.g. swapping one peer for another while
> keeping the count). To stay safe, change membership **one node at a time** so
> the old and new majorities always overlap, and let each change converge (the
> dashboard shows `agreed` on every node) before the next.

### Sizing the cluster

A `Leader` job fires successfully only while a quorum is up and mutually
reachable, so **pick an odd size**: each odd size adds one failure of headroom
(3 tolerates 1, 5 tolerates 2, 7 tolerates 3), while an even size needs the same
quorum as the odd size below it yet has an extra node that can fail, so yacron2
warns on even sizes (for `N > 2`). A 2-node cluster is strictly worse than a
single replica (both must be up, with no failover upside), so yacron2 **refuses
to start** with `electLeader` and 2 nodes (a `ConfigError`); a 2-node cluster is
fine for attestation-only.

Framed as expected **skipped firings** for an hourly `Leader` job (8760
firings/year), which is often the more intuitive view:

| N | p=0.99 | p=0.999 |
| --- | --- | --- |
| 1 | ≈88 skips/yr | ≈8.8 skips/yr |
| 3 | ≈2.6 skips/yr | ≈0.03 skips/yr |
| 5 | ≈0.09 skips/yr | negligible |

The binomial derivation and full availability tables are in the
[Appendix: cluster sizing math](#appendix-cluster-sizing-math) at the bottom of
this page.

### Failure handling

If `electLeader` is configured but the cluster listener fails to start (bad cert
files, a bad listen address, a port already in use), the node logs the error and
keeps running, and each policy honours its own contract: `Leader` jobs **fail
closed** and stay idle rather than falling back to running everything on every
replica, while `PreferLeader` jobs are **never-skip** and run anyway (a node
with no manager is exactly the "store/quorum unreachable" outage `PreferLeader`
accepts a double-run to survive; skipping would drop the job to zero runs on a
fleet-wide start failure). The operational consequence: a listener broken on
*every* replica means every replica runs every `PreferLeader` firing, which is
why `PreferLeader` is reserved for idempotent jobs. (`EveryNode` jobs are
unaffected; see below.) Leadership transitions are logged each time the node
acquires or loses scheduled-job leadership.

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
* When election is configured but no manager is running (e.g. the listener
  failed to start), `Leader` jobs **fail closed** (skip), while `PreferLeader`
  jobs are **never-skip** and run anyway -- on this and every other manager-less
  replica, the documented double-run cost, preferred to dropping the job to
  zero runs. `EveryNode` jobs are independent of cluster health, so they keep
  firing regardless.
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
no manager    -> Leader skips (fail closed), PreferLeader runs (never-skip; the leadership listener failed to start, which is the very outage PreferLeader accepts a double-run to survive)
conflict      -> skip (fail closed; a duplicate nodeName, a cluster-size disagreement, OR a coordination-policy mismatch is visible)
PreferLeader  -> run only if this node is the lowest reachable agreeing node
Leader        -> run only if this node is the quorum-gated elected leader
```

(The `conflict` row applies to `Leader` only; `PreferLeader` and `EveryNode` are
gated on none of a duplicate `nodeName`, a cluster-size disagreement, or a
coordination-policy mismatch. A coordination-policy conflict is a quorate peer
advertising a different `distribution` or `elect_leader` setting, surfaced as
`policy_conflict: true` with the differing descriptors in `conflicting_policies`;
it is the third trigger of the umbrella `conflict` flag alongside `conflict_names`
and `size_conflict`. Under `distribution: spread`, described next, the last two
lines become "the *per-job owner* among the reachable agreeing nodes" and "the
quorum-gated *per-job owner*" respectively.)

One transient exception to `PreferLeader`'s never-skip (gossip backend only): a
node whose manager was just (re)built -- a cold boot, or a reload that changed
the `cluster` section -- holds its never-skip gates **closed** while its view is
still converging: until every configured peer has been polled once, and
(bounded by about two poll `interval`s) until the current-build agreeing peers
re-attest this incarnation. Without the hold, a fresh manager's blank view
would elect *itself* on every node at once and double-run each due
`PreferLeader` firing on a healthy cluster. The cost is the mirror image: a
`PreferLeader` firing due inside that window is skipped -- on *every* node when
the held node is the rightful owner, since its peers still defer to it --
transient and self-healing, the same fail-closed trade the quorum gate makes.
A pending retry treats the hold as a transient denial (deferred, never
abandoned).

### `@reboot` jobs under leader election

`@reboot` fires once at startup, which is the one instant the cluster has *not*
yet converged: no peer has been polled, so there is no quorum and no elected
owner. Running a leader-gated `@reboot` job immediately would misfire: a
`Leader` job would see no quorum and skip *forever* (`@reboot` never re-fires),
and a `PreferLeader` job would see only itself on every node and run *everywhere*.
So under `electLeader` an `@reboot` job with `Leader` or `PreferLeader` policy is
**deferred**: held until the cluster converges, then run **once** on the owner
that policy resolves to. The deferral exists only to get past that boot-time
"every node sees only itself" window; **which owner runs it, and whether it runs
at all without a quorum, follows the job's policy exactly as for a scheduled
firing**:

* **`Leader`** runs on the **quorum-gated** elected owner. If no quorum ever
  forms, the deferred job **does not run** (the at-most-once trade: a skip is
  preferred to a double-run), and it also stands down while a `nodeName`/size
  conflict is visible.
* **`PreferLeader`** runs on the **quorum-free** availability owner (the lowest
  reachable agreeing node), so it **always resolves to some node and runs even
  with no quorum** (an isolated or minority node runs it itself), exactly
  mirroring `PreferLeader`'s never-skip contract for scheduled jobs. The price,
  as ever for `PreferLeader`, is a possible double-run across a partition.

For `@reboot` work that must run on **every** node at boot (warming a local
cache, announcing the node), use `clusterPolicy: EveryNode`, which is not
deferred.

A deferred `@reboot` one-shot is **never silently lost across a reload**.
Deferral only happens at the boot instant, so a job whose name momentarily
disappears from the loaded config before the cluster converges (a templating
glitch, or a remove-then-re-add seen mid-reload) is **kept pending**, not
dropped, and runs once the name comes back. The launch is always gated on the
name being present *and* still a `Leader`/`PreferLeader` `@reboot`, so:

* a job you **deliberately remove** from the config (and leave removed) never
  runs, since its name stays absent;
* a name **reused** for a different `@reboot` job runs the *current* definition,
  never the one captured at boot; and if the reused job is no longer a deferrable
  `@reboot` (it became `EveryNode`, or a real schedule), the original one-shot is
  considered gone and the new job is left to its own scheduling.

On a **lease backend** the same deferral applies, translated to lease vocabulary:
a `Leader` `@reboot` runs on **the lease holder** (and skips while the store is
unreachable), a `PreferLeader` `@reboot` runs on **this** node when the store is
unreachable (a possible boot-time double-run), and `EveryNode` is not deferred.
The cross-node "already ran" record that gossip advertises peer-to-peer is
**persisted in the lease store** instead (a Kubernetes Lease annotation, an etcd
sibling key, append-only records in the filesystem store's `cluster/reboot-ran`
stream), scoped to the current [job-set id](#the-job-set-id-foundation), so
a failover holder does not re-run the one-shot. Because the store outlives the
processes, this shifts the semantics: a `Leader` `@reboot` runs **once per job
configuration**, not once per boot. Restarting the whole fleet with an unchanged
config does *not* re-fire it -- every node reads the record back and retires the
one-shot without running it. It fires again only when the job set changes (a new
job-set id invalidates the stored record), so a warm-up or migration step that
must run on every deploy cannot rely on a leader-gated `@reboot` here unless the
deploy also changes the job set.

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

* **Same safety, not more.** Spread keeps the quorum gate and is at-most-once
  for `Leader` jobs, no weaker than single-leader. Under a clean partition every
  quorate node sees the same member set and computes the same owner, so at most
  one node runs each job. The subtle case is a *thin bridge* (a quorate pair
  that share too few witnesses to confirm each other): because the rendezvous
  winner is per-job, it can be exactly such an unconfirmable node, so the raw
  hash would let a peer that cannot see it self-own the job and double-run it,
  even though single-leader (whose winner is always the one global-lowest node,
  which everyone can see) would not. Spread closes this by folding the
  *unconfirmed* peers a quorate neighbour vouches for into each job's rendezvous
  and deferring to any that outrank it (two strict majorities of one `N` always
  overlap, so a co-owner you cannot see is always gossiped to you). The price is
  the same fail-closed trade single-leader makes: a job whose owner no quorate
  peer can currently confirm stands down until the view converges. This is a
  *load* optimization; it does not change the headline best-effort guarantee.
  `Leader` jobs still skip without quorum; `PreferLeader` still ignores quorum
  (its owner is computed over the reachable set, so an isolated node owns and
  runs all of its jobs, and it keeps its never-skip contract unchanged).
* **Rendezvous hashing, not modulo.** When a node leaves or joins, only *its*
  share of jobs is reassigned (to the next-highest-weight node); the rest stay
  put. A membership change is therefore minimally disruptive, unlike
  `hash % N`, which would reshuffle everything.
* **Best with many or heavy jobs.** Hashing is only *roughly* even, so with a
  handful of jobs the split is lumpy (several can land on one node). It pays off
  when a single node cannot comfortably carry all the scheduled work; for light
  workloads the default single leader is simpler and equally correct.
* **Keep it consistent.** Every node must agree on `distribution` (just like the
  peer list and `electLeader`). A quorate peer that agrees on the job set but
  advertises a different `distribution` (or `electLeader`) is treated as a
  first-class **coordination-policy conflict**: it surfaces as
  `policy_conflict: true` (with the differing descriptors in
  `conflicting_policies`) and, as the third trigger of the umbrella `conflict`
  flag, **stands this node's `Leader` jobs down** (fail closed) until every node
  reconverges on one policy. `distribution` is *not* part of the job-set id (it
  is cluster config, not a job property), so a mismatch does not show up as
  drift; treat it like `electLeader` and roll it out uniformly. It is inert
  without `electLeader` (and yacron2 warns if you set it anyway).

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
curl -s http://localhost:8080/jobs | python -m json.tool | grep -A1 clusterOwner
# under distribution: spread, each leader-gated job carries a "clusterOwner"
# naming the node that runs it; single-leader mode emits no clusterOwner (the
# leader from GET /cluster owns every Leader job)
```

## Observing the cluster

`GET /cluster` on the [web/HTTP interface](HTTP-API) returns the current view as
JSON. When no `cluster` section is configured it returns
`{"enabled": false, "peers": []}`; otherwise it returns the node's view:

```jsonc
{
  "enabled": true,
  "backend": "gossip",             // the active cluster.backend
  "node_name": "yacron-a",
  "job_set_id": "v1:…",
  "cluster_size": 3,
  "quorum": 2,
  "elect_leader": true,
  "distribution": "single-leader", // or "spread"
  "conflict": false,               // umbrella: true if any conflict pauses Leader jobs
  "conflict_names": [],            // the duplicated nodeName(s), if any
  "size_conflict": false,          // true if an agreeing peer declares a different N
  "conflicting_sizes": [],         // those divergent cluster sizes, if any
  "policy_conflict": false,        // true if an agreeing peer declares a different distribution/elect_leader
  "conflicting_policies": [],      // those differing coordination-policy descriptors, if any
  "quorate": true,                 // whether this node sees a quorum
  "leader": "yacron-a",            // null when not quorate, or always in spread mode
  "is_leader": true,               // always false in spread mode (no single leader)
  "peers": [
    {"host": "yacron-b.internal:8443", "status": "agreed",
     "node_name": "yacron-b", "job_set_id": "v1:…",
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

The JSON above is the **gossip** shape. A lease backend returns a lease-shaped
view instead: `backend` names the backend, `peers` is `[]`,
`cluster_size`/`quorum` are `1`, `conflict`/`size_conflict`/`policy_conflict`
are always `false`,
and an extra `lease` block carries the holder and expiry: for `kubernetes`
`{name, namespace, identity, holder, expiry}`, for `etcd`
`{electionName, identity, holder, leaseId, expiry}`, for `filesystem`
`{path, electionName, identity, holder, fence, expiry}` (`fence` is the
store's monotonic takeover counter). There `quorate` means the
node has a *fresh read of the lease store* (see
[Operating the lease backends](#operating-the-lease-backends-kubernetes-and-etcd)
and the [`GET /cluster`](HTTP-API#get-cluster) reference).

The same view is rendered as a **cluster panel** in the
[Web Dashboard](Web-Dashboard): a status dot per peer, the agreement tally, and
(when election is on) the quorum count and this node's role (leader, follower,
"no quorum", or, in spread mode, "spread (per-job owner)").

Beyond agreement and roles, the gossip exchange also carries **observability
payload**: each node piggybacks a compact per-job run summary (running /
enabled / next fire / last finished run) on its `/peer` response, capped so it
can never push the response past the gossip byte limit. Every node therefore
holds a fleet-wide picture of *what ran where* that is at most one `interval`
stale per peer, served as [`GET /fleet`](HTTP-API#get-fleet) and rendered as
the dashboard's [fleet view](Web-Dashboard#fleet-view-every-nodes-runs-in-one-pane),
the single pane of glass for `spread` mode, where each job's runs land on a
different node. The summaries are display-only: election, quorum, and every
run/skip decision ignore them, and a malformed summary from a peer degrades to
"no data for that node". The lease backends exchange nothing node-to-node, so
`/fleet` reports `enabled: false` there.

**Gossip as a secondary data plane (`cluster.observability`).** The same gossip
mechanism can carry more than election. Opt in and each node also gossips its
**whole-node CPU/memory** (the [`GET /node`](HTTP-API#get-node) numbers), so the
fleet view shows every node's live load beside its runs — invaluable in `spread`
mode for watching work distribute. The reading rides each `/peer` response as a
small `X-Yacron2-Node-Stats` header rather than in the body, on full responses
and bodyless `304`s alike, so live load values never defeat the exchange's
conditional `304` optimisation: a sharing cluster's steady-state round still
costs headers only, and each absorbed reading is as fresh as the last
successful poll. Crucially this is **available to any
backend**: a `kubernetes`/`etcd`/`filesystem` cluster (which has no node-to-node
channel of its own) can stand up a *second, election-inert* gossip mesh purely
for this observability data, leaving election with the lease store. See
[`cluster.observability`](Configuration-Reference#observability-overlay) for the
config and the two shapes (an opt-in marker under `backend: gossip`; a dedicated
mesh for the lease backends). Like the run summaries, node stats are best-effort
display data: a hostile or malformed peer payload degrades to "no data", never
poisoning the view or any decision.

### Monitoring and alerting

**`quorate` is the field to alert on for every backend** (on gossip it means "this
node sees a majority"; on a lease backend it means "this node has a fresh read of
the store"). This section covers the gossip signals; the lease equivalents are in
[Monitoring the lease backends](#monitoring-the-lease-backends).

Every signal you would alert on is exported natively at `GET /metrics` (see
[Metrics with Prometheus](Metrics-with-Prometheus)) as `yacron2_cluster_*`
series -- `yacron2_cluster_quorate`, `yacron2_cluster_is_leader`,
`yacron2_cluster_conflict{kind}`, `yacron2_cluster_peers{status}`,
`yacron2_cluster_size` / `yacron2_cluster_quorum`, plus the
`yacron2_cluster_leader_transitions_total` and
`yacron2_cluster_quorum_transitions_total` counters -- each mirroring a
pre-derived field on `GET /cluster` (the
[statsd integration](Metrics-with-Statsd) is still per-job). Useful alerts:

| Alert when | Field(s) | Metric(s) | Means |
| --- | --- | --- | --- |
| `quorate` is `false` for more than a few `interval`s | `quorate` | `yacron2_cluster_quorate` | this node cannot see a majority, so its `Leader` jobs are standing down |
| `conflict` is `true` | `conflict`, `conflict_names`, `size_conflict`, `conflicting_sizes`, `policy_conflict`, `conflicting_policies` | `yacron2_cluster_conflict{kind}` (`kind="nodename"` / `"size"` / `"policy"`) | a duplicate `nodeName`, a cluster-size disagreement, or a coordination-policy (`distribution`/`elect_leader`) mismatch is pausing `Leader` jobs (page on this) |
| `agreed` peers fall below `quorum` | count of `peers[].status == "agreed"` vs `quorum` | `yacron2_cluster_peers{status="agreed"}` vs `yacron2_cluster_quorum` | the cluster is one failure from losing quorum (this node counts itself toward quorum, so `quorum − 1` agreed peers is the last quorate state; any fewer duplicates the `quorate` alert) |
| any `peers[].status` is `untrusted` | `peers[].status`, `peers[].last_error` | `yacron2_cluster_peers{status="untrusted"}` | a peer's certificate failed verification (often a botched cert rotation; see [Certificate rotation](#certificate-rotation)) |

The [example alerts](Metrics-with-Prometheus#example-alerts) on the Prometheus
page include the quorum rule and the split-brain check
(`sum(yacron2_cluster_is_leader) > 1`). A blackbox / JSON-exporter probe
(Prometheus `json_exporter`, a Nagios check, etc.) scraping `GET /cluster` on
every replica remains a valid alternative, and is the only source for the
detail fields the metrics do not carry (per-peer `last_seen` and `last_error`,
`conflict_names`, `conflicting_sizes`, `conflicting_policies`; the leader's
name surfaces only as the `yacron2_cluster_leader_info{leader}` label). The same
transitions are also **logged** (leadership and quorum changes, conflict
onset at `ERROR` (clear at `INFO`), and per-peer `untrusted`/`unreachable`/drift
at `WARNING`), so a log-based alert is a viable second source.

### Detecting a double-run

The [best-effort guarantee](#guarantees-and-trade-offs) admits narrow windows
where two nodes each run the same `Leader` firing (a thin bridge, a >1-hop
gossip gap, or mid-convergence). This is **not** caught by the `conflict` flag:
that flag is only for a duplicate `nodeName` or a size disagreement, and by
construction the two transient leaders cannot see each other, so neither one's
`GET /cluster` shows anything wrong (each reports `is_leader: true`). There is
no single-node signal for it.

To detect it, scrape **every** replica's `GET /cluster` and alert when more than
one calls itself the leader:

```shell
# across all replicas, count how many believe they are the leader
for url in yacron-a:8080 yacron-b:8080 yacron-c:8080; do
  curl -s "http://$url/cluster" \
    | python -c 'import sys,json; print(str(json.load(sys.stdin).get("is_leader")).lower())'
done | grep -c '^true$'     # > 1 means a transient double-leader
```

Under `distribution: spread` there is no single leader, so instead compare the
`clusterOwner` each replica reports per job (`GET /jobs`) and alert if any job
has more than one distinct owner across the fleet. A non-idempotent job that
must *never* double-run belongs on a fenced backend (`kubernetes` / `etcd`, or
`filesystem` given NTP-bounded clocks) or a single replica, not the gossip
backend.

## Guarantees and trade-offs

The delivery guarantee each `clusterPolicy` gives depends on the backend. The
matrix below is the one-word summary (fuller wording follows); "fenced" is the
hard, single-holder guarantee, "best-effort" admits the narrow gossip windows,
and "may skip" / "may dup" name the side each policy gives up:

| `clusterPolicy` | `gossip` | `kubernetes` | `etcd` | `filesystem` |
| --- | --- | --- | --- | --- |
| `Leader` | best-effort (may skip; rare dup) | fenced (may skip) | fenced (may skip) | fenced under NTP-bounded skew (may skip) |
| `PreferLeader` | never-skip (may dup) | never-skip (may dup) | never-skip (may dup) | never-skip (may dup) |
| `EveryNode` | every-node | every-node | every-node | every-node |

On the lease backends "fenced" holds while the store is reachable; a store
outage is the one window a `Leader` job skips and a `PreferLeader` job may
double-run (see [Failure modes](#failure-modes)). On `filesystem` the fence
additionally assumes **NTP-bounded clock skew**: the lease expiry is compared
across host wall clocks with two 1 s margins (the holder stops calling itself
leader 1 s before its lease really expires; a challenger refuses to take over
until the observed expiry is 1 s in the past by *its* clock), so two leaders
need inter-host skew above the sum, ~2 s. Run NTP on every node that mounts
the store -- the same requirement [Durable State](Durable-State#operational-notes)
documents under "Clocks on shared mounts". The `kubernetes`/`etcd` takeover is
judged on a single clock (the challenger's own, or etcd's server), so those
two carry no such budget. `EveryNode` is never gated on any backend.

This gossip design intentionally keeps **no shared state**, which is what makes
it easy to run, but it means the guarantee is *best-effort*, not fenced
exactly-once. Because each node acts on a view only as fresh as its last poll
(`interval`), there are narrow windows where behaviour degrades:

* **Just after a leader dies**, a `Leader` firing may be *skipped* until the
  survivors notice (up to one `interval`) and re-elect.
* **A leader partitioned away while still alive** keeps electing itself on its
  last (now-stale) view until its *own* next poll fails (up to one `interval`),
  overlapping the majority's re-election, so a clean partition can briefly
  **double-run** a `Leader` firing, not only skip one. It self-heals once the
  cut-off node re-polls and stands down.
* **Asymmetric or partial reachability.** Two nodes that never agree with each
  other can each stay quorate through shared members that *bridge* them. The
  election turns that bridge from cause into cure: each side discovers the other
  through the shared members' gossip and, once it can confirm the other is
  itself quorate, the lower `nodeName` wins on both sides, so a bridge of at
  least `quorum - 1` shared members collapses two would-be leaders back to one.
  A node only ever elects a leader it can confirm is itself quorate, so in a
  *converged* view a **healthy majority is not silently stood down** (it elects
  a node that actually runs). Two deliberate trades come with that liveness:
  two quorate nodes whose bridge is *thinner* than `quorum - 1` shared members,
  are more than one gossip hop apart, or are still converging may each elect
  themselves and **double-run** a `Leader` job; and symmetrically (because
  bridge confirmation is only as fresh as the witnesses' last gossip) a
  confirmed candidate that has since become isolated can briefly draw the
  majority into deferring to it, a transient **skip** until the stale gossip
  ages out (~1–2 `interval`s). `spread` behaves the same per job. (Choosing
  instead to *fail closed* on the double-run (skip rather than double-run)
  would require a lease/consensus store; see below.)
* **While a resize is rolling out**, nodes briefly disagree on the cluster size
  `N`; `Leader` jobs across the whole cluster stand down (fail closed) until
  every node's `peers` agree again, the at-most-once-preserving trade-off (see
  [Consistent cluster size](#consistent-cluster-size)).
* A `PreferLeader` job **may double-run** across a partition (that is the point
  of the policy: it never skips).

If you need a hard exactly-once guarantee, you need a shared store (etcd, a
Kubernetes `Lease`, or -- given NTP-bounded clocks -- a shared mount via the
`filesystem` backend), which this design deliberately avoids. If a job
must *never* be skipped or doubled, run a single replica (`replicas: 1`) or use
an external coordinator. Tuning the `interval` shorter narrows the degraded
windows at the cost of more polling traffic.

## Certificate rotation

**Rotation is automatic** on the `gossip` backend: yacron2 reloads its mTLS
contexts when the mounted CA/cert/key change in place. On each config-reload pass
(every minute) it compares the on-disk CA, cert, and key against what it loaded
at startup and, if any changed, restarts the cluster manager to rebuild the TLS
contexts with the new material, so an in-place renewal (the cert-manager, Vault,
or Kubernetes mounted-secret pattern of same paths, new bytes) needs **no manual
restart**. A detected change is dry-run loaded first, so a half-written cert
observed mid-rotation is retried rather than tearing the cluster down. This is
`gossip`-only; the lease backends use no per-node mTLS certs.

For the operational runbooks (leaf-cert rotation, rolling the cluster **CA** with
trust overlap, and recovering from an `untrusted` cascade), see
[Cluster certificate operations](Production-Deployment#cluster-certificate-operations).

## Operating the lease backends (Kubernetes and etcd)

The `kubernetes`, `etcd`, and `filesystem` backends replace the gossip protocol
with a shared store, giving a **fenced** election while the store is reachable
(exactly-once on `kubernetes`/`etcd`; on `filesystem`, fenced under NTP-bounded
clock skew). They share one code path (`yacron2.leadership.LeaseBackend`) and
differ only in which store they talk to. This section covers how they elect, how
to deploy each, their failure modes, and how to monitor them; the config keys
are in the [Configuration Reference](Configuration-Reference#cluster).

How the lease backends talk to their store: `kubernetes` and `etcd` speak
**plain HTTP over the core `aiohttp` dependency**, namely the Kubernetes
apiserver's REST API and etcd's v3 gRPC-gateway JSON API, while `filesystem`
talks to no service at all (its store is a directory, driven with the standard
library). So the **core install gains no new dependency**, and by avoiding
grpc/protobuf wheels every lease backend runs on the full set of architectures
yacron2 ships for. The Kubernetes backend can optionally use the **official
`kubernetes` client** when it is installed
(`pip install yacron2[kubernetes]`): `cluster.kubernetes.clientLibrary: auto`
(the default) prefers it when importable and otherwise falls back to the
hand-rolled REST transport, so the choice is automatic per architecture
(`library` requires the client, `http` forces the hand-rolled path). etcd always
uses its own v3 JSON gateway, so it has no optional client.

### Lease backends at a glance

* **No peer list, no mTLS, no quorum math.** The store is the single source of
  truth, so the gossip-only keys `listen`, `tls`, `peers`, `interval`, and
  `driftAfter` are ignored (each logs a one-line startup advisory). A lease
  backend **always elects**, so `electLeader` is implied and `electLeader: false`
  is likewise ignored with an advisory (configuring a lease backend *is* opting
  into leadership). The cluster is logically a single holder (`cluster_size` /
  `quorum` report `1`), and `GET /cluster` returns a lease-shaped view; its full
  field list is under [Observing the cluster](#observing-the-cluster).
* **The lease is the fence, not a name.** Leadership is decided by the
  *lease*, so a duplicate node identity cannot make two nodes both lead the way
  it can on a naive lease holder: etcd fences on the **bound lease id** (only
  the node whose own lease backs the election key leads), and Kubernetes and
  filesystem write a **per-process token** into the holder they record
  (`<identity>#<token>`) so two nodes sharing a `nodeName` still write
  distinct holders. You should still give each node a stable, unique
  name for clear observability (see
  [Node identity](#node-identity-for-the-lease-backends)).
* **Local-expiry safety.** A holder only calls itself leader until a
  *locally-computed* lease deadline (renew time + duration, minus a small
  clock-skew margin), so a node whose renew loop stalls self-demotes **without a
  network round-trip**, and never two holders act at once.
* **`PreferLeader` keeps never-skip semantics.** A node that currently **cannot
  reach** the coordination store runs a `PreferLeader` job anyway (it may
  double-run); a healthy follower that **can** see the holder defers. `Leader`
  stays fail-closed: it skips while the store is unreachable. This is the
  deliberate, documented trade: a `PreferLeader` job never skips, at the cost of
  a possible double-run during a store outage.
* **`distribution: spread`** (an opt-in, gossip-only mode that fans jobs across
  nodes instead of one leader; see [Distribution](#distribution-one-leader-or-spread-the-load))
  **is rejected** at config load on a lease backend (a hard `ConfigError`, not a
  silent fallback). A single lease holder cannot also be a per-job owner; use the
  gossip backend if you need per-job spread.

### How a lease backend elects

All three reduce leadership to "hold a short-lived lease on a shared object and
keep renewing it":

* **Kubernetes** drives a single `coordination.k8s.io/v1` `Lease`. The holder
  writes its identity into `spec.holderIdentity` and refreshes `spec.renewTime`
  every `retryPeriodSeconds`; if it stops, another node observes the lease go
  stale and takes it over, the standard client-go leader-election algorithm.
  The takeover deadline is anchored to *the challenger's own clock from the
  moment it first saw the record* (client-go's `observedTime`), so it is
  **immune to clock skew** between holder and challenger: a fast clock cannot
  steal a freshly-renewed lease.
* **etcd** creates a single key (`electionName`) with a *create-if-absent*
  transaction (compare `CREATE` revision `== 0`), bound to a short-TTL lease it
  keeps alive. At most one node's transaction wins; if the holder dies the lease
  expires, etcd deletes the key, and another node's transaction wins. etcd's
  server enforces the TTL.
* **Filesystem** takes the same flock-guarded, fence-counted TTL lease the
  [durable state store](Durable-State) provides, on a lease file in the shared
  directory. The holder renews it under the lock every `max(1s, ttl / 3)`; if
  it stops, the written expiry passes and a challenger takes the lease over,
  bumping the **fence counter**. The takeover compares the challenger's wall
  clock against the expiry the holder *wrote* -- a cross-host clock
  comparison, so unlike the two backends above it carries a ~2 s clock-skew
  budget (see [Filesystem](#filesystem-backend-filesystem) below).

All three gate `is_leader()` on a **locally-computed** lease deadline (renew/keepalive
time + duration − a 1 s clock-skew margin), so a node whose renew loop stalls
**self-demotes with no network call**: that local expiry, not the store, is what
guarantees two holders never act at once (on `filesystem`, together with the
challenger-side margin and NTP-bounded clocks). Separately, `is_quorate()` reflects
whether the node has a *fresh successful read* of the store (within one lease
duration / TTL); when it goes stale, `Leader` jobs fail closed and the never-skip
`PreferLeader` default runs the job anyway.

> **Precision note.** The clock-skew margin applies to `is_leader`'s **lease
> deadline**, not to the `is_quorate` **freshness window** (the full duration /
> TTL, no margin). So a follower's *view of who leads* can briefly lag a dead
> holder by up to one freshness window, while the would-be leader has already
> self-demoted: bounded and self-healing, and `PreferLeader` never skips during
> it.

### Node identity for the lease backends

Leadership is fenced on the **lease**, but each node still carries an identity:

* **etcd**: the value written at the election key is `cluster.nodeName` (there
  is no separate `etcd.identity` key). Leadership is decided on the **bound
  lease id**, not this string, so even two nodes sharing a `nodeName` cannot both
  lead (only the node whose lease backs the key is leader); a shared name only
  makes the *displayed* holder ambiguous.
* **Kubernetes**: `cluster.kubernetes.identity` (defaulting to `nodeName`) is
  the human-readable holder; yacron2 appends a **per-process token** to the
  `holderIdentity` it actually writes (`<identity>#<token>`), so two nodes
  sharing an identity still write distinct holders and cannot both renew. The
  dashboard and `GET /cluster` strip the token back to the readable name (so
  `kubectl get lease … -o jsonpath='{.spec.holderIdentity}'` shows the suffixed
  form, while the dashboard shows the clean name).
* **filesystem**: the holder written into the lease file is always
  `<nodeName>#<12-hex per-process token>`, so two nodes sharing a `nodeName`
  (or a restarted daemon) can never adopt or renew each other's lease; as on
  Kubernetes, the dashboard and `GET /cluster` strip the token back to the
  readable name, and no run/skip decision ever string-compares it.

A duplicate identity therefore no longer silently breaks the fence, but give
each node a **stable, unique name** anyway so the holder shown in the dashboard,
`kubectl get lease`, or `etcdctl get` unambiguously names one node. In Kubernetes
both a Deployment and a StatefulSet give each pod a unique hostname; a
StatefulSet's ordinals just make the holder name predictable across restarts.

### Kubernetes (`backend: kubernetes`)

No mTLS, no peer list, no odd-replica rule: the apiserver is the authority, not
a quorum, so a plain `Deployment` with any replica count works:

```yaml
cluster:
  backend: kubernetes
  kubernetes:
    leaseName: yacron2-leader      # the Lease object the replicas contend for
    # leaseNamespace: null         # default: the pod's own namespace
    leaseDurationSeconds: 15        # failover happens within ~this long
    renewDeadlineSeconds: 10        # must be < leaseDurationSeconds
    retryPeriodSeconds: 2           # renew/observe cadence; must be < renewDeadlineSeconds
    # clientLibrary: auto          # auto | http | library (see below)
```

The three timings are cross-checked **at config load** (a `ConfigError`
otherwise): `renewDeadlineSeconds > 0`,
`leaseDurationSeconds > renewDeadlineSeconds`, `0 < retryPeriodSeconds <
renewDeadlineSeconds`, and `renewDeadlineSeconds + retryPeriodSeconds <
leaseDurationSeconds` (so a renew that just misses its deadline still leaves a
retry inside the lease window). Run `yacron2 --validate-config` to check them
before deploying; see the [Command-Line Reference](CLI-Reference).

* **RBAC (required).** The backend needs `get` (observe) and `update` (renew /
  take over / release) on the one named `Lease`, plus `create` on `leases` to
  first acquire it. `create` cannot be scoped by `resourceNames`, so it is a
  second, unscoped rule:

  ```yaml
  apiVersion: rbac.authorization.k8s.io/v1
  kind: Role
  rules:
    - apiGroups: ["coordination.k8s.io"]
      resources: ["leases"]
      resourceNames: ["yacron2-leader"]   # keep in sync with cluster.kubernetes.leaseName
      verbs: ["get", "update"]
    - apiGroups: ["coordination.k8s.io"]
      resources: ["leases"]
      verbs: ["create"]                    # create cannot be scoped by resourceNames
  ```

  A ready-to-apply `ServiceAccount` + `Role` + `RoleBinding` + 3-replica
  `Deployment` is in
  [`example/kubernetes/deployment.yaml`](https://github.com/ptweezy/yacron2/blob/develop/example/kubernetes/deployment.yaml).
  Run `yacron2 --validate-config` on the config before applying the manifests;
  see the [Command-Line Reference](CLI-Reference).
* **Credentials.** In-cluster, the pod's service-account token, CA, and
  namespace file are used automatically (`leaseNamespace` defaults to the pod's
  own namespace). For out-of-cluster / local testing set
  `cluster.kubernetes.kubeconfig` (and optionally `apiServer` to override the
  server URL).
* **Transport (`clientLibrary`).** `auto` (default) uses the official
  `kubernetes` client when it is importable (`pip install yacron2[kubernetes]`)
  and otherwise falls back to a hand-rolled apiserver REST transport over the
  core `aiohttp` dependency, so on an architecture without the client it still
  works. `http` forces the REST transport; `library` **requires** the native
  client and fails the backend start (the node's `Leader` jobs then fail closed)
  if it is not importable. Both transports drive the same Lease, so the choice
  is purely about which client code runs.
* **Failover timing.** A holder that dies is replaced within
  ~`leaseDurationSeconds`. On a *graceful* shutdown the holder clears
  `holderIdentity` so a survivor takes over immediately. Shorter durations fail
  over faster at the cost of more apiserver traffic. All three timings are
  ordered and **enforced at config load**:
  `leaseDurationSeconds > renewDeadlineSeconds`,
  `retryPeriodSeconds < renewDeadlineSeconds`, and
  `renewDeadlineSeconds + retryPeriodSeconds < leaseDurationSeconds` (so a
  renew that just misses its deadline still leaves a retry inside the window).

### etcd (`backend: etcd`)

Point the backend at one or more etcd endpoints (tried in order for failover):

```yaml
cluster:
  backend: etcd
  etcd:
    endpoints: [http://etcd-0:2379, http://etcd-1:2379]
    electionName: yacron2/leader   # the key; its value is the holder's nodeName
    ttl: 15                         # lease TTL, seconds (>= 3; keepalive every ~ttl/3)
    # username: root               # for an auth-enabled cluster …
    # password: { fromEnvVar: ETCD_PASSWORD }
    # tls: { ca: /etc/etcd/ca.pem, cert: /etc/etcd/client.pem, key: /etc/etcd/client.key }
```

* **Transport.** Speaks etcd's v3 gRPC-gateway JSON/HTTP API directly over
  `aiohttp`: no `etcd3`/grpc client, so no extra dependency and full
  architecture coverage. There is no optional native-client extra for etcd.
* **Authentication.** For an auth-enabled cluster set `username` and resolve the
  `password` from exactly one of `value` / `fromFile` / `fromEnvVar` (like
  `web.authToken`); a configured-but-empty source fails closed at load. The auth
  token is obtained at startup and **re-fetched automatically when it expires**
  (yacron2 re-authenticates on an etcd `401`), so a token TTL does not wedge the
  backend. Always pair a `username` with a resolvable `password`.
* **TLS.** For `https://` endpoints set `tls.ca` (and `tls.cert` / `tls.key` for
  client-cert auth). `http://` and `https://` endpoints are detected per-URL.
* **Failover timing.** A dead holder is replaced within ~`ttl`. On a *graceful*
  shutdown the holder **revokes** its lease, deleting the key at once for
  immediate failover. The value at `electionName` is the holder's `nodeName`, so
  `etcdctl get yacron2/leader` shows who leads. `ttl` must be **>= 3** (a smaller
  value is rejected at config load). etcd may *grant* a smaller TTL than
  requested (its `--min-lease-ttl` setting, or server load), which the backend
  honours: a smaller server-granted TTL narrows the effective leader window
  accordingly.

### Filesystem (`backend: filesystem`)

Point the backend at a directory on a mount every node shares (an Amazon S3
Files / EFS / NFSv4 mount). The mount *is* the store, so there is no service
to deploy and nothing beyond the standard library at work:

```yaml
cluster:
  backend: filesystem
  filesystem:
    path: /mnt/shared/yacron2       # the directory the election lease lives in (required)
    # electionName: cluster/leader  # the lease's name inside the store
    # ttl: 15                        # lease time-to-live, seconds (>= 3; renewed every ~ttl/3)
    # deploymentId: null             # namespace inside the store; null -> "default"
    # topology: auto                 # auto | single-node | shared (assert shared on Windows/macOS)
  nodeName: yacron-a
```

The full key table is in the
[Configuration Reference](Configuration-Reference#cluster); `deploymentId` and
`topology` carry the same semantics as their [`state:`](Durable-State)
counterparts. `ttl` must be **>= 3** (a smaller value is rejected at config
load: the holder keeps the lease only until `ttl` minus the clock-skew margin
and renews every `max(1s, ttl / 3)`, so a smaller `ttl` would make a node
treat its own freshly-won lease as expired).

* **How it elects.** Leadership is the [durable state store](Durable-State)'s
  flock-guarded, fence-counted TTL lease: one small file under the store's
  `leases/` directory, taken and renewed under an advisory lock, with a
  monotonic **fence counter** that bumps on every takeover. The holder string
  written into the lease is `<nodeName>#<12-hex per-process token>` (see
  [Node identity](#node-identity-for-the-lease-backends)). The holder renews
  every `max(1s, ttl / 3)` and calls itself leader only until a local
  `time.monotonic` deadline anchored *before* the renewing write, minus a 1 s
  clock-skew margin, so a stalled renew loop **self-demotes with no I/O**; a
  challenger refuses to take over until the observed expiry is a further 1 s
  in the past *by its own clock*.
* **Clocks: run NTP on every node.** Those two 1 s margins are the whole skew
  budget: the takeover compares wall clocks *across hosts*, so two
  simultaneous leaders need inter-host skew above their sum, **~2 s**. NTP
  keeps real fleets orders of magnitude below that; it is the same
  requirement [Durable State](Durable-State#operational-notes) documents
  under "Clocks on shared mounts". This backend does **not** inherit the
  `kubernetes`/`etcd` skew immunity (those judge takeover on a single clock).
* **The lock-fidelity probe (a hard refusal).** `start()` refuses a store
  whose locks are demonstrably fiction, raising a `ConfigError` of the form
  `cluster.backend filesystem: refusing to elect over <path>: …` instead of
  silently electing two leaders. Two same-host checks: a **functional probe**
  (two descriptors of one file must contend on a non-blocking exclusive lock;
  a mount that grants both has no-op locks, e.g. `… grants two exclusive
  locks on one file (its locks are no-ops)`), and on Linux a **mount-option
  sniff** (an NFS mount carrying `nolock` or `local_lock=flock`/`local_lock=all`
  satisfies flock host-locally, so its locks `are host-local and cannot fence
  other nodes`). A refused start leaves the manager unbuilt, so `Leader` jobs
  fail closed -- the safe direction (see
  [Failure handling](#failure-handling)).
* **The Windows/macOS residual.** Both probe checks run on one host, so a
  mount whose locks are real locally but not propagated across hosts is
  **not detectable** on platforms without `/proc/mounts` (Windows, macOS).
  There `topology: auto` cannot probe and resolves `single-node` -- the store
  then warns that "its locks only exclude processes on THIS host" and asks
  you to `set cluster.filesystem.topology: shared` if the directory really is
  a shared mount -- and on a Windows shared mount `start()` logs a loud
  advisory: "cross-host lock fidelity cannot be verified on this platform (no
  /proc/mounts); the election is safe only if the mount honours byte-range
  locks across hosts". The residual safety rests on that operator assertion.
* **What `quorate` means here.** A fresh **positive** observation of the
  lease store within one `ttl`: only an operation that returned an actual
  lease (a renew, an acquire, or a read that parsed a live holder) counts. A
  `None` from the lease API is deliberately *not* contact -- the store fails
  closed, conflating "denied" with "unreadable" -- so a sick store lapses
  quorum: `Leader` jobs fail closed and never-skip `PreferLeader` jobs run
  anyway (they may double-run across the outage), the same posture as the
  other lease backends' store outage. Each failed round logs
  `cluster: filesystem election round failed: …` at WARNING.
* **Sharing a directory with `state:`.** Legal, and recommended when both are
  used (same `path` *and* `deploymentId`): the election's embedded store runs
  none of the scheduler's durable-state chores (no manifests, no GC, no
  counters), the stream namespaces are disjoint, and election lease files
  are never deleted at all -- the scheduler's GC reclaims only the
  per-run DAG advance lease class, so a lease whose fences other nodes
  may still compare against is left alone at any age.
* **`@reboot` one-shots.** Same semantics as the other lease backends: once
  per job **configuration**, not once per boot. The "already ran" set is
  persisted as **append-only records** (one per newly-ran job, tagged with
  the job-set id) in the store's `cluster/reboot-ran` stream; readers union
  the records matching the *live* job-set id, and the stream is pruned to the
  newest **512** records (a documented bound). The set is re-read every 60 s
  and immediately on gaining leadership, so a failover leader never re-runs a
  one-shot the old leader marked moments before.
* **Failover timing.** A dead holder is replaced within ~`ttl` (plus the 1 s
  challenger margin). On a *graceful* stop the holder releases the lease
  best-effort so a survivor takes over at once; TTL expiry is the fallback.
* **Monitoring.** `GET /cluster` returns the lease-shaped view with
  `backend: "filesystem"` and a `lease` block
  `{path, electionName, identity, holder, fence, expiry}`; `fence` is the
  store's monotonic takeover counter, so a bump marks each real leadership
  change. No new Prometheus families; see
  [Monitoring the lease backends](#monitoring-the-lease-backends). A healthy
  start logs `cluster: filesystem election ready at <path> (election=…,
  identity=…, ttl=…)`.

### Failure modes

* **Store unreachable** (apiserver / etcd down, a hung or unmounted shared
  mount, or partitioned from this node).
  Within one duration / TTL the node's `is_quorate()` goes stale, so its
  `Leader` jobs **fail closed** (skip) and its `PreferLeader` jobs **run anyway**
  (the never-skip rule: they may double-run across the outage). When the store
  returns, leadership re-establishes within ~one duration. This is the lease
  backends' one double-run exposure, and it is the documented `PreferLeader`
  trade, not a fence break.
* **A fenced backend never shows two simultaneous leaders.** Unlike gossip there
  is no thin-bridge / convergence double-run window for `Leader` jobs, so
  scraping every replica for `is_leader: true` (the gossip double-run check)
  will never find two while the store is healthy. On `filesystem` this holds
  while inter-host clock skew also stays inside the ~2 s budget (see
  [Filesystem](#filesystem-backend-filesystem)); a store with fake locks never
  gets this far -- it is refused at start with a `ConfigError` ("refusing to
  elect over …") and `Leader` jobs fail closed.
* **Kubernetes optimistic concurrency.** Writes carry the observed
  `resourceVersion`; a node that loses the race gets an HTTP `409`, stands down
  for that round, and retries. The graceful release is best-effort: if it races
  a concurrent write the handover may instead wait out `leaseDurationSeconds`.
* **etcd lease loss.** If a keepalive reports the lease gone (TTL ≤ 0) the holder
  re-grants a fresh lease and re-campaigns, becoming leader again only if it
  re-wins the key.
* **etcd TTL below the floor.** If the server-honoured TTL ever drops below the
  usable floor (3 s), the fence collapses to ~zero and this node's `Leader` jobs
  fail closed (safe, but it can no longer lead). yacron2 logs a **one-time
  warning** on that transition (and a recovery once the granted TTL returns above
  the floor); check etcd's `--min-lease-ttl` and load if you see it.

### Monitoring the lease backends

**`quorate` is the field to alert on for every backend**; on a lease backend it
means this node has a fresh read of the store (on `filesystem`, a fresh
*positive* read: an operation that actually returned a lease), not that it
sees a majority. The
gossip alerts on the [Monitoring](#monitoring-and-alerting) table (per-peer
status, agreed-peers-vs-quorum, `untrusted` certs, the multi-leader scrape) **do
not apply**: a lease view has `peers: []`, `quorum: 1`, and `conflict` is always
`false`, and a fenced backend never reports two leaders (on `filesystem`,
within its ~2 s skew budget). So the signal that
matters is **`quorate`**:

| Alert when | Field(s) | Means |
| --- | --- | --- |
| `quorate` is `false` on a replica for more than a few rounds | `quorate` | that replica **cannot reach the lease store** (not "no majority"): its `Leader` jobs are standing down and its `PreferLeader` jobs may double-run |
| the holder is unexpected or flapping | `lease.holder`, `lease.expiry`, and on `filesystem` `lease.fence` (it bumps on every real takeover) | leadership is moving more than it should (renew loop starved, duration too short) |

Probe `GET /cluster` on each replica, or watch the store directly
(`kubectl get lease <name> -o jsonpath='{.spec.holderIdentity}'`,
`etcdctl get <electionName>`; the filesystem election lease is a small JSON
file under the store's `leases/` directory on the mount). The same leadership
transitions are logged.

The lease-backend `@reboot` behaviour is covered inline in the main
[`@reboot` section](#reboot-jobs-under-leader-election).

For running multiple replicas on Kubernetes (both `backend: kubernetes` and
`backend: gossip` on a StatefulSet), see
[Production and Container Deployment](Production-Deployment).

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
docker compose -f docker-compose-cluster.yml stop yacron-a   # watch leadership move to yacron-b
```

The compose file's header comments document the full set of things to try
(losing quorum, drift, the per-policy job behaviour).

**A full showcase.** For the fullest end-to-end demo (`distribution: spread`,
all three `clusterPolicy` values, and mTLS together), the repository ships
[`docker-compose-acme.yml`](https://github.com/ptweezy/yacron2/blob/develop/docker-compose-acme.yml);
its walkthrough is in
[`example/acme-platform/README.md`](https://github.com/ptweezy/yacron2/blob/develop/example/acme-platform/README.md).

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

### A fenced backend locally

To try a **lease backend** instead of gossip,
[`example/etcd/`](https://github.com/ptweezy/yacron2/tree/develop/example/etcd)
ships a `docker-compose.yml` with a single etcd plus two yacron2 instances
(`backend: etcd`) and one job of each `clusterPolicy`:

```shell
docker compose -f example/etcd/docker-compose.yml up --build
# exactly one instance leads; fail it over and watch the other take over within ~ttl:
docker compose -f example/etcd/docker-compose.yml stop yacron2-a
docker compose -f example/etcd/docker-compose.yml exec etcd etcdctl get yacron2/leader
```

For the Kubernetes `Lease` backend,
[`example/kubernetes/`](https://github.com/ptweezy/yacron2/tree/develop/example/kubernetes)
has the RBAC + `Deployment` to apply against any cluster (k3d/kind for local).

## See also

- [Configuration Reference](Configuration-Reference): the `cluster` section and `clusterPolicy` option schema.
- [HTTP Control API](HTTP-API): the `GET /cluster` endpoint.
- [Web Dashboard](Web-Dashboard): the cluster panel and per-job policy display.
- [Production and Container Deployment](Production-Deployment): running multiple replicas under Kubernetes.
- [Architecture and Internals](Architecture-and-Internals): where `cluster.py` fits in the daemon.

## Appendix: cluster sizing math

This expands the practical rule in [Sizing the cluster](#sizing-the-cluster) with
the underlying probability. A `Leader` job fires successfully only while a quorum
is up and mutually reachable. If each node is independently up with probability
`p`, and the quorum is `q = ⌊N/2⌋ + 1`, then the chance a given firing runs is
the probability that **at least `q` of `N` nodes are up**, a binomial tail:

```text
P(runs) = Σ (from k=q to N)  C(N, k) · p^k · (1 − p)^(N − k)
```

The table evaluates that for a few realistic per-node availabilities, as a
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

Caveats on the math:

* It assumes **independent** failures. Correlated failures (a bad config push, a
  shared host, zone, or power domain) break that assumption, and then more nodes
  can even hurt. Spread the nodes across independent failure domains; `p` should
  be realistic uptime *including* deploys and restarts, not raw hardware MTBF.
* It only models "is a quorum up". It does *not* capture the narrow
  membership-change windows in [Guarantees and trade-offs](#guarantees-and-trade-offs)
  (a firing may still slip through them), nor `PreferLeader` duplication, which
  is about partitions rather than node-up probability.
