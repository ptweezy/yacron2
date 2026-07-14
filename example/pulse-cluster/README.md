# Pulse (clustered) — a distributed second-level uptime / SLA monitor

The clustered sibling of [`example/pulse-monitor`](../pulse-monitor): a
**three-node, mutual-TLS cluster with leader election** that probes a critical
upstream **every few seconds** and splits the work the way a real monitoring
fleet should.

## Why cluster a monitor?

Two reasons, and this demo shows both:

1. **Independent vantage points.** For liveness, you *want* every node to probe
   the upstream on its own. A node whose network path to the upstream is broken
   will report `DOWN` while the others report `UP` — that disagreement is
   exactly how you catch a **partial or partition outage** that a single prober
   sitting in one place would miss. So `liveness-probe` is `EveryNode`.
2. **Don't triple the load for a single signal.** Latency is latency; sampling
   it from all three nodes 30 times a minute just hammers the upstream for no
   extra information. So `latency-slo` is `Leader` — one elected node measures
   it, everyone benefits.

| Job | Cadence | `clusterPolicy` | Runs on |
| --- | --- | --- | --- |
| `welcome` | `@reboot` | `EveryNode` | every node, at start |
| `liveness-probe` | every **5 s** | `EveryNode` | **all three** nodes (independent checks) |
| `latency-slo` | every **2 s** | `Leader` | the **leader only** (one authoritative sampler) |
| `heartbeat` | every **10 s** | `EveryNode` | every node (each proves itself alive) |
| `sla-rollup` | every **60 s** | `Leader` | the leader only (one summary) |

The probes are the same second-level schedules as the single-node example
(`second: "*/5"` and the 7-field `*/2 * * * * * *`); clustering is layered on
top. Note the scheduler ticks every second for the probes yet `sla-rollup`
still fires **exactly once per minute** on the leader.

## Run it

```console
docker compose -f example/pulse-cluster/docker-compose.yml up --build
```

- cronstable-a → <http://localhost:8080/> (the leader while all three are up)
- cronstable-b → <http://localhost:8081/>
- cronstable-c → <http://localhost:8082/> (also the upstream under test — a follower)

A one-shot `certgen` service mints a throwaway cluster CA and one cert per node
before the nodes start (`gen-certs.sh`; **local-experimentation certs only** —
see the script's header). Scroll to the **cluster** panel on any dashboard to
see the peer table, quorum, and elected leader.

Stop and wipe (including the throwaway certs):

```console
docker compose -f example/pulse-cluster/docker-compose.yml down -v
```

## Two independent experiments

The default upstream is **cronstable-c**, deliberately a *follower* (highest node
name, so never the leader). That keeps these two demos from interfering:

- **Fail the leader** —
  `docker compose -f example/pulse-cluster/docker-compose.yml stop cronstable-a`.
  Quorum holds (2 of 3), leadership moves to **cronstable-b**, and `latency-slo`
  resumes on <http://localhost:8081/>. The upstream is still up, so liveness
  stays green everywhere. *(Pure leader-failover.)*

- **Outage** —
  `docker compose -f example/pulse-cluster/docker-compose.yml stop cronstable-c`.
  Within ~5 s, `liveness-probe` goes **red** on the surviving nodes and their
  reports print the on-call `PAGE` line — the outage is caught from every
  remaining vantage point. Quorum is unaffected (a + b = 2 of 3). Bring it
  back with `... start cronstable-c`. *(Pure outage-detection.)*

## Spread the leader work (optional)

By default one leader runs every `Leader` job. Uncomment `distribution: spread`
in **all three** `node-*.yaml` files and recreate
(`docker compose -f example/pulse-cluster/docker-compose.yml up -d`):
`latency-slo` and `sla-rollup` then get per-job owners via rendezvous hashing,
so they can land on different nodes instead of both on the leader — same quorum
gate, same guarantee, just spread out.

## See also

- [`example/pulse-monitor`](../pulse-monitor) — the single-node version.
- [`example/cluster/docker-compose.yml`](../cluster/docker-compose.yml) — the
  clustering feature tour (all `clusterPolicy` values, drift, quorum) without
  the monitoring theme.
- [Clustering and Leader Election](../../wiki/Clustering-and-Leader-Election.md)
  and [Second-level schedules](../../wiki/Schedules-and-Timezones.md#second-level-schedules)
  in the wiki.
