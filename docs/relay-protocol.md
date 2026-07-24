# Push relay protocol (v1)

This document is the wire contract between a cronstable daemon and a push
relay: the hosted service that accepts sealed alert ciphertexts from daemons
and forwards them to the platform push service (APNs). The relay
implementation lives in a separate repository and its source will be
published; anything that implements this contract can serve as the relay a
daemon's `push.relay.url` points at.

The design goal is that the relay is not a trusted party. Every alert is
sealed end to end (a libsodium sealed box: X25519 + XSalsa20-Poly1305) to
each paired device's public key before it leaves the daemon. The relay
handles ciphertext and routing metadata only; the paired device's app
decrypts the payload on the phone, inside its Notification Service
Extension.

All fields described here are versioned under `"v": 1`. Both the relay
envelope and the sealed plaintext carry their own `v`, so either side of the
protocol can evolve independently.

## Inbound request

The daemon sends one HTTP POST per (alert, device) to `push.relay.url`, with
a JSON body:

```json
{
  "v": 1,
  "device": "8f3a1b…",
  "ciphertext": "TmV2ZXIgcGxhaW50ZXh0…",
  "collapseId": "93bc5d02dc2a24b5365347573b6f5115",
  "priority": "time-sensitive",
  "event": false
}
```

| Field | Type | Description |
| --- | --- | --- |
| `v` | int | Protocol version. Always `1`. |
| `device` | string | The platform push token (APNs device token), exactly as the device registered it at pairing time. Opaque to the daemon; the relay uses it to address the notification. |
| `ciphertext` | string | The sealed alert, base64. A libsodium sealed box encrypted to the target device's X25519 public key. At most 3000 characters, so the relay's final APNs JSON stays under the 4096-byte APNs cap with headroom for the relay's own envelope. |
| `collapseId` | string | An opaque coalescing key: 32 lowercase hex characters (a truncated SHA-256 over the alert's identity fields, computed by the daemon). The same alert, reported again or by another node, produces the same id. |
| `priority` | string | `time-sensitive` or `passive`. The relay maps it to the APNs interruption level: `time-sensitive` breaks through scheduled summaries, `passive` does not. |
| `event` | bool | `true` when the alert is a daemon event (the `notify:` fan-out: DAG failures, approval gates, leadership and quorum changes), `false` for job, SLA, and test alerts. Routing metadata only; the event's content is inside the ciphertext. |

The request carries no authentication from the daemon in v1; relay
deployments own their admission policy (network controls, per-device rate
limits keyed on `device`).

## Responses

| Status | Meaning | Daemon behavior |
| --- | --- | --- |
| 2xx | Accepted; the relay has taken responsibility for forwarding. | Success. |
| 429 | Rate limited. | Logged per device (up to 512 bytes of the response body); no retry. |
| Other 4xx | Rejected (malformed body, unknown or unroutable device token). | Logged per device; no retry. |
| 5xx | Relay-side failure. | Logged per device; no retry. |

The daemon never retries a relay POST: delivery semantics past acceptance
(retries toward APNs, dedup, suppression) are the relay's responsibility.
A failed POST is logged and never propagates into the daemon's reporting
path.

## Relay responsibilities

- **Deduplication and coalescing** on (`device`, `collapseId`): several
  nodes of a cluster may report the same alert; the relay collapses them
  without learning what the alert is about.
- **Rate limiting** per device token.
- **Flap suppression**: a job failing and recovering in a tight loop should
  not produce an unbounded notification stream; the relay owns the
  suppression policy, keyed on `collapseId`.
- **APNs forwarding** with `mutable-content` set, so the receiving app's
  Notification Service Extension runs, decrypts the ciphertext, and renders
  the notification locally. The `priority` field maps to the APNs
  interruption level.

## Privacy guarantees

- The relay never sees plaintext. Job names, hostnames, schedules, log
  lines, and event details exist only inside the sealed box, which only the
  target device's private key (generated on the phone and never leaving it)
  can open.
- `collapseId` is a truncated hash of identity fields, not the fields
  themselves: it lets the relay coalesce identical alerts without learning
  the job name or run id behind them.
- Sealing uses an ephemeral sender key per message (anonymous-sender sealed
  box), so the daemon holds no long-lived sending secret worth stealing.

## Sealed plaintext

What the app decrypts is a compact JSON document. Fields other than the
common set are present only when they apply (empty and null values are
omitted).

Common to every alert:

| Field | Type | Description |
| --- | --- | --- |
| `v` | int | Payload version. Always `1`. |
| `kind` | string | `success`, `failure`, `sla`, `event`, or `test`. |
| `name` | string | The job name; the DAG or node name for events; `test` for test alerts. |
| `success` | bool | Whether the reported outcome is a success (`false` for SLA breaches and events). |
| `host` | string | The reporting daemon's host name. |
| `ts` | string | ISO-8601 UTC instant the alert was built. |

Job run context, on any alert where the value is known:

| Field | Type | Description |
| --- | --- | --- |
| `run_id` | string | The run's durable-ledger id. |
| `schedule` | string | The job's schedule as a crontab line. |
| `started_at` | string | ISO-8601 instant the run started. |
| `exit_code` | int | The process exit code. |
| `fail_reason` | string | Why the run counts as failed. |

`kind: success` / `kind: failure` only, when the report's `includeLogTail`
is on and output was captured:

| Field | Type | Description |
| --- | --- | --- |
| `log_tail` | array of string | The last captured output lines (stderr when captured, else stdout), at most 40 lines before size trimming. |

`kind: event` only:

| Field | Type | Description |
| --- | --- | --- |
| `event` | string | The event name (`dag_failure`, `approval_waiting`, `leader_change`, `quorum_loss`). |
| `subject` | string | One-line headline. |
| `message` | string | Body detail. |
| `dag`, `run_key`, `taskkey`, `role`, `leader` | string | Event extras, present when the event carries them. |

`kind: sla` only:

| Field | Type | Description |
| --- | --- | --- |
| `sla_check` | string | The breached check (`maxTimeSinceSuccess`, `lateAfter`, or `maxRuntime`). |
| `threshold_seconds` | number | The configured threshold. |
| `observed_seconds` | number | The measured value that breached it. |
| `last_success_at` | string | ISO-8601 instant of the last known success. |

`kind: test` alerts (from `POST /push/devices/{id}/test`) carry the common
fields plus a fixed `message`.

### Size fitting

The daemon guarantees the sealed, base64-encoded ciphertext never exceeds
3000 characters. When a payload is too large it is shrunk in this order,
re-checking after each step:

1. `log_tail` lines are dropped oldest-first (the newest lines carry the
   failure).
2. Long free-text fields (`message`, `fail_reason`, `subject`) are halved,
   never below 64 characters.
3. Optional context fields (`schedule`, `started_at`, `run_id`) are dropped.

The alert's identity (`name`, `kind`, `host`) is never trimmed.
