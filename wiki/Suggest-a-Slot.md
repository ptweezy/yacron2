# Suggest a Slot

Where should the next job go? cronstable answers from the fleet's real fires: it walks every enabled schedule over the next 24 hours (the same enumeration behind [Schedule Pressure](Schedule-Pressure)) and recommends the least-loaded slot.

```
GET /schedule/suggest?period=hourly
GET /schedule/suggest?period=daily&tz=Europe/London
```

`period=hourly` picks a minute of the hour (a `<m> * * * *` schedule); `period=daily` picks a minute and hour (`<m> <h> * * *`). `tz` frames the daily pick (default UTC).

```json
{
  "period": "hourly",
  "minute": 29,
  "expression": "29 * * * *",
  "fires_in_window": 0,
  "busiest": { "minute": 0, "fires_in_window": 851 },
  "alternatives": [
    { "minute": 31, "expression": "31 * * * *", "fires_in_window": 0 },
    { "minute": 28, "expression": "28 * * * *", "fires_in_window": 0 }
  ],
  "based_on": { "jobs": 41, "start": "2026-07-18T16:20:00+00:00", "hours": 24 },
  "hash_hint": "H * * * *"
}
```

The choice is deterministic, so the same fleet always gets the same answer: least fires first, ties broken toward the slot circularly farthest from the busiest one, then toward the earliest slot. That tie-break is why an idle fleet is told `:30`, not `:00`: the outside world stampedes at the top of the hour even when your fleet does not. `busiest` is included for contrast, `alternatives` are the two runners-up, and `hash_hint` names the [`H` spelling](Hashed-Schedules) that would keep future jobs spreading themselves without anyone consulting this endpoint again.

The same analyzer backs the `cron_suggest_slot` [MCP tool](MCP), so an agent asked to "add a cleanup job" can pick a schedule that does not pile onto the herd.

## In the dashboards

The [web dashboard](Web-Dashboard)'s schedule-pressure card has "suggest an hourly slot" and "suggest a daily slot" buttons; the suggested expression is a chip you click to copy. The [terminal dashboard](Terminal-Dashboard)'s pressure overlay shows both suggestions inline, computed locally from the same shared analyzer.

## Suggest versus H

Both solve the same collision problem from different ends. A suggested slot is explicit: the schedule reads as a concrete minute, at the cost of being a point-in-time answer that no one re-balances later. An [`H` hashed slot](Hashed-Schedules) is self-maintaining: every job spreads itself, at the cost of the minute living in the hash rather than the config text. New fleets tend to standardize on `H`; established fleets use suggest to place jobs that must keep an explicit, reviewable schedule.
