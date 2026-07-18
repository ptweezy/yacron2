# Business-Day Schedules (L-n, nW, LW, d#n)

Payroll, billing, and close-of-books jobs run on month-shaped days: "the last weekday", "three days before month-end", "the third Friday". Plain cron cannot say any of those, so cronstable's dialect includes four additive day forms that can:

```yaml
jobs:
  - name: monthly-close-prep
    command: ./stage-the-books
    schedule: "30 1 LW * *"       # the month's last weekday (Mon-Fri)
  - name: payroll-file-transmit
    command: ./send-payroll
    schedule: "0 7 L-3 * *"       # three days before the month's final day
  - name: invoice-day-run
    command: ./bill
    schedule: "0 8 15W * *"       # the weekday nearest the 15th
  - name: board-pack-render
    command: ./render
    schedule: "0 6 * * 5#3"       # the third Friday (also spelled fri#3)
```

## The forms

| Form | Field | Meaning |
|------|-------|---------|
| `L-<n>` | day-of-month | `n` days before the month's final day, `n` in 1 to 30 (`L-1` in January is the 30th; a bare `L` stays the final day itself) |
| `<n>W` | day-of-month | the weekday (Mon-Fri) nearest day `n`, within the same month |
| `LW` | day-of-month | the month's last weekday |
| `<d>#<n>` | day-of-week | the month's `n`-th such weekday, `n` in 1 to 5; `d` is any single weekday value (numeric `0`-`7` with 7 meaning Sunday, or a name like `fri`) |

Every form is an ordinary list item, so they combine freely with plain values and each other: `1,15W,L` in day-of-month, `mon#1,L5` in day-of-week (`L5`, the month's last Friday, predates these forms; `5#3` is its obvious missing sibling).

## Exact edge rules

The rules match Quartz, the scheduler these spellings come from:

- **`<n>W` shifts to the nearest weekday.** A Saturday target resolves to the Friday before; a Sunday target to the Monday after. At the month's edges the shift flips inward so the fire never leaves the month: `1W` on a Saturday 1st fires Monday the 3rd, and `31W` on a Sunday 31st fires Friday the 29th.
- **A target the month never reaches does not fire that month.** `31W` in April behaves like a plain `31`: no April fire. The [schedule linter](Schedule-Linting) warns (`skipped-months`) when every selected month is too short.
- **`L-<n>` can also outrun a month.** `L-30` reaches day 1 only in 31-day months; `L-28` lands in February only when the leap 29th exists, which earns the linter's `leap-day-only` note.
- **`<d>#<n>` skips months without an n-th such weekday.** `5#5` fires only in months with five Fridays. Ordinals run 1 to 5; no month holds six of one weekday.
- **The day-field AND rule applies unchanged.** When day-of-month and day-of-week are both restricted, a day must satisfy both, so `0 0 15W * fri` fires only when the weekday nearest the 15th is itself a Friday. See [Schedules and Timezones](Schedules-and-Timezones).

## Quartz compatibility notes

These forms make most Quartz day expressions paste straight in, `?` included (`0 0 12 ? * MON#2 *` parses verbatim). Two differences remain deliberate:

- **Weekday numbers differ.** Quartz numbers Sunday to Saturday as 1 to 7; this dialect keeps its own 0 to 7 (both 0 and 7 are Sunday). A pasted numeric `6#3` therefore means the third Saturday here, not Quartz's third Friday. Weekday **names** agree in both dialects, so `FRI#3` means the same thing everywhere; prefer names when porting.
- **Last-weekday is spelled `L5`, not `5L`.** A trailing-L Quartz form fails with a hint naming the `L<n>` spelling. Likewise `#` outside day-of-week and `W` outside day-of-month fail with a hint naming the one field each is valid in.

## Where the forms show up

Every schedule surface understands them from the engine's parsed ground truth, not from re-parsing text:

- The plain-English describers, server and dashboard alike: `0 0 L-3 * *` reads "At 00:00, on 3 days before the last day of the month".
- [Schedule linting](Schedule-Linting): the month-reachability checks above, plus the day-field AND warning when combined with a weekday restriction.
- The no-run explainer (`GET /schedule/why`, MCP `cron_why_no_run`) decomposes them per instant: "day-of-month wanted the weekday nearest day 15 (15W)".
- [Schedule pressure](Schedule-Pressure), [duplicate detection](Duplicate-Schedule-Detection) (semantic equality knows `fri#3` equals `5#3`), and the fire previews.
- The [calendar export and week calendar](Calendar-Export), where month-shaped jobs land on the days the engine will actually fire.

See also: [Schedules and Timezones](Schedules-and-Timezones) for the whole dialect, [Hashed Schedules](Hashed-Schedules) for the `H` forms.
