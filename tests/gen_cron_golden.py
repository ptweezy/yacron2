"""Regenerate the cron-expression golden vectors (tests/data/cron_golden.json).

cronstable's in-house cron engine (:mod:`cronstable.cronexpr`) replaced the
third-party ``crontab`` (parse-crontab) dependency.  Its compatibility is
proven by replaying the vectors this script generates: for every expression
in the corpus it records what the OLD library did (parse ok/error,
``next()`` from a grid of fixed instants, ``test()`` over a grid of fixed
datetimes), and ``tests/test_cronexpr.py`` asserts the new engine does
exactly the same.  The vectors are committed, so neither CI nor the install
needs the old package; this script is only for regenerating or extending
them.

Usage (needs the old library, which is NOT a cronstable dependency anymore)::

    pip install "crontab>=1,<2"
    python tests/gen_cron_golden.py

Everything here is deterministic: fixed instants, fixed corpus, sorted keys.
The instants deliberately include DST transitions (America/New_York,
Europe/Berlin), month/year boundaries, the year-field range cap (2099), leap
days, microsecond nows, and both folds of an ambiguous local time.
"""

import datetime
import json
import os
import sys
from zoneinfo import ZoneInfo

try:
    from crontab import CronTab  # the OLD library, dev-only
except ImportError:
    sys.exit(
        "the old `crontab` package is required to regenerate goldens: "
        'pip install "crontab>=1,<2"'
    )

OUT = os.path.join(os.path.dirname(__file__), "data", "cron_golden.json")

# --------------------------------------------------------------------------
# Expression corpus.  Valid, invalid, and deliberately-uncertain forms alike:
# the generator records whatever the old library actually does with each.
# --------------------------------------------------------------------------
EXPRESSIONS = [
    # -- expressions appearing in the repo (tests, wiki, README) --
    "* * * * *",
    "* * * * * * *",
    "*/10 * * * *",
    "*/15 * * * *",
    "*/15 * * * * *",
    "*/15 * * * * * *",
    "*/5  *   * * *",
    "*/5 * * * *",
    "*/5 * 19 7 * 2017",
    "0 * * * *",
    "0 0 * * *",
    "0 0 29 2 *",
    "0 12 * * *",
    "0 12 * * * 2030",
    "0 2 * * *",
    "1 8 * * *",
    "27 19 * * *",
    "30 4 * * mon-fri",
    "49 14 * * *",
    "59 14 * * *",
    # -- @ macros (including ones the old lib rejects) --
    "@yearly",
    "@annually",
    "@monthly",
    "@weekly",
    "@daily",
    "@hourly",
    "@midnight",
    "@reboot",
    "@minutely",
    "@fortnightly",
    # -- minute field forms --
    "1-5 * * * *",
    "1,15,30 * * * *",
    "0-59/30 * * * *",
    "5/15 * * * *",
    "1-9/2 * * * *",
    "*/59 * * * *",
    "*/60 * * * *",
    "50-59/15 * * * *",
    "1-5/10 * * * *",
    "55/2 * * * *",
    "58/5 * * * *",
    "05 * * * *",
    "59 * * * *",
    "60 * * * *",
    "*/0 * * * *",
    "*/1 * * * *",
    "5-1 * * * *",
    "55-5 * * * *",
    "-1 * * * *",
    "1- * * * *",
    "1--5 * * * *",
    "1,,5 * * * *",
    ", * * * *",
    "* * * * *,",
    "*/5/3 * * * *",
    "1.5 * * * *",
    "L * * * *",
    "R * * * *",
    "H * * * *",
    "foo * * * *",
    # -- hour field forms --
    "* 0-6 * * *",
    "* */6 * * *",
    "30 6,18 * * *",
    "* 23 * * *",
    "* 24 * * *",
    "* */23 * * *",
    "* */24 * * *",
    # -- day-of-month field forms --
    "0 0 1 * *",
    "0 0 L * *",
    "0 0 l * *",
    "0 0 1,L * *",
    "0 0 1,15,31 * *",
    "0 0 */7 * *",
    "0 0 28-31 * *",
    "0 0 31 * *",
    "0 0 0 * *",
    "0 0 32 * *",
    "0 0 */31 * *",
    "0 0 */32 * *",
    "0 0 L-2 * *",
    "0 0 LW * *",
    "0 0 15W * *",
    "0 0 l,15 * *",
    "0 0 l/2 * *",
    # -- month field forms --
    "0 0 1 jan *",
    "0 0 1 JAN *",
    "0 0 1 jan,jul *",
    "0 0 * jan-jun *",
    "0 0 * */3 *",
    "0 0 1 2 *",
    "0 0 * dec *",
    "0 0 1 3/4 *",
    "0 0 * 13 *",
    "0 0 * 0 *",
    "0 0 * */12 *",
    "0 0 * */13 *",
    "0 0 1 mon *",
    # -- day-of-week field forms --
    "0 0 * * 0",
    "0 0 * * 7",
    "0 0 * * 0,7",
    "0 0 * * sun",
    "0 0 * * SUN",
    "0 0 * * mon,wed,fri",
    "0 0 * * mon-fri/2",
    "0 0 * * sat-sun",
    "0 0 * * sun-sat",
    "0 0 * * mon-sun",
    "0 0 * * fri-mon",
    "0 0 * * 6-7",
    "0 0 * * 6-0",
    "0 0 * * 0-7",
    "0 0 * * 7-7",
    "0 0 * * 4-7",
    "0 0 * * 4-6/2",
    "0 0 * * 4-7/3",
    "0 0 * * */2",
    "0 0 * * */6",
    "0 0 * * */7",
    "0 0 * * */8",
    "0 0 * * 5/2",
    "0 0 * * 7/2",
    "0 0 * * 8",
    "0 0 * * 7-8",
    "0 0 * * L5",
    "0 0 * * l5",
    "0 0 * * L0",
    "0 0 * * L7",
    "0 0 * * L5,L1",
    "0 0 * * L5-6",
    "0 0 * * L5-L6",
    "0 0 * * Lmon",
    "0 0 * * 5L",
    "0 0 * * L",
    "0 0 * * 1#2",
    "0 0 * * 1,L5",
    "0 0 * * l0-1",
    "0 0 * * L6-7",
    "0 0 * * l5-6/2",
    "0 0 * * l7-3",
    "0 0 * * l3-1",
    "0 0 * * l1-7",
    "0 0 * * l0-0",
    "0 0 * * l*",
    "0 0 * * l8",
    "0 0 * * l1-8",
    "0 0 * * l05",
    "0 0 * * mon-5",
    "0 0 * * 0-0",
    "0 0 * * sun-sun",
    # -- day-of-month + day-of-week interplay (old lib: AND, unlike Vixie) --
    "0 0 13 * 5",
    "0 0 13 * *",
    "30 4 1,15 * 5",
    # -- year field forms --
    "0 0 1 1 * 2027",
    "0 0 1 1 * 2027,2029",
    "0 0 1 1 * 2027-2029/2",
    "0 0 1 1 * */5",
    "0 0 1 1 * 1970/5",
    "0 0 1 1 * */129",
    "0 0 1 1 * */130",
    "0 0 1 1 * 2099",
    "0 0 1 1 * 1970",
    "0 0 1 1 * 2020",
    "0 0 1 1 * 1969",
    "0 0 1 1 * 2100",
    "0 0 29 2 * 2025",
    "* * * * * 2026",
    "0 0 1 1 * 2026-2100",
    "0 0 31 11 * *",
    "0 12 * * mon-fri 2026-2028",
    "0 0 1 1 2027",
    # -- dates that never (or rarely) exist: next() must terminate --
    "0 0 29 2 * *",
    "0 0 30 2 * *",
    "0 0 31 2,4 * *",
    "0 0 31 4,6,9,11 * *",
    "0 0 L 2 *",
    # -- 7-field (seconds) forms --
    "*/2 * * * * * *",
    "0,30 * * * * * *",
    "59 59 23 31 12 * 2099",
    "30 */5 * * * * *",
    "15-45/10 30 12 * * * *",
    "59 * * * * * *",
    "60 * * * * * *",
    # -- whitespace tolerance --
    "  */5   *  * * *  ",
    "*\t* * * *",
    "30 6 * * *\n",
    "\n30 6 * * *",
    "",
    "* * * *",
    "* * * * * * * *",
]

# Fixed naive instants for next() (ISO strings; microseconds allowed).
NAIVE_NEXT_NOWS = [
    "2026-01-07T12:00:00",
    "2026-01-07T12:00:30",
    "2026-01-07T12:00:30.500000",
    "2026-01-07T12:00:00.000001",
    "2026-01-07T11:59:59",
    "2026-01-11T00:00:00",  # a Sunday, exactly midnight
    "2026-01-31T23:59:00",  # month end
    "2026-12-31T23:59:30",  # year end
    "2026-02-28T12:00:00",
    "2028-02-28T23:59:00",  # leap year, day before Feb 29
    "2026-03-08T01:59:00",  # US spring-forward day (as civil time)
    "2026-11-01T00:30:00",  # US fall-back day (as civil time)
    "2097-06-15T00:00:00",  # near the year-field cap
    "2099-12-31T23:58:00",  # at the year-field cap
    "2099-12-31T23:59:30",  # crossing the cap: does next() enter 2100?
    "1970-01-01T00:00:00",
]

# Fixed aware instants for next(): civil time in tz, with fold.
AWARE_NEXT_NOWS = [
    {"tz": "UTC", "dt": "2026-01-07T12:00:30", "fold": 0},
    {"tz": "America/New_York", "dt": "2026-01-07T12:00:00", "fold": 0},
    {"tz": "America/New_York", "dt": "2026-03-08T01:00:00", "fold": 0},
    {"tz": "America/New_York", "dt": "2026-03-08T01:59:00", "fold": 0},
    {"tz": "America/New_York", "dt": "2026-11-01T00:00:00", "fold": 0},
    {"tz": "America/New_York", "dt": "2026-11-01T01:45:00", "fold": 0},
    {"tz": "America/New_York", "dt": "2026-11-01T01:45:00", "fold": 1},
    {"tz": "Europe/Berlin", "dt": "2026-03-29T01:59:00", "fold": 0},
    {"tz": "Europe/Berlin", "dt": "2026-10-25T02:30:00", "fold": 0},
    {"tz": "Europe/Berlin", "dt": "2026-10-25T02:30:00", "fold": 1},
]

# Fixed datetimes for test().
TEST_DTS = [
    "2026-01-07T12:00:00",
    "2026-01-07T12:05:00",
    "2026-01-07T12:05:15",
    "2026-01-07T12:05:16",
    "2026-01-07T12:05:30",
    "2026-01-07T12:05:00.500000",
    "2026-01-10T00:00:00",  # Sat
    "2026-01-11T00:00:00",  # Sun
    "2026-01-12T00:00:00",  # Mon
    "2026-01-13T00:00:00",  # Tue (the 13th)
    "2026-01-14T00:00:00",  # Wed
    "2026-01-16T00:00:00",  # Fri (dow-only match for `13 * 5`)
    "2026-02-13T00:00:00",  # Friday the 13th (dom AND dow)
    "2026-01-23T00:00:00",  # a Friday, not the last
    "2026-01-25T00:00:00",  # last Sunday of Jan
    "2026-01-26T00:00:00",  # last Monday of Jan
    "2026-01-30T00:00:00",  # last Friday of Jan
    "2026-01-31T00:00:00",  # last day of Jan (a Saturday)
    "2026-02-28T00:00:00",  # last day of Feb (non-leap)
    "2028-02-29T00:00:00",  # leap day
    "2026-04-01T00:00:00",
    "2026-05-01T00:00:00",
    "2026-07-01T00:00:00",
    "2026-08-01T00:00:00",
    "2027-01-01T00:00:00",
    "2030-01-01T00:00:00",
    "2026-01-01T00:00:00",
    "2026-03-01T00:00:00",
    "2026-06-15T04:00:00",
    "2026-06-15T18:30:00",
    "2150-01-01T00:00:00",  # beyond the year-field cap
]


def _aware(spec):
    dt = datetime.datetime.fromisoformat(spec["dt"])
    return dt.replace(tzinfo=ZoneInfo(spec["tz"]), fold=spec["fold"])


def main() -> None:
    exprs = {}
    for expr in EXPRESSIONS:
        try:
            ct = CronTab(expr)
        except ValueError:
            exprs[expr] = {"ok": False}
            continue
        entry = {"ok": True}
        entry["next"] = [
            ct.next(now=datetime.datetime.fromisoformat(s), default_utc=True)
            for s in NAIVE_NEXT_NOWS
        ]
        entry["aware_next"] = [
            ct.next(now=_aware(spec), default_utc=False)
            for spec in AWARE_NEXT_NOWS
        ]
        entry["test"] = [
            ct.test(datetime.datetime.fromisoformat(s)) for s in TEST_DTS
        ]
        exprs[expr] = entry

    # Semantic equality pairs: the old CronTab compares by expansion, not by
    # source string.  Record representative verdicts so the new engine's
    # __eq__ is held to the same standard.
    eq_pairs = [
        ["*/5 * * * *", "0-59/5 * * * *"],
        ["*/5 * * * *", "*/5  *   * * *"],
        ["@daily", "0 0 * * *"],
        ["@weekly", "0 0 * * 0"],
        ["@yearly", "0 0 1 1 *"],
        ["0 0 * * 7", "0 0 * * 0"],
        ["0 0 * * sat-sun", "0 0 * * 6-7"],
        ["*/5 * * * *", "0 0 * * *"],
        ["* * * * *", "* * * * * * *"],
        ["0 0 * * *", "0 0 * * * 2026"],
        ["0 0 1 1 * 2027", "0 0 1 1 * 2027"],
    ]
    equality = [
        {"a": a, "b": b, "equal": CronTab(a) == CronTab(b)}
        for a, b in eq_pairs
    ]

    out = {
        "_comment": (
            "Golden vectors recorded from the legacy `crontab` "
            "(parse-crontab) package. Regenerate with "
            "tests/gen_cron_golden.py; do not edit by hand."
        ),
        "naive_next_nows": NAIVE_NEXT_NOWS,
        "aware_next_nows": AWARE_NEXT_NOWS,
        "test_dts": TEST_DTS,
        "equality": equality,
        "exprs": exprs,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=1, sort_keys=True)
        f.write("\n")
    n_ok = sum(1 for e in exprs.values() if e["ok"])
    print(
        "wrote {}: {} expressions ({} valid, {} invalid), "
        "{} next-instants, {} aware-instants, {} test-instants".format(
            OUT,
            len(exprs),
            n_ok,
            len(exprs) - n_ok,
            len(NAIVE_NEXT_NOWS),
            len(AWARE_NEXT_NOWS),
            len(TEST_DTS),
        )
    )


if __name__ == "__main__":
    main()
