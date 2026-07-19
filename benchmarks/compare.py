#!/usr/bin/env python3
"""Compare two benchmark runs, render the release chart, gate regressions.

Consumes JSON documents written by benchmarks/bench.py.  Both sides accept
several files (the CI job runs the suites in interleaved rounds on one
runner); per metric the rounds are merged with the metric's own estimator
("min" for time, "median" for memory) so a single noisy round cannot fake or
mask a regression.

Outputs:
  --md PATH          markdown summary (release-notes section)
  --svg PATH         diverging bar chart of the largest changes
  --merged-out PATH  the merged current-side results as one JSON document

Gating: a metric fails when it slows down by more than its declared gate
percentage AND by more than its absolute floor (so microsecond jitter on a
sub-millisecond metric never gates).  Failures exit 1 unless --warn-only
(ordinary commits) or --accept (an acknowledged, intentional regression).

Usage:
    python benchmarks/compare.py \
        --baseline perf/old.*.json --current perf/new.*.json \
        --md perf-summary.md --svg perf-chart.svg
"""

import argparse
import json
import statistics
import sys

# Chart palette: the validated reference dataviz palette (diverging blue/red
# pair, ink and chrome tokens), light and dark, selected per mode.  The dark
# block is applied by prefers-color-scheme inside the SVG; renderers without
# media-query support fall back to the self-contained light card.
_LIGHT = {
    "surface": "#fcfcfb",
    "border": "rgba(11,11,11,0.10)",
    "grid": "#e1e0d9",
    "baseline": "#c3c2b7",
    "ink1": "#0b0b0b",
    "ink2": "#52514e",
    "muted": "#898781",
    "faster": "#2a78d6",
    "slower": "#e34948",
}
_DARK = {
    "surface": "#1a1a19",
    "border": "rgba(255,255,255,0.10)",
    "grid": "#2c2c2a",
    "baseline": "#383835",
    "ink1": "#ffffff",
    "ink2": "#c3c2b7",
    "muted": "#898781",
    "faster": "#3987e5",
    "slower": "#e66767",
}

_MAX_CHART_ROWS = 16


def _fmt(value, unit):
    if unit == "MB":
        return "%.2f MB" % value
    if value < 0.001:
        return "%.1f us" % (value * 1e6)
    if value < 1.0:
        return "%.2f ms" % (value * 1e3)
    return "%.3f s" % value


def _load(paths):
    docs = []
    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            docs.append(json.load(f))
    return docs


def _merge(docs):
    """Merge several rounds into one {name: entry} map.

    The entry's stats are recomputed over every round's raw repeats; `value`
    uses the metric's declared estimator across rounds.
    """
    merged = {}
    for doc in docs:
        for entry in doc.get("results", []):
            name = entry["name"]
            slot = merged.setdefault(name, {"entry": None, "round_values": []})
            if entry.get("skipped"):
                slot.setdefault("skip_reason", entry.get("reason"))
                continue
            if slot["entry"] is None:
                slot["entry"] = dict(entry)
            slot["round_values"].append(entry["value"])
    out = {}
    for name, slot in merged.items():
        if slot["entry"] is None:
            out[name] = {
                "name": name,
                "skipped": True,
                "reason": slot.get("skip_reason", "skipped"),
            }
            continue
        entry = slot["entry"]
        vals = slot["round_values"]
        entry["value"] = (
            min(vals)
            if entry.get("compare") == "min"
            else statistics.median(vals)
        )
        entry["round_values"] = vals
        out[name] = entry
    return out


def _delta_pct(base, cur):
    if base <= 0:
        return None
    return (cur - base) / base * 100.0


def _compare(baseline, current):
    """Per-metric comparison rows plus gate violations."""
    rows = []
    violations = []
    for name, cur in current.items():
        if cur.get("skipped"):
            continue
        base = baseline.get(name)
        if base is None or base.get("skipped"):
            rows.append(
                {
                    "name": name,
                    "entry": cur,
                    "base_value": None,
                    "delta_pct": None,
                    "gated": False,
                }
            )
            continue
        delta = _delta_pct(base["value"], cur["value"])
        gated = False
        gate_pct = cur.get("gate_pct")
        if (
            delta is not None
            and gate_pct is not None
            and delta > gate_pct
            and (cur["value"] - base["value"]) > (cur.get("gate_floor") or 0.0)
        ):
            gated = True
            violations.append(
                "%s regressed %+.1f%% (%s to %s, gate %.0f%%)"
                % (
                    name,
                    delta,
                    _fmt(base["value"], cur["unit"]),
                    _fmt(cur["value"], cur["unit"]),
                    gate_pct,
                )
            )
        rows.append(
            {
                "name": name,
                "entry": cur,
                "base_value": base["value"],
                "delta_pct": delta,
                "gated": gated,
            }
        )
    return rows, violations


# ---------------------------------------------------------------------------
# SVG chart
# ---------------------------------------------------------------------------


def _nice_limit(max_abs):
    for candidate in (5, 10, 15, 20, 30, 40, 50):
        if max_abs <= candidate:
            return candidate
    return 50


def _bar_path(x0, y, length, height, rightward):
    """A bar from the zero baseline: square there, 4px rounded data-end."""
    r = min(4.0, abs(length))
    h = height
    if rightward:
        return (
            "M%.1f,%.1f h%.1f a%.1f,%.1f 0 0 1 %.1f,%.1f v%.1f "
            "a%.1f,%.1f 0 0 1 -%.1f,%.1f h-%.1f z"
            % (
                x0,
                y,
                length - r,
                r,
                r,
                r,
                r,
                h - 2 * r,
                r,
                r,
                r,
                r,
                length - r,
            )
        )
    return (
        "M%.1f,%.1f h-%.1f a%.1f,%.1f 0 0 0 -%.1f,%.1f v%.1f "
        "a%.1f,%.1f 0 0 0 %.1f,%.1f h%.1f z"
        % (x0, y, length - r, r, r, r, r, h - 2 * r, r, r, r, r, length - r)
    )


def build_svg(rows, base_label, cur_label):
    """Diverging horizontal bars: % runtime change per metric, vs baseline."""
    comparable = [r for r in rows if r["delta_pct"] is not None]
    shown = sorted(comparable, key=lambda r: -abs(r["delta_pct"]))
    shown = shown[:_MAX_CHART_ROWS]
    shown.sort(key=lambda r: -r["delta_pct"])
    omitted = len(comparable) - len(shown)

    width = 860
    gutter = 230
    plot_right = width - 96
    center = gutter + (plot_right - gutter) / 2.0
    half = (plot_right - gutter) / 2.0 - 4
    row_h = 24
    bar_h = 12
    top = 78
    plot_bottom = top + len(shown) * row_h
    height = plot_bottom + (46 if omitted > 0 else 30)

    limit = _nice_limit(max((abs(r["delta_pct"]) for r in shown), default=5.0))
    scale = half / limit

    css = (
        "svg{color-scheme:light dark;"
        "font-family:system-ui,-apple-system,'Segoe UI',sans-serif}"
        ".surface{fill:%(surface)s;stroke:%(border)s}"
        ".t1{fill:%(ink1)s}.t2{fill:%(ink2)s}.t3{fill:%(muted)s}"
        ".num{font-variant-numeric:tabular-nums}"
        ".grid{stroke:%(grid)s;stroke-width:1}"
        ".zero{stroke:%(baseline)s;stroke-width:1}"
        ".fast{fill:%(faster)s}.slow{fill:%(slower)s}" % _LIGHT
    )
    dark_css = (
        "@media(prefers-color-scheme:dark){"
        ".surface{fill:%(surface)s;stroke:%(border)s}"
        ".t1{fill:%(ink1)s}.t2{fill:%(ink2)s}.t3{fill:%(muted)s}"
        ".grid{stroke:%(grid)s}"
        ".zero{stroke:%(baseline)s}"
        ".fast{fill:%(faster)s}.slow{fill:%(slower)s}}" % _DARK
    )

    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
        'viewBox="0 0 %d %d" role="img" '
        'aria-label="Performance change per benchmark, %s vs %s">'
        % (width, height, width, height, cur_label, base_label),
        "<style>%s%s</style>" % (css, dark_css),
        '<rect class="surface" x="0.5" y="0.5" width="%d" height="%d" rx="8"/>'
        % (width - 1, height - 1),
        '<text class="t1" x="20" y="30" font-size="14" font-weight="600">'
        "cronstable %s: performance vs %s</text>" % (cur_label, base_label),
        '<text class="t2" x="20" y="50" font-size="11">'
        "% change in runtime and memory per benchmark. Lower is better; "
        "bars left of zero are faster than the previous release.</text>",
        # Legend: identity for the two directions, text in ink tokens.
        '<rect class="fast" x="%d" y="21" width="10" height="10" rx="2"/>'
        % (width - 180),
        '<text class="t2" x="%d" y="30" font-size="11">faster</text>'
        % (width - 165),
        '<rect class="slow" x="%d" y="21" width="10" height="10" rx="2"/>'
        % (width - 112),
        '<text class="t2" x="%d" y="30" font-size="11">slower</text>'
        % (width - 97),
    ]

    for tick in (-limit, -limit / 2.0, limit / 2.0, limit):
        x = center + tick * scale
        parts.append(
            '<line class="grid" x1="%.1f" y1="%d" x2="%.1f" y2="%d"/>'
            % (x, top - 6, x, plot_bottom + 4)
        )
        parts.append(
            '<text class="t3 num" x="%.1f" y="%d" font-size="10" '
            'text-anchor="middle">%+g%%</text>' % (x, plot_bottom + 18, tick)
        )
    parts.append(
        '<line class="zero" x1="%.1f" y1="%d" x2="%.1f" y2="%d"/>'
        % (center, top - 6, center, plot_bottom + 4)
    )
    parts.append(
        '<text class="t3 num" x="%.1f" y="%d" font-size="10" '
        'text-anchor="middle">0</text>' % (center, plot_bottom + 18)
    )

    for i, row in enumerate(shown):
        y_mid = top + i * row_h + row_h / 2.0
        y_bar = y_mid - bar_h / 2.0
        delta = row["delta_pct"]
        clamped = max(-limit, min(limit, delta))
        length = abs(clamped) * scale
        parts.append(
            '<text class="t2" x="%d" y="%.1f" font-size="11" '
            'text-anchor="end">%s</text>'
            % (gutter - 10, y_mid + 4, row["name"])
        )
        label = "%+.1f%%" % delta
        if row["gated"]:
            label += " (gate)"
        if length >= 0.75:
            cls = "slow" if delta > 0 else "fast"
            parts.append(
                '<path class="%s" d="%s"/>'
                % (cls, _bar_path(center, y_bar, length, bar_h, delta > 0))
            )
        if delta > 0:
            parts.append(
                '<text class="t2 num" x="%.1f" y="%.1f" font-size="10">'
                "%s</text>" % (center + length + 6, y_mid + 4, label)
            )
        else:
            parts.append(
                '<text class="t2 num" x="%.1f" y="%.1f" font-size="10" '
                'text-anchor="end">%s</text>'
                % (center - length - 6, y_mid + 4, label)
            )

    if omitted > 0:
        parts.append(
            '<text class="t3" x="20" y="%d" font-size="10">'
            "%d largest changes shown; %d further metrics changed less "
            "(full table in the release notes).</text>"
            % (plot_bottom + 36, len(shown), omitted)
        )
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


# ---------------------------------------------------------------------------
# Markdown summary
# ---------------------------------------------------------------------------


def build_md(
    rows,
    violations,
    base_label,
    cur_label,
    baseline_missing,
    current,
    baseline,
    img_url=None,
    accept=False,
):
    lines = ["### Performance vs %s" % (base_label or "(no baseline)")]
    lines.append("")
    if baseline_missing:
        lines.append(
            "No previous release baseline exists, so this release records "
            "benchmark results without a comparison. The next release will "
            "diff against these numbers."
        )
        lines.append("")
    if img_url:
        lines.append(
            "![Performance change per benchmark, %s vs %s](%s)"
            % (cur_label, base_label, img_url)
        )
        lines.append("")
    if not baseline_missing:
        if violations and accept:
            lines.append(
                "**Gate: regressions accepted** (a `[perf:accept]` marker "
                "acknowledged them):"
            )
            lines.extend("- %s" % v for v in violations)
        elif violations:
            lines.append("**Gate: FAILED**")
            lines.extend("- %s" % v for v in violations)
        else:
            lines.append(
                "**Gate: passed.** No metric exceeded its regression limit."
            )
        lines.append("")
        lines.append(
            "Both versions ran interleaved on one runner; time metrics "
            "compare best-of-rounds, memory metrics compare medians. "
            "Negative change is faster or smaller."
        )
        lines.append("")

        comparable = [r for r in rows if r["delta_pct"] is not None]
        comparable.sort(key=lambda r: -abs(r["delta_pct"]))
        lines.append("<details>")
        lines.append(
            "<summary>All benchmark results (%d metrics)</summary>"
            % len(comparable)
        )
        lines.append("")
        lines.append(
            "| Benchmark | %s | %s | Change |" % (base_label, cur_label)
        )
        lines.append("|---|---:|---:|---:|")
        for row in comparable:
            entry = row["entry"]
            mark = " **(gate)**" if row["gated"] else ""
            lines.append(
                "| %s | %s | %s | %+.1f%%%s |"
                % (
                    row["name"],
                    _fmt(row["base_value"], entry["unit"]),
                    _fmt(entry["value"], entry["unit"]),
                    row["delta_pct"],
                    mark,
                )
            )
        lines.append("")
        lines.append("</details>")

        new_metrics = [r["name"] for r in rows if r["delta_pct"] is None]
        if new_metrics:
            lines.append("")
            lines.append(
                "New in this release (no baseline yet): %s."
                % ", ".join(sorted(new_metrics))
            )
        dropped = sorted(
            name
            for name, entry in baseline.items()
            if not entry.get("skipped")
            and (name not in current or current[name].get("skipped"))
        )
        if dropped:
            lines.append("")
            lines.append(
                "Measured in %s but not in this run: %s."
                % (base_label, ", ".join(dropped))
            )
    else:
        lines.append("| Benchmark | %s |" % cur_label)
        lines.append("|---|---:|")
        for name in sorted(current):
            entry = current[name]
            if entry.get("skipped"):
                continue
            lines.append(
                "| %s | %s |" % (name, _fmt(entry["value"], entry["unit"]))
            )
    lines.append("")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--baseline", nargs="*", default=[])
    parser.add_argument("--current", nargs="+", required=True)
    parser.add_argument("--baseline-label", default=None)
    parser.add_argument("--current-label", default=None)
    parser.add_argument("--md")
    parser.add_argument("--svg")
    parser.add_argument("--merged-out")
    parser.add_argument("--img-url")
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="report gate failures as warnings, never exit nonzero",
    )
    parser.add_argument(
        "--accept",
        action="store_true",
        help="acknowledge regressions ([perf:accept]): report but pass",
    )
    args = parser.parse_args(argv)

    current_docs = _load(args.current)
    current = _merge(current_docs)
    cur_label = args.current_label or current_docs[0].get(
        "cronstable_version", "current"
    )

    baseline_missing = not args.baseline
    baseline = {}
    base_label = args.baseline_label
    if not baseline_missing:
        baseline_docs = _load(args.baseline)
        baseline = _merge(baseline_docs)
        base_label = base_label or baseline_docs[0].get(
            "cronstable_version", "baseline"
        )

    rows, violations = _compare(baseline, current)

    if args.merged_out:
        merged_doc = dict(current_docs[0])
        merged_doc["results"] = [current[name] for name in sorted(current)]
        merged_doc["merged_from"] = len(current_docs)
        with open(args.merged_out, "w", encoding="utf-8") as f:
            json.dump(merged_doc, f, indent=1, sort_keys=True)
            f.write("\n")

    if args.svg and not baseline_missing:
        with open(args.svg, "w", encoding="utf-8") as f:
            f.write(build_svg(rows, base_label, cur_label))

    if args.md:
        with open(args.md, "w", encoding="utf-8") as f:
            f.write(
                build_md(
                    rows,
                    violations,
                    base_label,
                    cur_label,
                    baseline_missing,
                    current,
                    baseline,
                    img_url=args.img_url,
                    accept=args.accept,
                )
            )

    comparable = sum(1 for r in rows if r["delta_pct"] is not None)
    print(
        "compared %d metrics (%s vs %s): %d gate violation(s)"
        % (comparable, cur_label, base_label or "no baseline", len(violations))
    )
    for violation in violations:
        if args.accept or args.warn_only:
            print("::warning::perf gate: %s" % violation)
        else:
            print("::error::perf gate: %s" % violation)

    if violations and not args.warn_only and not args.accept:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
