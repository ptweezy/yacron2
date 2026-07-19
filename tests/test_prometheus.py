"""Tests for the native Prometheus metrics endpoint (cronstable.prometheus).

Three tiers, mirroring the web-API tests in test_cron.py: pure unit tests
for the exposition renderer and the accumulator registry, direct handler
calls with a hand-rolled fake request, and real-HTTP tests that stand the
server up via start_stop_web_app and scrape it like Prometheus would.
"""

import datetime
import math

import pytest

import cronstable.cron
from cronstable.cron import Cron, JobRunInfo
from cronstable.job import JobOutputStream
from cronstable.prometheus import (
    CONTENT_TYPE_OPENMETRICS,
    CONTENT_TYPE_TEXT,
    DEFAULT_DURATION_BUCKETS,
    MetricFamily,
    PrometheusMetrics,
    escape_label_value,
    format_value,
    render_families,
    resolve_metrics_config,
)
from tests._commands import cmd_print, cmd_sleep, yaml_command


def sample_value(text, name, **labels):
    """Return the value of the sample ``name{labels...}`` in ``text``.

    Label order in the exposition is an implementation detail, so match on
    the parsed label set, not the literal line.
    """
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(name):
            continue
        rest = line[len(name):]
        if rest.startswith("{"):
            label_str, _, value_str = rest[1:].partition("} ")
            found = {}
            for part in label_str.split(","):
                key, _, val = part.partition("=")
                found[key] = val.strip('"')
            if found != {k: str(v) for k, v in labels.items()}:
                continue
            return float(value_str)
        elif rest.startswith(" ") and not labels:
            return float(rest.strip())
    return None


# ---------------------------------------------------------------------------
# exposition renderer
# ---------------------------------------------------------------------------


def test_escape_label_value():
    assert escape_label_value('a"b\\c\nd') == 'a\\"b\\\\c\\nd'
    # a carriage return is escaped too: a raw CR is not valid inside a quoted
    # label value in the OpenMetrics exposition grammar.
    assert escape_label_value("a\rb") == "a\\rb"


def test_format_value():
    assert format_value(0) == "0"
    assert format_value(3.0) == "3"
    assert format_value(-2.5) == "-2.5"
    assert format_value(math.inf) == "+Inf"
    assert format_value(-math.inf) == "-Inf"
    assert format_value(math.nan) == "NaN"
    # timestamps must keep full precision, not scientific notation
    assert format_value(1783009182.2757554) == "1783009182.2757554"


def test_render_counter_text_format():
    fam = MetricFamily("app_runs", "counter", "Runs.")
    fam.add({"job_name": "a"}, 3)
    text = render_families([fam])
    assert "# HELP app_runs_total Runs." in text
    assert "# TYPE app_runs_total counter" in text
    assert 'app_runs_total{job_name="a"} 3' in text
    assert "# EOF" not in text


def test_render_counter_openmetrics():
    fam = MetricFamily("app_runs", "counter", "Runs.")
    fam.add({}, 1)
    text = render_families([fam], openmetrics=True)
    # OpenMetrics names the family without the _total suffix, but the
    # sample itself still carries it; the output ends with # EOF.
    assert "# TYPE app_runs counter" in text
    assert "app_runs_total 1" in text
    assert text.endswith("# EOF\n")


def test_render_info_metric():
    fam = MetricFamily("app", "info", "Build info.")
    fam.add({"version": "1.0"}, 1)
    text = render_families([fam])
    assert "# TYPE app_info gauge" in text
    assert 'app_info{version="1.0"} 1' in text
    om = render_families([fam], openmetrics=True)
    assert "# TYPE app info" in om
    assert 'app_info{version="1.0"} 1' in om


def test_render_skips_empty_families():
    fam = MetricFamily("app_empty", "gauge", "Nothing.")
    assert "app_empty" not in render_families([fam])


def test_render_escapes_help_and_labels():
    fam = MetricFamily("app_g", "gauge", "line1\nline2")
    fam.add({"name": 'quo"te'}, 1)
    text = render_families([fam])
    assert "# HELP app_g line1\\nline2" in text
    assert 'app_g{name="quo\\"te"} 1' in text


def test_render_escapes_help_quotes_openmetrics_only():
    # the OpenMetrics ABNF forbids a raw double quote in HELP text, while
    # the classic text format treats \" as two literal characters -- so
    # the quote is escaped only on the OpenMetrics rendering.
    fam = MetricFamily("app_g", "gauge", 'say "hi"')
    fam.add({}, 1)
    assert '# HELP app_g say "hi"' in render_families([fam])
    assert '# HELP app_g say \\"hi\\"' in render_families(
        [fam], openmetrics=True
    )


# ---------------------------------------------------------------------------
# web.metrics config resolution
# ---------------------------------------------------------------------------


def test_resolve_metrics_config_default_on():
    resolved = resolve_metrics_config({"listen": ["http://127.0.0.1:0"]})
    assert resolved == {
        "public": False,
        "durationBuckets": DEFAULT_DURATION_BUCKETS,
    }


def test_resolve_metrics_config_bool_shorthand():
    assert resolve_metrics_config({"metrics": False}) is None
    resolved = resolve_metrics_config({"metrics": True})
    assert resolved is not None and resolved["public"] is False


def test_resolve_metrics_config_map_form():
    assert resolve_metrics_config({"metrics": {"enabled": False}}) is None
    resolved = resolve_metrics_config(
        {"metrics": {"public": True, "durationBuckets": [1.0, 10.0]}}
    )
    assert resolved == {"public": True, "durationBuckets": (1.0, 10.0)}


# ---------------------------------------------------------------------------
# accumulator registry
# ---------------------------------------------------------------------------


def _registry_text(metrics):
    """Render ``metrics`` against a job-less Cron (accumulators only)."""
    cron = Cron(None)
    cron.metrics = metrics
    return metrics.render(cron)


def test_registry_counts_runs_and_outcome_timestamps():
    metrics = PrometheusMetrics()
    metrics.job_run_recorded("j", "success", 2.0)
    metrics.job_run_recorded("j", "failure", 400.0)
    metrics.job_run_recorded("j", "cancelled", None)
    text = _registry_text(metrics)
    assert sample_value(
        text, "cronstable_job_runs_total", job_name="j", status="success"
    ) == 1
    assert sample_value(
        text, "cronstable_job_runs_total", job_name="j", status="failure"
    ) == 1
    assert sample_value(
        text, "cronstable_job_runs_total", job_name="j", status="cancelled"
    ) == 1
    # both last-outcome timestamps were stamped
    assert sample_value(
        text, "cronstable_job_last_success_timestamp_seconds", job_name="j"
    ) is not None
    assert sample_value(
        text, "cronstable_job_last_failure_timestamp_seconds", job_name="j"
    ) is not None
    # histogram: 2.0 lands in le=5.0 and later, 400.0 only from le=900;
    # the cancelled run carried no duration.
    assert sample_value(
        text, "cronstable_job_duration_seconds_bucket", job_name="j", le="1.0"
    ) == 0
    assert sample_value(
        text, "cronstable_job_duration_seconds_bucket", job_name="j", le="5.0"
    ) == 1
    assert (
        sample_value(
            text,
            "cronstable_job_duration_seconds_bucket",
            job_name="j",
            le="900.0",
        )
        == 2
    )
    assert (
        sample_value(
            text,
            "cronstable_job_duration_seconds_bucket",
            job_name="j",
            le="+Inf",
        )
        == 2
    )
    assert (
        sample_value(text, "cronstable_job_duration_seconds_sum", job_name="j")
        == 402.0
    )
    assert (
        sample_value(
            text, "cronstable_job_duration_seconds_count", job_name="j"
        )
        == 2
    )


def test_registry_accumulates_cpu_and_peak_rss():
    from cronstable.resources import ResourceUsage

    metrics = PrometheusMetrics()
    metrics.job_run_recorded(
        "j", "success", 2.0, ResourceUsage(1.0, 0.5, 1000, 3)
    )
    metrics.job_run_recorded(
        "j", "success", 3.0, ResourceUsage(2.0, 1.0, 4000, 5)
    )
    text = _registry_text(metrics)
    # user/system CPU accumulate as a per-mode counter
    assert (
        sample_value(
            text, "cronstable_job_cpu_seconds_total", job_name="j", mode="user"
        )
        == 3.0
    )
    assert (
        sample_value(
            text,
            "cronstable_job_cpu_seconds_total",
            job_name="j",
            mode="system",
        )
        == 1.5
    )
    # peak RSS is a high-water mark across runs, not a sum
    assert (
        sample_value(text, "cronstable_job_peak_rss_bytes", job_name="j")
        == 4000
    )


def test_registry_cpu_absent_for_unmonitored_job():
    metrics = PrometheusMetrics()
    metrics.job_run_recorded("j", "success", 2.0)  # no resources
    text = _registry_text(metrics)
    # an unmonitored job exports no CPU counter / peak-RSS gauge at all
    assert (
        sample_value(
            text, "cronstable_job_cpu_seconds_total", job_name="j", mode="user"
        )
        is None
    )
    assert (
        sample_value(text, "cronstable_job_peak_rss_bytes", job_name="j")
        is None
    )


def test_counter_snapshot_round_trip_seeds_cpu():
    from cronstable.resources import ResourceUsage

    metrics = PrometheusMetrics()
    metrics.job_run_recorded(
        "j", "success", 2.0, ResourceUsage(1.0, 0.5, 8000, 3)
    )
    snap = metrics.counters_snapshot()

    restored = PrometheusMetrics()
    seeded = restored.seed_counters(snap, keep=["j"])
    assert seeded == 1
    text = _registry_text(restored)
    assert (
        sample_value(
            text, "cronstable_job_cpu_seconds_total", job_name="j", mode="user"
        )
        == 1.0
    )
    assert (
        sample_value(text, "cronstable_job_peak_rss_bytes", job_name="j")
        == 8000
    )


def test_seed_counters_skips_corrupt_cpu_sums():
    from cronstable.resources import ResourceUsage

    def snapshot_with(value):
        return {
            "buckets": list(DEFAULT_DURATION_BUCKETS),
            "jobs": {"j": {"cpu_user_sum": value, "cpu_system_sum": value}},
        }

    # a corrupted or hand-edited snapshot (negative, NaN, or bool) must be
    # skipped field-by-field, never fatal, and never poison the counter
    for bad in (-1000, float("nan"), True):
        metrics = PrometheusMetrics()
        metrics.job_run_recorded(
            "j", "success", 2.0, ResourceUsage(1.0, 0.5, 8000, 3)
        )
        metrics.seed_counters(snapshot_with(bad), keep=["j"])
        text = _registry_text(metrics)
        assert (
            sample_value(
                text,
                "cronstable_job_cpu_seconds_total",
                job_name="j",
                mode="user",
            )
            == 1.0
        )
        assert (
            sample_value(
                text,
                "cronstable_job_cpu_seconds_total",
                job_name="j",
                mode="system",
            )
            == 0.5
        )

    # a normal positive value still seeds (added to the live accumulator)
    metrics = PrometheusMetrics()
    metrics.job_run_recorded(
        "j", "success", 2.0, ResourceUsage(1.0, 0.5, 8000, 3)
    )
    metrics.seed_counters(snapshot_with(2.5), keep=["j"])
    text = _registry_text(metrics)
    assert (
        sample_value(
            text, "cronstable_job_cpu_seconds_total", job_name="j", mode="user"
        )
        == 3.5
    )
    assert (
        sample_value(
            text,
            "cronstable_job_cpu_seconds_total",
            job_name="j",
            mode="system",
        )
        == 3.0
    )


def test_registry_prune_drops_removed_jobs():
    metrics = PrometheusMetrics()
    metrics.job_run_recorded("keep", "success", 1.0)
    metrics.job_run_recorded("gone", "success", 1.0)
    metrics.prune(["keep"])
    text = _registry_text(metrics)
    assert 'job_name="keep"' in text
    assert 'job_name="gone"' not in text


def test_registry_bucket_change_resets_histograms_not_counters():
    metrics = PrometheusMetrics()
    metrics.job_run_recorded("j", "success", 2.0)
    metrics.set_duration_buckets((1.0, 10.0))
    text = _registry_text(metrics)
    # the histogram restarted under the new bounds...
    assert (
        sample_value(
            text, "cronstable_job_duration_seconds_count", job_name="j"
        )
        == 0
    )
    assert (
        sample_value(
            text,
            "cronstable_job_duration_seconds_bucket",
            job_name="j",
            le="10.0",
        )
        == 0
    )
    # ...but the outcome counter kept its value
    assert (
        sample_value(
            text, "cronstable_job_runs_total", job_name="j", status="success"
        )
        == 1
    )
    # setting the same buckets again is a no-op (no reset)
    metrics.job_run_recorded("j", "success", 0.5)
    metrics.set_duration_buckets((1.0, 10.0))
    text = _registry_text(metrics)
    assert (
        sample_value(
            text, "cronstable_job_duration_seconds_count", job_name="j"
        )
        == 1
    )


def test_registry_failure_counters():
    metrics = PrometheusMetrics()
    metrics.job_start_failed("j")
    metrics.job_retry_launched("j")
    metrics.job_retry_launched("j")
    metrics.job_permanent_failure("j")
    text = _registry_text(metrics)
    assert (
        sample_value(text, "cronstable_job_start_failures_total", job_name="j")
        == 1
    )
    assert (
        sample_value(text, "cronstable_job_retries_total", job_name="j") == 2
    )
    assert (
        sample_value(
            text, "cronstable_job_permanent_failures_total", job_name="j"
        )
        == 1
    )


def test_registry_config_parse_tracking():
    metrics = PrometheusMetrics()
    # before any parse, neither config family is emitted
    text = _registry_text(metrics)
    assert "cronstable_config_last_reload_successful" not in text
    metrics.config_parse(True)
    text = _registry_text(metrics)
    assert sample_value(text, "cronstable_config_last_reload_successful") == 1
    ok_time = sample_value(
        text, "cronstable_config_last_reload_success_timestamp_seconds"
    )
    assert ok_time is not None
    metrics.config_parse(False)
    text = _registry_text(metrics)
    assert sample_value(text, "cronstable_config_last_reload_successful") == 0
    # the success timestamp still reports the last GOOD parse
    assert (
        sample_value(
            text, "cronstable_config_last_reload_success_timestamp_seconds"
        )
        == ok_time
    )


# ---------------------------------------------------------------------------
# scrape-time gauges from Cron state (direct handler tier)
# ---------------------------------------------------------------------------

_TWO_JOBS = """
jobs:
  - name: alpha
    command: echo hi
    schedule: "*/5 * * * *"
  - name: beta
    command: echo no
    schedule: "@reboot"
    enabled: false
"""


def _closed_output():
    output = JobOutputStream()
    output.close()
    return output


class FakeRequest:
    def __init__(self, headers=None):
        self.headers = headers or {}


@pytest.mark.asyncio
async def test_web_metrics_handler_reports_job_state():
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.web_config = {}
    now = datetime.datetime.now(datetime.timezone.utc)
    cron._record_run(
        "alpha",
        JobRunInfo(
            outcome="failure",
            exit_code=3,
            started_at=now - datetime.timedelta(seconds=7),
            finished_at=now,
            fail_reason="boom",
            output=_closed_output(),
        ),
    )
    resp = await cron._web_metrics(FakeRequest())
    assert resp.headers["Content-Type"] == CONTENT_TYPE_TEXT
    text = resp.body.decode("utf-8")
    assert sample_value(text, "cronstable_jobs", state="enabled") == 1
    assert sample_value(text, "cronstable_jobs", state="disabled") == 1
    assert sample_value(text, "cronstable_job_enabled", job_name="alpha") == 1
    assert sample_value(text, "cronstable_job_enabled", job_name="beta") == 0
    assert sample_value(text, "cronstable_job_running", job_name="alpha") == 0
    # a cron schedule gets a next-run timestamp; @reboot/disabled does not
    assert (
        sample_value(
            text, "cronstable_job_next_run_timestamp_seconds", job_name="alpha"
        )
        is not None
    )
    assert (
        sample_value(
            text, "cronstable_job_next_run_timestamp_seconds", job_name="beta"
        )
        is None
    )
    # the recorded run feeds both the counters and the last-run gauges
    assert (
        sample_value(
            text,
            "cronstable_job_runs_total",
            job_name="alpha",
            status="failure",
        )
        == 1
    )
    assert (
        sample_value(text, "cronstable_job_last_run_success", job_name="alpha")
        == 0
    )
    assert (
        sample_value(
            text, "cronstable_job_last_run_exit_code", job_name="alpha"
        )
        == 3
    )
    assert (
        sample_value(
            text, "cronstable_job_last_run_duration_seconds", job_name="alpha"
        )
        == 7
    )
    assert (
        sample_value(
            text,
            "cronstable_job_info",
            job_name="alpha",
            schedule="*/5 * * * *",
            cluster_policy="Leader",
        )
        == 1
    )
    # build info and job-set fingerprint are present
    assert "cronstable_info{version=" in text
    assert 'cronstable_job_set_info{job_set_id="v1:' in text
    # no cluster configured
    assert sample_value(text, "cronstable_cluster_enabled") == 0


_NEXT_RUN_GATE = """
jobs:
  - name: cron-on
    command: echo hi
    schedule: "*/5 * * * *"
  - name: cron-off
    command: echo hi
    schedule: "*/5 * * * *"
    enabled: false
  - name: boot-on
    command: echo hi
    schedule: "@reboot"
"""


@pytest.mark.asyncio
async def test_next_run_gate_checks_enabled_and_schedule_independently():
    # both halves of the suppression gate, pinned separately: a DISABLED
    # job with a perfectly good cron schedule gets no next-run sample, and
    # neither does an ENABLED @reboot job (a fixture job that is both at
    # once would leave the enabled check unpinned).
    cron = Cron(None, config_yaml=_NEXT_RUN_GATE)
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text,
            "cronstable_job_next_run_timestamp_seconds",
            job_name="cron-on",
        )
        is not None
    )
    assert (
        sample_value(
            text,
            "cronstable_job_next_run_timestamp_seconds",
            job_name="cron-off",
        )
        is None
    )
    assert (
        sample_value(
            text,
            "cronstable_job_next_run_timestamp_seconds",
            job_name="boot-on",
        )
        is None
    )


@pytest.mark.asyncio
async def test_next_run_reads_seeded_next_fire_index():
    # Steady-state path: once the loop has seeded the next-fire index, a scrape
    # must read the job's next fire straight from cron._next_fire instead of
    # re-walking the crontab (the fallback the gate test above exercises on a
    # loop that never ran). Seed a DISTINCTIVE instant that "*/5 * * * *" could
    # never itself produce (not on a 5-minute boundary, and years out), so a
    # regression that recomputed via the fallback would render a different
    # value and fail here rather than silently matching.
    cron = Cron(None, config_yaml=_NEXT_RUN_GATE)
    when = datetime.datetime(2099, 1, 1, 0, 2, 3, tzinfo=datetime.timezone.utc)
    cron._next_fire["cron-on"] = when
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text,
            "cronstable_job_next_run_timestamp_seconds",
            job_name="cron-on",
        )
        == when.timestamp()
    )
    # a disabled job never enters the index, so it still gets no sample even
    # when an enabled sibling is served from it.
    assert (
        sample_value(
            text,
            "cronstable_job_next_run_timestamp_seconds",
            job_name="cron-off",
        )
        is None
    )


@pytest.mark.asyncio
async def test_web_metrics_handler_openmetrics_negotiation():
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.web_config = {}
    resp = await cron._web_metrics(
        FakeRequest({"Accept": "application/openmetrics-text; version=1.0.0"})
    )
    assert resp.headers["Content-Type"] == CONTENT_TYPE_OPENMETRICS
    text = resp.body.decode("utf-8")
    assert "# TYPE cronstable_job_runs counter" in text
    assert text.endswith("# EOF\n")


@pytest.mark.asyncio
async def test_web_metrics_handler_merges_operator_headers():
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.web_config = {
        "headers": {"X-Custom": "yes", "Content-Type": "text/bogus"}
    }
    resp = await cron._web_metrics(FakeRequest())
    assert resp.headers["X-Custom"] == "yes"
    # the exposition content type is the endpoint's contract: it wins over
    # an operator-configured Content-Type (unlike the other handlers).
    assert resp.headers["Content-Type"] == CONTENT_TYPE_TEXT


@pytest.mark.asyncio
async def test_web_metrics_content_type_override_is_case_insensitive():
    # header names are case-insensitive on the wire: a case-variant
    # operator spelling must be replaced, not emitted as a second,
    # conflicting Content-Type header (scrapers read the first one).
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.web_config = {"headers": {"content-type": "text/bogus"}}
    resp = await cron._web_metrics(FakeRequest())
    assert resp.headers.getall("Content-Type") == [CONTENT_TYPE_TEXT]


# ---------------------------------------------------------------------------
# job lifecycle end-to-end: real subprocesses feeding the counters
# ---------------------------------------------------------------------------


async def _run_to_completion(cron, name):
    running_job = cron.running_jobs[name][-1]
    await running_job.wait()
    await cron._handle_finished_job(running_job)
    # the report+retry-arm sequence (which emits the failure counters) runs
    # as a spawned per-job task; wait for it like the reaper's old inline
    # await did.
    await cron._drain_completions()
    return running_job


@pytest.mark.asyncio
async def test_metrics_after_successful_and_failed_runs():
    config = (
        "jobs:\n"
        "  - name: ok\n" + yaml_command(cmd_print(out="hi")) + "\n"
        '    schedule: "* * * * *"\n'
        "  - name: bad\n" + yaml_command(cmd_print(out="no", code=3)) + "\n"
        '    schedule: "* * * * *"\n'
    )
    cron = Cron(None, config_yaml=config)
    await cron.maybe_launch_job(cron.cron_jobs["ok"])
    await _run_to_completion(cron, "ok")
    await cron.maybe_launch_job(cron.cron_jobs["bad"])
    await _run_to_completion(cron, "bad")
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text, "cronstable_job_runs_total", job_name="ok", status="success"
        )
        == 1
    )
    assert (
        sample_value(
            text, "cronstable_job_runs_total", job_name="bad", status="failure"
        )
        == 1
    )
    assert (
        sample_value(text, "cronstable_job_last_run_success", job_name="ok")
        == 1
    )
    assert (
        sample_value(text, "cronstable_job_last_run_exit_code", job_name="bad")
        == 3
    )
    assert (
        sample_value(
            text, "cronstable_job_duration_seconds_count", job_name="ok"
        )
        == 1
    )
    # no retries were configured, so the failure is immediately permanent
    assert (
        sample_value(
            text, "cronstable_job_permanent_failures_total", job_name="bad"
        )
        == 1
    )
    assert (
        sample_value(
            text, "cronstable_job_permanent_failures_total", job_name="ok"
        )
        == 0
    )


@pytest.mark.asyncio
async def test_metrics_count_retries_and_permanent_failure():
    config = (
        "jobs:\n"
        "  - name: flaky\n" + yaml_command(cmd_print(out="x", code=1)) + "\n"
        '    schedule: "* * * * *"\n'
        "    onFailure:\n"
        "      retry:\n"
        "        maximumRetries: 1\n"
        "        initialDelay: 0.01\n"
        "        maximumDelay: 0.01\n"
        "        backoffMultiplier: 1\n"
    )
    cron = Cron(None, config_yaml=config)
    await cron.launch_scheduled_job(cron.cron_jobs["flaky"])
    await _run_to_completion(cron, "flaky")
    # the failure armed a retry task; let it fire and launch the retry
    state = cron.retry_state["flaky"]
    assert state.task is not None
    await state.task
    await _run_to_completion(cron, "flaky")
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text,
            "cronstable_job_runs_total",
            job_name="flaky",
            status="failure",
        )
        == 2
    )
    assert (
        sample_value(text, "cronstable_job_retries_total", job_name="flaky")
        == 1
    )
    # retries exhausted -> exactly one permanent failure
    assert (
        sample_value(
            text, "cronstable_job_permanent_failures_total", job_name="flaky"
        )
        == 1
    )


@pytest.mark.asyncio
async def test_metrics_count_start_failures():
    config = (
        "jobs:\n"
        "  - name: ghost\n"
        "    command:\n"
        '      - "definitely-not-a-real-command-xyz"\n'
        '    schedule: "* * * * *"\n'
    )
    cron = Cron(None, config_yaml=config)
    await cron.maybe_launch_job(cron.cron_jobs["ghost"])
    await _run_to_completion(cron, "ghost")
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text, "cronstable_job_start_failures_total", job_name="ghost"
        )
        == 1
    )
    assert (
        sample_value(
            text,
            "cronstable_job_runs_total",
            job_name="ghost",
            status="failure",
        )
        == 1
    )
    assert (
        sample_value(
            text, "cronstable_job_last_run_exit_code", job_name="ghost"
        )
        == 127
    )


@pytest.mark.asyncio
async def test_metrics_count_cancelled_runs():
    config = (
        "jobs:\n"
        "  - name: slow\n" + yaml_command(cmd_sleep(60)) + "\n"
        '    schedule: "* * * * *"\n'
        "    killTimeout: 5\n"
    )
    cron = Cron(None, config_yaml=config)
    await cron.maybe_launch_job(cron.cron_jobs["slow"])
    running_job = cron.running_jobs["slow"][0]
    running_job.cancelled = True  # what _web_cancel_job sets
    await running_job.cancel()
    await running_job.wait()
    await cron._handle_finished_job(running_job)
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text,
            "cronstable_job_runs_total",
            job_name="slow",
            status="cancelled",
        )
        == 1
    )
    assert (
        sample_value(text, "cronstable_job_last_run_success", job_name="slow")
        == 0
    )


@pytest.mark.asyncio
async def test_retry_swallowed_by_forbid_is_not_counted():
    config = (
        "jobs:\n"
        "  - name: busy\n" + yaml_command(cmd_sleep(60)) + "\n"
        '    schedule: "* * * * *"\n'
        "    concurrencyPolicy: Forbid\n"
        "    killTimeout: 5\n"
        "    onFailure:\n"
        "      retry:\n"
        "        maximumRetries: 2\n"
        "        initialDelay: 0.01\n"
        "        maximumDelay: 0.01\n"
        "        backoffMultiplier: 1\n"
    )
    cron = Cron(None, config_yaml=config)
    # an instance is still running when the retry fires: Forbid swallows
    # the launch, so the retries counter must not move (it reports retries
    # actually launched).
    await cron.maybe_launch_job(cron.cron_jobs["busy"])
    running_job = cron.running_jobs["busy"][0]
    try:
        await cron.schedule_retry_job("busy", 0, 1)
        text = cron.metrics.render(cron)
        assert (
            sample_value(text, "cronstable_job_retries_total", job_name="busy")
            == 0
        )
    finally:
        running_job.cancelled = True
        await running_job.cancel()
        await running_job.wait()
        await cron._handle_finished_job(running_job)


# ---------------------------------------------------------------------------
# cluster metrics
# ---------------------------------------------------------------------------


class FakeManager:
    distribution = "single-leader"

    def __init__(self, is_leader=True, quorate=True):
        self._is_leader = is_leader
        self._quorate = quorate

    def view_dict(self):
        return {
            "backend": "gossip",
            "node_name": "n1",
            "job_set_id": "v1:abc",
            "cluster_size": 3,
            "quorum": 2,
            "elect_leader": True,
            "distribution": self.distribution,
            "conflict": False,
            "conflict_names": [],
            "size_conflict": False,
            "conflicting_sizes": [],
            "policy_conflict": True,
            "conflicting_policies": ["n2: distribution=spread"],
            "quorate": self._quorate,
            "leader": "n1" if self._is_leader else None,
            "is_leader": self._is_leader,
            "peers": [
                {"host": "n2:8443", "status": "agreed"},
                {"host": "n3:8443", "status": "agreed"},
                {"host": "n4:8443", "status": "unreachable"},
            ],
        }

    def is_leader(self):
        return self._is_leader

    def is_quorate(self):
        return self._quorate

    def conflict_names(self):
        return []

    def conflicting_sizes(self):
        return []

    def conflicting_policies(self):
        return []

    def cluster_size(self):
        return 3


@pytest.mark.asyncio
async def test_cluster_metrics_from_manager_view():
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.cluster_manager = FakeManager()
    text = cron.metrics.render(cron)
    assert sample_value(text, "cronstable_cluster_enabled") == 1
    assert (
        sample_value(
            text,
            "cronstable_cluster_info",
            backend="gossip",
            node_name="n1",
            distribution="single-leader",
        )
        == 1
    )
    assert sample_value(text, "cronstable_cluster_size") == 3
    assert sample_value(text, "cronstable_cluster_quorum") == 2
    assert sample_value(text, "cronstable_cluster_quorate") == 1
    assert sample_value(text, "cronstable_cluster_is_leader") == 1
    assert (
        sample_value(text, "cronstable_cluster_leader_info", leader="n1") == 1
    )
    assert (
        sample_value(text, "cronstable_cluster_conflict", kind="nodename") == 0
    )
    assert (
        sample_value(text, "cronstable_cluster_conflict", kind="policy") == 1
    )
    assert sample_value(text, "cronstable_cluster_peers", status="agreed") == 2
    assert (
        sample_value(text, "cronstable_cluster_peers", status="unreachable")
        == 1
    )
    # zero-filled for statuses with no peer, so alert series never vanish
    assert (
        sample_value(text, "cronstable_cluster_peers", status="drifted") == 0
    )
    # observe-only cluster (electLeader off): the transition counters are
    # omitted rather than exposed permanently frozen at zero while the
    # quorate gauge visibly changes
    assert "cronstable_cluster_leader_transitions_total" not in text
    assert "cronstable_cluster_quorum_transitions_total" not in text


@pytest.mark.asyncio
async def test_cluster_metrics_survive_backend_error():
    class BrokenManager:
        def view_dict(self):
            raise RuntimeError("backend bug")

    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.cluster_manager = BrokenManager()
    text = cron.metrics.render(cron)
    # job metrics still render; the cluster block degrades to the enabled
    # gauge instead of failing the whole scrape
    assert sample_value(text, "cronstable_cluster_enabled") == 1
    assert "cronstable_cluster_info" not in text
    assert 'cronstable_job_enabled{job_name="alpha"}' in text


@pytest.mark.asyncio
async def test_cluster_transition_counters():
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron._elect_leader_configured = True
    cron.cluster_manager = FakeManager(is_leader=False, quorate=False)
    cron._log_cluster_role()  # no transition yet: both latches were False
    cron.cluster_manager = FakeManager(is_leader=True, quorate=True)
    cron._log_cluster_role()  # both flip on
    cron.cluster_manager = FakeManager(is_leader=False, quorate=True)
    cron._log_cluster_role()  # leadership flips off, quorum unchanged
    text = cron.metrics.render(cron)
    assert (
        sample_value(text, "cronstable_cluster_leader_transitions_total") == 2
    )
    assert (
        sample_value(text, "cronstable_cluster_quorum_transitions_total") == 1
    )


# ---------------------------------------------------------------------------
# real HTTP: route registration, auth, and the public exemption
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_web_metrics_served_by_default():
    import aiohttp

    cron = Cron(None, config_yaml=_TWO_JOBS)
    await cron.start_stop_web_app({"listen": ["http://127.0.0.1:0"]})
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/metrics") as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == CONTENT_TYPE_TEXT
                text = await resp.text()
                assert "cronstable_info{version=" in text
            # a Prometheus scraper advertising OpenMetrics gets it
            async with session.get(
                base + "/metrics",
                headers={
                    "Accept": (
                        "application/openmetrics-text;"
                        "version=1.0.0,text/plain;q=0.5"
                    )
                },
            ) as resp:
                assert resp.status == 200
                assert resp.headers["Content-Type"] == CONTENT_TYPE_OPENMETRICS
                assert (await resp.text()).endswith("# EOF\n")
    finally:
        await cron.start_stop_web_app(None)


@pytest.mark.asyncio
async def test_web_metrics_disabled():
    import aiohttp

    cron = Cron(None, config_yaml=_TWO_JOBS)
    await cron.start_stop_web_app(
        {"listen": ["http://127.0.0.1:0"], "metrics": False}
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/metrics") as resp:
                assert resp.status == 404
            async with session.get(base + "/jobs") as resp:
                assert resp.status == 200  # the rest of the API is untouched
    finally:
        await cron.start_stop_web_app(None)


@pytest.mark.asyncio
async def test_web_metrics_requires_token_by_default():
    import aiohttp

    cron = Cron(None, config_yaml=_TWO_JOBS)
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": {"value": "secret"},
            "ui": False,
        }
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/metrics") as resp:
                assert resp.status == 401
            # scrape_configs `authorization: credentials: secret` works
            async with session.get(
                base + "/metrics",
                headers={"Authorization": "Bearer secret"},
            ) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)


@pytest.mark.asyncio
async def test_web_metrics_requires_token_with_ui_enabled():
    import aiohttp

    # the production default is ui: true, where the auth-exempt set is
    # composed from WEB_PUBLIC_PATHS -- pin that /metrics is NOT part of
    # it (a widened WEB_PUBLIC_PATHS would otherwise silently serve
    # metrics unauthenticated).
    cron = Cron(None, config_yaml=_TWO_JOBS)
    await cron.start_stop_web_app(
        {"listen": ["http://127.0.0.1:0"], "authToken": {"value": "secret"}}
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/") as resp:
                assert resp.status == 200  # the UI page stays public
            async with session.get(base + "/metrics") as resp:
                assert resp.status == 401
            async with session.get(
                base + "/metrics",
                headers={"Authorization": "Bearer secret"},
            ) as resp:
                assert resp.status == 200
    finally:
        await cron.start_stop_web_app(None)


@pytest.mark.asyncio
async def test_web_metrics_public_exemption():
    import aiohttp

    cron = Cron(None, config_yaml=_TWO_JOBS)
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "authToken": {"value": "secret"},
            "ui": False,
            "metrics": {"public": True},
        }
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            # /metrics was deliberately opened up...
            async with session.get(base + "/metrics") as resp:
                assert resp.status == 200
            # ...but the data endpoints stay behind the token
            async with session.get(base + "/jobs") as resp:
                assert resp.status == 401
    finally:
        await cron.start_stop_web_app(None)


@pytest.mark.asyncio
async def test_web_metrics_custom_buckets_applied():
    import aiohttp

    cron = Cron(None, config_yaml=_TWO_JOBS)
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "metrics": {"durationBuckets": [1.0, 30.0]},
        }
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/metrics") as resp:
                text = await resp.text()
        assert 'le="30.0"' in text
        assert 'le="300.0"' not in text
    finally:
        await cron.start_stop_web_app(None)


@pytest.mark.asyncio
async def test_web_metrics_bucket_change_applies_on_web_restart():
    import aiohttp

    # an operator edits web.metrics.durationBuckets and reloads: the web
    # app is restarted for the config change, the new bounds must be
    # applied (histograms restart), and the outcome counters must survive.
    cron = Cron(None, config_yaml=_TWO_JOBS)
    await cron.start_stop_web_app({"listen": ["http://127.0.0.1:0"]})
    cron.metrics.job_run_recorded("alpha", "success", 2.0)
    await cron.start_stop_web_app(
        {
            "listen": ["http://127.0.0.1:0"],
            "metrics": {"durationBuckets": [1.0, 30.0]},
        }
    )
    try:
        port = cron.web_runner.addresses[0][1]
        base = "http://127.0.0.1:{}".format(port)
        async with aiohttp.ClientSession() as session:
            async with session.get(base + "/metrics") as resp:
                text = await resp.text()
        assert 'le="30.0"' in text
        assert 'le="300.0"' not in text
        assert (
            sample_value(
                text, "cronstable_job_duration_seconds_count", job_name="alpha"
            )
            == 0
        )
        assert (
            sample_value(
                text,
                "cronstable_job_runs_total",
                job_name="alpha",
                status="success",
            )
            == 1
        )
    finally:
        await cron.start_stop_web_app(None)


# ---------------------------------------------------------------------------
# config reload integration
# ---------------------------------------------------------------------------

_GOOD_FILE = """
jobs:
  - name: reloaded
    command: echo hi
    schedule: "* * * * *"
"""


def test_update_config_records_reload_outcome(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_GOOD_FILE, encoding="utf-8")
    cron = Cron(str(cfg))
    text = cron.metrics.render(cron)
    assert sample_value(text, "cronstable_config_last_reload_successful") == 1
    # break the file: the reload fails and the gauge flips to 0
    cfg.write_text("jobs: [", encoding="utf-8")
    with pytest.raises(cronstable.cron.ConfigError):
        cron.update_config()
    text = cron.metrics.render(cron)
    assert sample_value(text, "cronstable_config_last_reload_successful") == 0


def test_update_config_prunes_removed_jobs(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text(_GOOD_FILE, encoding="utf-8")
    cron = Cron(str(cfg))
    cron.metrics.job_run_recorded("reloaded", "success", 1.0)
    # replace the job set: the old job's accumulator series disappears
    cfg.write_text(
        _GOOD_FILE.replace("name: reloaded", "name: other"),
        encoding="utf-8",
    )
    cron.update_config()
    text = cron.metrics.render(cron)
    assert 'job_name="other"' in text
    assert 'job_name="reloaded"' not in text


@pytest.mark.asyncio
async def test_prune_spares_still_running_removed_job(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        _GOOD_FILE + "  - name: doomed\n" + yaml_command(cmd_sleep(60)) + "\n"
        '    schedule: "* * * * *"\n'
        "    killTimeout: 5\n",
        encoding="utf-8",
    )
    cron = Cron(str(cfg))
    cron.metrics.job_run_recorded("doomed", "success", 1.0)
    await cron.maybe_launch_job(cron.cron_jobs["doomed"])
    running_job = cron.running_jobs["doomed"][0]
    # remove the job while its instance is still running: the reload must
    # NOT prune its accumulator, or the finishing run would recreate the
    # series from zero (a phantom counter reset for Prometheus).
    cfg.write_text(_GOOD_FILE, encoding="utf-8")
    cron.update_config()
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text,
            "cronstable_job_runs_total",
            job_name="doomed",
            status="success",
        )
        == 1
    )
    # the run finishes onto the surviving accumulator, not a fresh one
    running_job.cancelled = True
    await running_job.cancel()
    await running_job.wait()
    await cron._handle_finished_job(running_job)
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text,
            "cronstable_job_runs_total",
            job_name="doomed",
            status="success",
        )
        == 1
    )
    assert (
        sample_value(
            text,
            "cronstable_job_runs_total",
            job_name="doomed",
            status="cancelled",
        )
        == 1
    )
    # the next reload -- nothing running any more -- prunes it for good
    cron.update_config()
    text = cron.metrics.render(cron)
    assert 'job_name="doomed"' not in text


# ---------------------------------------------------------------------------
# seed_counters: non-numeric bucket bounds
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_bucket", ["not-a-float", None])
def test_seed_counters_non_numeric_buckets_skip_histogram(bad_bucket):
    # A hand-edited / corrupt snapshot whose "buckets" cannot be coerced to
    # floats (ValueError for a string, TypeError for None) must not be fatal:
    # buckets_match falls back to False, so the histogram is NOT seeded, but
    # the outcome counters still seed (they are bucket-independent).
    snapshot = {
        "buckets": [bad_bucket],
        "jobs": {
            "j": {
                "runs": {"success": 2},
                "duration_sum": 4.0,
                "duration_count": 2,
                "bucket_counts": [1] * len(DEFAULT_DURATION_BUCKETS),
            }
        },
    }
    metrics = PrometheusMetrics()
    seeded = metrics.seed_counters(snapshot, keep=["j"])
    assert seeded == 1
    text = _registry_text(metrics)
    # outcome counter seeded despite the unusable buckets
    assert (
        sample_value(
            text, "cronstable_job_runs_total", job_name="j", status="success"
        )
        == 2
    )
    # histogram left at zero: the corrupt buckets blocked _seed_histogram
    assert (
        sample_value(
            text, "cronstable_job_duration_seconds_count", job_name="j"
        )
        == 0
    )


def test_seed_counters_matching_buckets_seeds_histogram_baseline():
    # Control for the test above: with valid, matching buckets the very same
    # payload DOES seed the histogram, proving the zero above is caused by the
    # bad bounds and not by some other omission.
    snapshot = {
        "buckets": list(DEFAULT_DURATION_BUCKETS),
        "jobs": {
            "j": {
                "runs": {"success": 2},
                "duration_sum": 4.0,
                "duration_count": 2,
                "bucket_counts": [1] * len(DEFAULT_DURATION_BUCKETS),
            }
        },
    }
    metrics = PrometheusMetrics()
    metrics.seed_counters(snapshot, keep=["j"])
    text = _registry_text(metrics)
    assert (
        sample_value(
            text, "cronstable_job_duration_seconds_count", job_name="j"
        )
        == 2
    )


# ---------------------------------------------------------------------------
# _state_families: lock-acquisition and throttle families
# ---------------------------------------------------------------------------


class FakeStateBackend:
    """Minimal stand-in exposing the surface _state_families reads."""

    def __init__(self, stats):
        self._stats = stats

    def view_dict(self):
        return {"backend": "sqlite", "topology": "single"}

    def stats(self):
        return self._stats


@pytest.mark.asyncio
async def test_state_families_emit_lock_and_throttle_counters():
    stats = {
        "ops": {"put": {"count": 5, "errors": 1, "seconds": 0.25}},
        "lock": {"acquisitions": 4, "wait_seconds": 1.5},
        "throttle": {"count": 3, "wait_seconds": 0.75},
    }
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.state_backend = FakeStateBackend(stats)
    text = cron.metrics.render(cron)

    # lock family (acquisitions > 0 gate is open)
    assert (
        sample_value(text, "cronstable_state_lock_acquisitions_total") == 4
    )
    assert (
        sample_value(text, "cronstable_state_lock_wait_seconds_total") == 1.5
    )
    # throttle family (count > 0 gate is open)
    assert sample_value(text, "cronstable_state_throttled_ops_total") == 3
    assert (
        sample_value(text, "cronstable_state_throttle_wait_seconds_total")
        == 0.75
    )
    # the info + op families rendered too
    assert 'cronstable_state_info{backend="sqlite"' in text
    assert (
        sample_value(text, "cronstable_state_ops_total", op="put") == 5
    )


@pytest.mark.asyncio
async def test_state_families_omit_lock_and_throttle_when_idle():
    # zero acquisitions / zero throttled ops keep those families off the
    # scrape (no frozen zeros), the negative side of the gates above.
    stats = {
        "lock": {"acquisitions": 0, "wait_seconds": 0.0},
        "throttle": {"count": 0, "wait_seconds": 0.0},
    }
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.state_backend = FakeStateBackend(stats)
    text = cron.metrics.render(cron)
    assert "cronstable_state_lock_acquisitions_total" not in text
    assert "cronstable_state_throttled_ops_total" not in text


# ---------------------------------------------------------------------------
# _job_families: last-run CPU / RSS gauges
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_job_families_emit_last_run_cpu_and_rss():
    from cronstable.resources import ResourceUsage

    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.web_config = {}
    now = datetime.datetime.now(datetime.timezone.utc)
    cron._record_run(
        "alpha",
        JobRunInfo(
            outcome="success",
            exit_code=0,
            started_at=now - datetime.timedelta(seconds=3),
            finished_at=now,
            fail_reason=None,
            output=_closed_output(),
            resource_usage=ResourceUsage(1.25, 0.75, 9000, 4),
        ),
    )
    text = cron.metrics.render(cron)
    # cpu_total_seconds = user + system = 2.0
    assert (
        sample_value(
            text, "cronstable_job_last_run_cpu_seconds", job_name="alpha"
        )
        == 2.0
    )
    assert (
        sample_value(
            text, "cronstable_job_last_run_max_rss_bytes", job_name="alpha"
        )
        == 9000
    )


@pytest.mark.asyncio
async def test_job_families_omit_last_run_cpu_when_unmonitored():
    # the negative side: a run without resource_usage exports neither gauge.
    cron = Cron(None, config_yaml=_TWO_JOBS)
    cron.web_config = {}
    now = datetime.datetime.now(datetime.timezone.utc)
    cron._record_run(
        "alpha",
        JobRunInfo(
            outcome="success",
            exit_code=0,
            started_at=now - datetime.timedelta(seconds=3),
            finished_at=now,
            fail_reason=None,
            output=_closed_output(),
        ),
    )
    text = cron.metrics.render(cron)
    assert (
        sample_value(
            text, "cronstable_job_last_run_cpu_seconds", job_name="alpha"
        )
        is None
    )
    assert (
        sample_value(
            text, "cronstable_job_last_run_max_rss_bytes", job_name="alpha"
        )
        is None
    )
