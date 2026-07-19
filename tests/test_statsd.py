"""Tests for the statsd metric emitters.

Exercise the UDP client protocol's datagram/error callbacks and the
resource-usage arm of the per-job metric writer.
"""

import logging

from cronstable.statsd import StatsdClientProtocol, StatsdJobMetricWriter


# ---------------------------------------------------------------------------
# The datagram callbacks and the resource-usage metric arm.
# ---------------------------------------------------------------------------


def test_client_protocol_datagram_received_is_silent(caplog):
    # The statsd channel is write-only: an inbound datagram (a server reply, a
    # stray packet on the port) must be dropped without raising and without
    # logging, so it cannot turn into per-packet log noise. Asserting only
    # that it returns None would pass for any body that falls off the end,
    # including one that logged first.
    proto = StatsdClientProtocol("m.start:1|g\n", loop=None)
    with caplog.at_level(logging.DEBUG, logger="statsd"):
        proto.datagram_received(b"anything", ("127.0.0.1", 8125))
    assert caplog.records == []


def test_client_protocol_error_received_logs_the_exception_detail(caplog):
    proto = StatsdClientProtocol("m.start:1|g\n", loop=None)
    with caplog.at_level(logging.ERROR, logger="statsd"):
        proto.error_received(OSError("network is unreachable"))
    assert "UDP error received" in caplog.text
    # the placeholder must interpolate the actual exception, not drop it
    assert "network is unreachable" in caplog.text


class _FakeUsage:
    def __init__(self, cpu_total_seconds, max_rss_bytes):
        self.cpu_total_seconds = cpu_total_seconds
        self.max_rss_bytes = max_rss_bytes


class _FakeJob:
    def __init__(self, failed=False, resource_usage=None):
        self.failed = failed
        self.resource_usage = resource_usage


class _SendRecorder:
    """Stands in for the UDP send seam; records the datagrams built."""

    def __init__(self):
        self.calls = []

    async def __call__(self, host, port, message):
        self.calls.append((host, port, message))


async def test_job_stopped_appends_cpu_and_rss_when_resource_monitored(
    monkeypatch,
):
    recorder = _SendRecorder()
    monkeypatch.setattr("cronstable.statsd.send_to_statsd", recorder)
    usage = _FakeUsage(cpu_total_seconds=1.5, max_rss_bytes=1048576)
    writer = StatsdJobMetricWriter(
        "host", 8125, "app.job", _FakeJob(resource_usage=usage)
    )
    await writer.job_started()
    await writer.job_stopped()

    message = recorder.calls[-1][2]
    assert "app.job.stop:1|g" in message
    assert "app.job.success:1|g" in message
    assert "app.job.cpu:1500|ms|@0.1" in message  # 1.5s -> 1500ms
    assert "app.job.max_rss:1048576|g" in message


async def test_job_stopped_omits_usage_and_marks_failure(monkeypatch):
    recorder = _SendRecorder()
    monkeypatch.setattr("cronstable.statsd.send_to_statsd", recorder)
    writer = StatsdJobMetricWriter(
        "host", 8125, "app.job", _FakeJob(failed=True, resource_usage=None)
    )
    await writer.job_started()
    await writer.job_stopped()

    message = recorder.calls[-1][2]
    assert "app.job.success:0|g" in message  # failed -> 0
    assert "cpu" not in message and "max_rss" not in message


async def test_job_stopped_is_a_noop_before_started(monkeypatch):
    recorder = _SendRecorder()
    monkeypatch.setattr("cronstable.statsd.send_to_statsd", recorder)
    writer = StatsdJobMetricWriter("host", 8125, "app.job", _FakeJob())
    await writer.job_stopped()  # start_time is None
    assert recorder.calls == []


async def test_prefix_metacharacters_are_stripped(monkeypatch):
    # a configured prefix carrying statsd wire metacharacters (newline, ':',
    # '|') must not be able to forge or inject extra samples into the datagram
    recorder = _SendRecorder()
    monkeypatch.setattr("cronstable.statsd.send_to_statsd", recorder)
    writer = StatsdJobMetricWriter(
        "host", 8125, "evil\nother.metric:0|g", _FakeJob()
    )
    assert writer.prefix == "evilother.metric0g"  # metachars removed
    await writer.job_started()
    # only the sanitised prefix reaches the wire; no injected extra line
    assert recorder.calls[-1][2] == "evilother.metric0g.start:1|g\n"
