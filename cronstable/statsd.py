import asyncio
import logging
import time

logger = logging.getLogger("statsd")


class StatsdClientProtocol:
    def __init__(self, message, loop):
        self.message = message
        self.loop = loop
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.message.encode())

    def datagram_received(self, data, addr):
        pass

    def error_received(self, exc):
        # the format string needs a placeholder, otherwise logging raises a
        # TypeError and the actual exception detail is lost.
        logger.error("UDP error received: %s", exc)

    def connection_lost(self, exc):
        pass


async def send_to_statsd(host, port, message):
    loop = asyncio.get_running_loop()
    connect = loop.create_datagram_endpoint(
        lambda: StatsdClientProtocol(message, loop), remote_addr=(host, port)
    )
    transport, protocol = await connect
    transport.close()


class StatsdJobMetricWriter:
    def __init__(self, host, port, prefix, job):
        self.host = host
        self.port = port
        self.prefix = prefix
        self.start_time = None
        self.job = job

    async def job_started(self) -> None:
        self.start_time = time.perf_counter()
        await send_to_statsd(
            self.host,
            self.port,
            "{prefix}.start:1|g\n".format(prefix=self.prefix),
        )

    async def job_stopped(self) -> None:
        if self.start_time is None:
            return
        duration_seconds = time.perf_counter() - self.start_time
        duration = int(round(duration_seconds * 1000))
        message = (
            "{prefix}.stop:1|g\n"
            "{prefix}.success:{success}|g\n"
            "{prefix}.duration:{duration}|ms|@0.1\n"
        ).format(
            prefix=self.prefix,
            success=0 if self.job.failed else 1,
            duration=duration,
        )
        # When the run was resource-monitored (see cronstable.resources) ship
        # the CPU time and peak RSS alongside the duration. resource_usage is
        # finalized in RunningJob._on_stop before this hook runs, so it is
        # already populated here; None when monitoring was off or unavailable.
        usage = getattr(self.job, "resource_usage", None)
        if usage is not None:
            message += (
                "{prefix}.cpu:{cpu}|ms|@0.1\n{prefix}.max_rss:{rss}|g\n"
            ).format(
                prefix=self.prefix,
                cpu=int(round(usage.cpu_total_seconds * 1000)),
                rss=usage.max_rss_bytes,
            )
        await send_to_statsd(self.host, self.port, message)
