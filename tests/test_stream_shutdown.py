"""Event streams notice departed clients and server shutdown promptly."""

import asyncio
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from logplane import bus
from logplane.local_config import initialize_local_config

ROOT = Path(__file__).resolve().parents[1]


class FakeRequest:
    def __init__(self):
        self.disconnected = False

    async def is_disconnected(self):
        return self.disconnected


class StreamLoopTests(unittest.TestCase):
    """Drive the SSE generator directly with a stubbed history window."""

    def run_stream(self, scenario, *, poll=0.01, keepalive=0.05, fetch=None):
        calls = []

        def default_fetch(last_id, topics, limit, correlation_id):
            calls.append(last_id)
            return [], last_id, False

        async def main():
            request = FakeRequest()
            with (
                patch.object(bus, "new_event", asyncio.Condition()),
                patch.object(bus, "STREAM_POLL_SECONDS", poll),
                patch.object(bus, "STREAM_KEEPALIVE_SECONDS", keepalive),
                patch.object(bus, "fetch_stream_window", fetch or default_fetch),
                patch.object(bus, "_shutting_down", False),
            ):
                response = await bus.stream(request, from_id=0, topics=None, correlation_id=None)
                return await scenario(response.body_iterator, request)

        return asyncio.run(main()), calls

    def test_keepalive_cadence_and_history_checks_are_unchanged(self):
        async def scenario(stream, request):
            first = await asyncio.wait_for(stream.__anext__(), 1)
            second = await asyncio.wait_for(stream.__anext__(), 1)
            return first, second

        (first, second), calls = self.run_stream(scenario)
        self.assertEqual(": keepalive\n\n", first)
        self.assertEqual(": keepalive\n\n", second)
        # One read on start and one after each keepalive interval (five poll
        # slices here), never one read per slice.
        self.assertEqual(2, len(calls))

    def test_a_quiet_subscriber_gets_keepalives_while_other_topics_stay_busy(self):
        # Every worker heartbeat is appended to the log. A stream filtered to
        # topics that stay quiet must still be sent its keepalives on time;
        # resetting the wait on each append left such a stream silent forever.
        async def scenario(stream, request):
            async def other_topics():
                while True:
                    await asyncio.sleep(0.005)
                    async with bus.new_event:
                        bus.event_generation += 1
                        bus.new_event.notify_all()

            noise = asyncio.ensure_future(other_topics())
            try:
                started = time.monotonic()
                first = await asyncio.wait_for(stream.__anext__(), 2)
                second = await asyncio.wait_for(stream.__anext__(), 2)
                return first, second, time.monotonic() - started
            finally:
                noise.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await noise

        with patch.object(bus, "event_generation", 0):
            (first, second, elapsed), _ = self.run_stream(scenario)
        self.assertEqual(": keepalive\n\n", first)
        self.assertEqual(": keepalive\n\n", second)
        self.assertGreaterEqual(elapsed, 0.1)  # two 0.05s intervals, not a flood

    def test_an_event_sent_to_the_stream_restarts_its_keepalive_interval(self):
        delivered = {"id": 7, "topic": "task.created"}

        def fetch(last_id, topics, limit, correlation_id):
            if bus.event_generation:
                return ([delivered] if last_id < 7 else []), 7, False
            return [], last_id, False

        async def scenario(stream, request):
            waiter = asyncio.ensure_future(stream.__anext__())
            await asyncio.sleep(0.15)
            async with bus.new_event:
                bus.event_generation += 1
                bus.new_event.notify_all()
            event = await asyncio.wait_for(waiter, 1)
            sent = time.monotonic()
            keepalive = await asyncio.wait_for(stream.__anext__(), 2)
            return event, keepalive, time.monotonic() - sent

        with patch.object(bus, "event_generation", 0):
            (event, keepalive, gap), _ = self.run_stream(
                scenario, poll=0.01, keepalive=0.3, fetch=fetch
            )
        self.assertIn('"id": 7', event)
        self.assertEqual(": keepalive\n\n", keepalive)
        self.assertGreaterEqual(gap, 0.25)

    def test_departed_client_is_noticed_within_a_poll_slice(self):
        async def scenario(stream, request):
            waiter = asyncio.ensure_future(stream.__anext__())
            await asyncio.sleep(0.05)
            request.disconnected = True
            started = time.monotonic()
            with self.assertRaises(StopAsyncIteration):
                await asyncio.wait_for(waiter, 1)
            return time.monotonic() - started

        elapsed, _ = self.run_stream(scenario, keepalive=100)
        self.assertLess(elapsed, 0.5)

    def test_shutdown_ends_open_streams_within_a_poll_slice(self):
        async def scenario(stream, request):
            waiter = asyncio.ensure_future(stream.__anext__())
            await asyncio.sleep(0.05)
            bus.begin_shutdown()
            with self.assertRaises(StopAsyncIteration):
                await asyncio.wait_for(waiter, 1)
            return bus._shutting_down

        stopped, _ = self.run_stream(scenario, keepalive=100)
        self.assertTrue(stopped)
        self.assertFalse(bus._shutting_down)  # the patch restored module state

    def test_a_new_event_is_delivered_without_waiting_for_a_slice(self):
        delivered = {"id": 7, "topic": "task.created"}

        def fetch(last_id, topics, limit, correlation_id):
            if bus.event_generation:
                return ([delivered] if last_id < 7 else []), 7, False
            return [], last_id, False

        async def scenario(stream, request):
            waiter = asyncio.ensure_future(stream.__anext__())
            await asyncio.sleep(0.02)
            async with bus.new_event:
                bus.event_generation += 1
                bus.new_event.notify_all()
            return await asyncio.wait_for(waiter, 1)

        with patch.object(bus, "event_generation", 0):
            chunk, _ = self.run_stream(scenario, poll=10, keepalive=100, fetch=fetch)
        self.assertIn('"id": 7', chunk)


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def serve_environment():
    environment = {k: v for k, v in os.environ.items() if not k.startswith("LOGPLANE_")}
    environment["PYTHONPATH"] = str(ROOT / "src")
    return environment


def config_on_port(directory, port):
    config = initialize_local_config(directory)
    value = json.loads(config.read_text())
    value["bus"]["port"] = port
    config.write_text(json.dumps(value))
    return config


class ServeStartupFailureTests(unittest.TestCase):
    def test_port_in_use_exits_with_the_documented_usage_status(self):
        with tempfile.TemporaryDirectory() as directory, socket.socket() as holder:
            holder.bind(("127.0.0.1", 0))
            holder.listen()
            config = config_on_port(directory, holder.getsockname()[1])
            result = subprocess.run(
                [sys.executable, "-m", "logplane.cli", "serve", "--config", str(config)],
                cwd=directory, env=serve_environment(), capture_output=True, text=True,
                timeout=30,
            )
        self.assertEqual(2, result.returncode, result.stderr)
        self.assertIn("could not start", result.stderr)


@unittest.skipIf(os.name != "posix", "uses POSIX signals")
class ServeShutdownTests(unittest.TestCase):
    def test_ctrl_c_stops_promptly_with_a_connected_stream(self):
        with tempfile.TemporaryDirectory() as directory:
            port = free_port()
            config = config_on_port(directory, port)
            environment = serve_environment()
            log = Path(directory, "serve.log")
            with log.open("w") as handle:
                server = subprocess.Popen(
                    [sys.executable, "-m", "logplane.cli", "serve", "--config", str(config)],
                    cwd=directory, env=environment, stdout=handle, stderr=subprocess.STDOUT,
                )
            try:
                url = f"http://127.0.0.1:{port}"
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    try:
                        if httpx.get(f"{url}/health", timeout=1).status_code == 200:
                            break
                    except httpx.HTTPError:
                        time.sleep(0.1)
                else:
                    self.fail(f"bus did not start:\n{log.read_text()}")

                connected = threading.Event()

                def follow():
                    try:
                        with httpx.stream("GET", f"{url}/events/stream", timeout=None) as response:
                            connected.set()
                            for _ in response.iter_lines():
                                pass
                    except httpx.HTTPError:
                        pass

                threading.Thread(target=follow, daemon=True).start()
                self.assertTrue(connected.wait(5))
                time.sleep(0.3)

                started = time.monotonic()
                server.send_signal(signal.SIGINT)
                server.wait(timeout=10)
                elapsed = time.monotonic() - started
            finally:
                if server.poll() is None:
                    server.kill()
                    server.wait()
            output = log.read_text()
            self.assertEqual(130, server.returncode, output)
            self.assertLess(elapsed, 4, output)
            self.assertIn("Finished server process", output)
            self.assertNotIn("Traceback", output)


# Starts the real bus with a short keepalive interval, so a test can watch
# several intervals go by without waiting on the 15-second default.
FAST_KEEPALIVE_BUS = """
import sys
import uvicorn
from logplane import cli

run = uvicorn.Server.run

def run_with_fast_keepalives(self, *args, **kwargs):
    from logplane import bus
    bus.STREAM_KEEPALIVE_SECONDS = 0.25
    bus.STREAM_POLL_SECONDS = 0.05
    return run(self, *args, **kwargs)

uvicorn.Server.run = run_with_fast_keepalives
sys.exit(cli.main(["serve", "--config", sys.argv[1]]))
"""


@unittest.skipIf(os.name != "posix", "uses POSIX signals")
class IdleWorkerStreamTests(unittest.TestCase):
    def test_an_idle_worker_with_a_stream_read_timeout_keeps_one_connection(self):
        # A real bus, a real worker runtime and its real heartbeats. The
        # worker's own heartbeats keep the log busy while its assignment
        # stream stays quiet; with a read timeout on that stream (set whenever
        # a disconnect timeout is configured) it must still hear keepalives,
        # not time out and reconnect over and over.
        from logplane import runtime as runtime_module
        from logplane.client import BusClient
        from logplane.executors import Completed
        from logplane.runtime import WorkerRuntime

        class Executor:
            def execute(self, assignment):
                return Completed("done")

        with tempfile.TemporaryDirectory() as directory:
            port = free_port()
            config = config_on_port(directory, port)
            log = Path(directory, "serve.log")
            with log.open("w") as handle:
                server = subprocess.Popen(
                    [sys.executable, "-c", FAST_KEEPALIVE_BUS, str(config)],
                    cwd=directory, env=serve_environment(), stdout=handle,
                    stderr=subprocess.STDOUT,
                )
            worker = None
            thread = None
            try:
                url = f"http://127.0.0.1:{port}"
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    try:
                        if httpx.get(f"{url}/health", timeout=1).status_code == 200:
                            break
                    except httpx.HTTPError:
                        time.sleep(0.1)
                else:
                    self.fail(f"bus did not start:\n{log.read_text()}")

                opened = []
                stream_once = BusClient._stream_once

                def counting(client, *args, **kwargs):
                    opened.append(kwargs.get("stream_timeout"))
                    return stream_once(client, *args, **kwargs)

                messages = []
                with (
                    patch.object(BusClient, "_stream_once", counting),
                    patch.object(runtime_module, "STREAM_READ_TIMEOUT_SECONDS", 2.0),
                ):
                    worker = WorkerRuntime(
                        BusClient(url, actor="alice", offset_dir=Path(directory)),
                        name="alice", executor=Executor(), heartbeat_seconds=0.05,
                        disconnect_timeout_seconds=30, log=messages.append,
                    )
                    thread = threading.Thread(target=worker.run, daemon=True)
                    thread.start()
                    time.sleep(5.0)  # two and a half read timeouts of idling
                    still_running = thread.is_alive()
                    worker.stop()
                    thread.join(timeout=10)
            finally:
                if worker is not None:
                    worker.stop()
                server.send_signal(signal.SIGINT)
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
            output = log.read_text()
            self.assertTrue(still_running, messages)
            self.assertFalse(thread.is_alive(), messages)
            self.assertEqual([2.0], opened, f"{messages}\n{output}")
            self.assertFalse(worker.stopped_by_disconnect)
            self.assertEqual(0, worker.exit_code)


if __name__ == "__main__":
    unittest.main()
