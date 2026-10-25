"""``logplane up``: run the bus and the coordinator (PM) together.

Both run as child processes, exactly as ``logplane serve`` and ``logplane pm``
would, so nothing about their behaviour changes. The bus log goes to a file;
the PM's reconciliation lines are shown in the terminal and kept in a file.
If either process exits, the other is stopped, so ``up`` never leaves a bus
running without its coordinator.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional, TextIO

import httpx

from logplane.local_config import LocalConfig
from logplane.pm_lock import pm_status

STOP_TIMEOUT_SECONDS = 10.0


def run_up(config_path: Path, *, snapshot: bool, quiet: bool,
           stdout: TextIO, stderr: TextIO) -> int:
    local = LocalConfig.from_file(config_path)
    config_path = local.path
    url = local.bus_url
    if pm_status(url)["status"] == "running":
        print(f"logplane: a coordinator is already running for {url}; stop it, or run "
              "`logplane serve` alone for the bus", file=stderr)
        return 2
    # Ctrl-C and a plain `kill` both stop the pair cleanly, even when started
    # from a context that ignores SIGINT (a background job, some supervisors).
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, _interrupt)
    logs = local.database_path.parent / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ, PYTHONUNBUFFERED="1")
    base = [sys.executable, "-m", "logplane.cli"]

    with (logs / "bus.log").open("a", encoding="utf-8") as bus_log:
        bus = subprocess.Popen(
            [*base, "serve", "--config", str(config_path)], stdout=bus_log,
            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=environment,
            start_new_session=True,
        )
    processes = {"bus": bus}
    try:
        if not _wait_for_bus(url, bus, timeout=20):
            code = bus.poll()
            print(f"logplane: the bus did not start on {url}"
                  + (f" (exit {code})" if code is not None else "")
                  + f"; see {logs / 'bus.log'}", file=stderr)
            return 2
        pm = subprocess.Popen(
            [*base, "pm", "--config", str(config_path), *(["--snapshot"] if snapshot else [])],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            env=environment, start_new_session=True, text=True, bufsize=1,
        )
        processes["pm"] = pm
        echo = threading.Thread(target=_copy_lines, daemon=True,
                                args=(pm.stdout, logs / "pm.log", None if quiet else stdout))
        echo.start()

        print(f"logplane is up: bus {url} · coordinator started", file=stdout)
        print(f"Logs: {logs / 'bus.log'}, {logs / 'pm.log'}", file=stdout)
        print("In another terminal, start a worker and submit work, for example:", file=stdout)
        print("  logplane demo-worker alice", file=stdout)
        print('  logplane submit "Hello"', file=stdout)
        print("Press Ctrl-C to stop both.", file=stdout, flush=True)

        while True:
            for name, process in processes.items():
                code = process.poll()
                if code is not None:
                    print(f"logplane: the {'bus' if name == 'bus' else 'coordinator'} exited "
                          f"(status {code}); stopping. See {logs / (name + '.log')}",
                          file=stderr)
                    return 1
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping the coordinator, then the bus...", file=stdout, flush=True)
        return 0
    finally:
        # Coordinator first, so it never runs without a bus to talk to.
        for name in ("pm", "bus"):
            process = processes.get(name)
            if process is not None:
                _stop(process)


def _interrupt(signum, frame):
    raise KeyboardInterrupt


def _wait_for_bus(url: str, process: subprocess.Popen, *, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            return False
        try:
            if httpx.get(f"{url}/health", timeout=1).status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    return False


def _copy_lines(source, path: Path, echo: Optional[TextIO]) -> None:
    with path.open("a", encoding="utf-8") as log:
        for line in source:
            log.write(line)
            log.flush()
            if echo is not None:
                echo.write(line)
                echo.flush()


def _stop(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    try:
        process.send_signal(signal.SIGINT)
        process.wait(timeout=STOP_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=STOP_TIMEOUT_SECONDS)
