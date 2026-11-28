"""Internal process boundary for CLI probes; not an adapter security sandbox."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time

DEFAULT_CHECK_TIMEOUT = 30.0
MAX_REPORT_BYTES = 64 * 1024
CLEANUP_GRACE_SECONDS = 0.25
GROUP_INSPECTION_SECONDS = 1.0


def _darwin_group_exited(process: subprocess.Popen) -> bool:
    """Prove a denied Darwin group signal has no executing target left.

    XNU killpg1 filters SZOMB members before counting signalable processes, so
    a zombie-only group can return EPERM rather than ESRCH. Never interpret
    EPERM alone (or just the leader's exit) as successful descendant cleanup.
    """
    if sys.platform != 'darwin' or process.poll() is None:
        return False
    try:
        result = subprocess.run(
            ['/bin/ps', '-A', '-o', 'pgid=,stat='], stdin=subprocess.DEVNULL,
            capture_output=True, text=True, timeout=GROUP_INSPECTION_SECONDS,
            env={'LC_ALL': 'C', 'PATH': '/usr/bin:/bin'})
        if result.returncode != 0 or not result.stdout.strip():
            return False
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) != 2 or not fields[0].isdigit():
                return False
            if int(fields[0]) == process.pid and not fields[1].startswith('Z'):
                return False
        return True
    except (OSError, subprocess.TimeoutExpired, UnicodeError):
        return False


def _signal_group(process: subprocess.Popen, sig: int) -> bool:
    """Return false for an absent/exited group; fail closed on denied cleanup."""
    try:
        os.killpg(process.pid, sig)
        return True
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        if _darwin_group_exited(process):
            return False
        raise PermissionError(
            f'adapter cleanup could not signal process group {process.pid}; '
            'no-live-members verification failed; child processes may remain') from exc


def _stop_group(process: subprocess.Popen) -> None:
    # Do not skip group cleanup when its leader has already exited: an adapter
    # may have left a child alive. Only our newly-created process group is used.
    if not _signal_group(process, signal.SIGTERM):
        process.wait(timeout=1)
        return
    try:
        process.wait(timeout=CLEANUP_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        pass
    # A child can ignore TERM even if the leader exits promptly.
    _signal_group(process, signal.SIGKILL)
    process.wait(timeout=1)


def run_adapter_check(*, python_target: str | None = None,
                      integration_config: str | None = None,
                      timeout: float = DEFAULT_CHECK_TIMEOUT):
    """Bound loading, execution, and close; return the ordinary conformance report.

    Library callers with already-created executors still use the synchronous
    conformance.check_executor(). This internal helper loads CLI targets afresh.
    """
    from logplane.conformance import ConformanceCheck, ConformanceReport

    if bool(python_target) == bool(integration_config):
        raise ValueError('choose exactly one Python target or integration config')
    if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('adapter check timeout must be a positive finite number')

    def failed(name, detail):
        return ConformanceReport((ConformanceCheck(name, False, detail),))

    def interrupted(*_):
        raise KeyboardInterrupt

    main_thread = threading.current_thread() is threading.main_thread()
    previous = signal.getsignal(signal.SIGTERM) if main_thread else None
    process = None
    with tempfile.TemporaryDirectory(prefix='logplane-adapter-check-') as tmp:
        report_path = Path(tmp) / 'report.json'
        args = [sys.executable, '-m', 'logplane.adapter_check', '--report', str(report_path)]
        # Resolve/read the adapter configuration only in the timed child.
        args += (['--python-target', python_target] if python_target
                 else ['--config', str(integration_config)])
        try:
            if main_thread:
                signal.signal(signal.SIGTERM, interrupted)
            deadline = time.monotonic() + timeout
            process = subprocess.Popen(
                args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, start_new_session=True)
            try:
                process.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                return failed('probe timeout',
                              f'adapter check exceeded {timeout:g}s during loading, execution, '
                              'or cleanup; check for blocking hooks or use --timeout with '
                              'a larger value for an intentionally slow trusted probe')
            if process.returncode != 0:
                return failed('probe process',
                              f'adapter check process exited with status {process.returncode} '
                              'before a valid result; check adapter startup and native dependencies')
            try:
                # A broken adapter must not turn the result channel into a
                # blocking FIFO/device or a symlink after it exits.
                descriptor = os.open(report_path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
                with os.fdopen(descriptor, 'rb') as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise ValueError('report is not a regular file')
                    raw = stream.read(MAX_REPORT_BYTES + 1)
                if len(raw) > MAX_REPORT_BYTES:
                    raise ValueError('oversized report')
                envelope = json.loads(raw)
                if envelope.get('kind') == 'error':
                    error = envelope['message']
                    if not isinstance(error, str):
                        raise ValueError('invalid error')
                else:
                    error = None
                    value = envelope['report']
                    checks = value['checks']
                    outcome = value['outcome_type']
                    if (envelope.get('kind') != 'report' or not isinstance(checks, list)
                            or not checks or (outcome is not None and not isinstance(outcome, str))):
                        raise ValueError('invalid report')
                    decoded = []
                    for check in checks:
                        if (set(check) != {'name', 'ok', 'detail', 'required'}
                                or not isinstance(check['name'], str)
                                or not isinstance(check['detail'], str)
                                or type(check['ok']) is not bool
                                or type(check['required']) is not bool):
                            raise ValueError('invalid check')
                        decoded.append(ConformanceCheck(**check))
            except (OSError, ValueError, KeyError, TypeError, AttributeError):
                return failed('probe report', 'adapter check returned a missing, oversized, or invalid report')
            if error is not None:
                raise ValueError(error)
            return ConformanceReport(tuple(decoded), outcome)
        finally:
            try:
                if process is not None:
                    _stop_group(process)
            finally:
                if main_thread:
                    signal.signal(signal.SIGTERM, previous)


def _child(args) -> None:
    # All adapter-controlled hooks, including imports and constructors, stay
    # within this process and therefore within the parent's deadline.
    from logplane.conformance import check_executor
    from logplane.executors import share_process_group_with_commands
    from logplane.integration import IntegrationConfig, PythonAgentAdapter, load_python_target

    # This process and everything it starts form one group that the parent
    # stops as a whole, on timeout or interrupt. A CLI agent given a session
    # of its own would be outside that group, and would be left running when
    # the parent kills this process before it can clean up.
    share_process_group_with_commands()
    try:
        if args.config:
            executor = IntegrationConfig.from_file(args.config).build_executor()
        else:
            target = load_python_target(args.python_target)
            executor = target if callable(getattr(target, 'execute', None)) else PythonAgentAdapter(target)
        envelope = {'kind': 'report', 'report': check_executor(executor).to_dict()}
    except Exception as exc:
        envelope = {'kind': 'error', 'message': f'{type(exc).__name__}: {exc}'[:2048]}
    encoded = json.dumps(envelope, allow_nan=False).encode('utf-8')
    if len(encoded) > MAX_REPORT_BYTES:
        envelope = {'kind': 'report', 'report': {
            'outcome_type': None,
            'checks': [{'name': 'probe report', 'ok': False, 'required': True,
                        'detail': 'adapter check report exceeds the 64 KiB limit'}]}}
        encoded = json.dumps(envelope).encode('utf-8')
    # The enclosing directory is private. Direct adapter stdout/stderr are
    # discarded, so noise or inherited pipe handles cannot corrupt/block JSON.
    with open(args.report, 'xb') as stream:
        stream.write(encoded)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--python-target')
    group.add_argument('--config')
    _child(parser.parse_args())
