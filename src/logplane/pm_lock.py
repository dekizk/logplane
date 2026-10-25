"""Where the single-PM lock lives, and whether a PM currently holds it.

The PM takes an exclusive ``flock`` on a per-user, per-bus-URL file for its
whole lifetime. Other local tools can ask whether that lock is held without
reading the event log, so ``logplane doctor`` can tell a stopped coordinator
apart from one that is simply idle.
"""

from __future__ import annotations

import errno
import fcntl
import getpass
import hashlib
import os
import tempfile
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def lock_path(bus_url: str) -> Path:
    """One lock per (user, machine, bus URL), not per checkout directory.

    Keyed by bus URL so two PMs from different working copies still exclude
    each other. This is only a local-process guard: a PM on another machine
    or running as another OS user is not excluded.
    """
    digest = hashlib.sha256(bus_url.encode()).hexdigest()[:16]
    base = Path(os.environ.get("XDG_RUNTIME_DIR") or tempfile.gettempdir())
    return base / f"logplane-pm-{getpass.getuser()}-{digest}.lock"


def pm_status(bus_url: str, *, path: Optional[Path] = None) -> dict:
    """Report whether a PM on this machine holds the lock for ``bus_url``.

    Returns ``{"status": ..., "detail": ...}`` where status is ``running``,
    ``not_running``, ``not_found`` (no PM has run for this URL here), or
    ``unknown`` (the bus is not local, so its PM would not be visible).
    """
    host = urlparse(bus_url).hostname or ""
    if host not in LOOPBACK_HOSTS:
        return {"status": "unknown", "detail": "the bus is not on this machine"}
    path = path or lock_path(bus_url)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return {"status": "not_found", "detail": f"no PM has run for {bus_url} on this machine"}
    except OSError as exc:
        return {"status": "unknown", "detail": f"cannot read the PM lock: {exc.strerror}"}
    try:
        try:
            # A shared, non-blocking request fails only while a PM holds its
            # exclusive lock; it is released immediately either way.
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                pid = os.pread(fd, 32, 0).decode(errors="replace").strip()
                return {"status": "running", "detail": f"pid {pid}" if pid.isdigit() else "running"}
            return {"status": "unknown", "detail": f"cannot check the PM lock: {exc.strerror}"}
        fcntl.flock(fd, fcntl.LOCK_UN)
        return {"status": "not_running", "detail": f"no PM is running for {bus_url}"}
    finally:
        os.close(fd)
