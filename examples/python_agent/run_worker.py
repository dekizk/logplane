"""Wire ExistingAgent to logplane without changing its core logic."""

import os

from logplane import BusClient, PythonAgentAdapter, WorkerRuntime

from examples.python_agent.agent import ExistingAgent


def main():
    name = "minimal-python-agent"
    runtime = WorkerRuntime(
        BusClient(
            os.environ.get("LOGPLANE_URL", "http://127.0.0.1:8765"),
            actor=name,
        ),
        name=name,
        capabilities=("python-example",),
        executor=PythonAgentAdapter(ExistingAgent()),
    )
    runtime.run()
    # Non-zero only when the disconnect timeout stopped the worker, so a
    # supervisor can restart it on failure.
    if runtime.exit_code:
        raise SystemExit(runtime.exit_code)


if __name__ == "__main__":
    main()
