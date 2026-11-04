# Agent examples

This directory contains optional examples of connecting existing agents to the
framework-neutral contract in `executors.py`. Nothing under `examples/` is
imported by the logplane core, and example-specific dependencies do not belong
in the core requirements files.

Each example should:

- implement the public `Executor` protocol;
- let `WorkerRuntime` own registration, leases, retries, and lifecycle events;
- keep agent-specific prompts, configuration, and cancellation here;
- use fake-agent tests so the standard suite needs no credentials or paid API;
- document the authority granted to the external agent;
- avoid adding agent-specific topics to the coordination log.

Copyable examples:

- [`python_agent/`](python_agent/) wraps a Python object with `run()`;
- [`cli_agent/`](cli_agent/) uses the versioned stdin/stdout protocol;
- [`http_agent/`](http_agent/) demonstrates the guarded HTTP bridge, retry-safe
  effect identity, and cooperative cancellation;
- [`hermes/`](hermes/) is the realistic model-agent integration, including a
  one-command code review by a fleet of Hermes reviewers with a crash, a cost
  cap, and idempotent filing through a Python agent.
- [`audit_triage/`](audit_triage/) explains the `logplane demo audit-triage`
  pipeline and how to swap its stand-in stages for real agents.
- [`openclaw/`](openclaw/) runs OpenClaw as a tool-free worker: one task, a Hermes ->
  OpenClaw handoff, and a trial that crashes and freezes workers mid-task, each
  through a disposable live bus.
