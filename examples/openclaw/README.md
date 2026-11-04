# OpenClaw: first worker trial

This optional, **text-only pilot** runs OpenClaw's `agent exec` once per
assignment. Logplane owns registration, heartbeats, assignment, and completion.
OpenClaw owns the agent turn. Nothing here is imported by Logplane's core.

Tested with OpenClaw **2026.9.6**, GPT-6 Luna/high, and Nous Portal on macOS.
The OpenClaw pilot requires all tools denied and plugins disabled. It includes
a single-task smoke test and a real Hermes-to-OpenClaw DAG demonstration, not a
code-editing adapter or a production-readiness claim. The process runner uses
POSIX process groups; native Windows is unsupported.

## Run the disposable trial

Install Logplane's development dependencies and editable package as described
in the root README. OpenClaw and a logged-in Hermes/Nous Portal installation
are separate prerequisites, not Python package dependencies.

In one terminal, start the **subscription proxy**, not the Hermes agent API:

```sh
hermes proxy start --provider nous --host 127.0.0.1 --port 18645
```

This documented Hermes feature passes raw model requests to Nous Portal using
your existing login. Hermes does **not** execute the task. The config's
`unused-local-proxy` value is a dummy token, not a secret. The proxy accepts any
bearer token: keep it on loopback and stop it with Ctrl-C when finished.

From the repository root in another terminal:

```sh
.venv/bin/python -m examples.openclaw.live_smoke --yes
```

If OpenClaw is not on PATH, provide its executable explicitly. On the machine
used for the recorded trial:

```sh
.venv/bin/python -m examples.openclaw.live_smoke --yes \
  --openclaw-command "$HOME/.openclaw/bin/openclaw"
```

`--yes` approves a paid assignment. The script starts a separate local bus and
PM, runs a capacity-one OpenClaw worker, submits one task returning
`{"value":42}`, checks replayed state, and stops the Logplane services. It
prints the retained evidence directory. `--output /path/to/new-directory`
chooses a new destination; existing destinations are never overwritten.
The supplied config uses `nousportal/openai/gpt-6-luna`, high thinking, a
2,048-token model output setting, and a 120-second worker timeout. No task
retries are enabled; provider/runtime internal retries remain their own concern.

## Connect to your existing bus

Start your bus, PM, and the subscription proxy first. Use an empty disposable
working directory; no repository access is needed for this pilot.

```sh
mkdir -p /tmp/logplane-openclaw-work
.venv/bin/python -m examples.openclaw.run_worker \
  --openclaw-command "$HOME/.openclaw/bin/openclaw" \
  --config examples/openclaw/nous-proxy.json \
  --working-directory /tmp/logplane-openclaw-work \
  --bus-url http://127.0.0.1:8765
```

In another terminal, targeting that same bus:

```sh
logplane submit 'Return a completed outcome with result {"value":42}.' \
  --capability openclaw --max-retries 0 --url http://127.0.0.1:8765
logplane tasks --url http://127.0.0.1:8765
```

## Run the mixed-agent handoff

With the same subscription proxy running, from the repository root:

```sh
.venv/bin/python -m examples.openclaw.mixed_trial --yes \
  --openclaw-command "$HOME/.openclaw/bin/openclaw"
```

Add `--hermes-command /absolute/path/to/hermes` if Hermes is not on PATH.
`--output /path/to/new-directory` selects a new evidence directory. This uses
GPT-6 Luna/high through Nous Portal for both agents and approves two paid
assignments with no automatic task retries.

Both tasks are created before execution. Hermes returns a fresh marker and
three numbers from its context. OpenClaw's own context is empty: its assignment
references Hermes' completion event, and WorkerRuntime resolves that reference
into the dependency result. OpenClaw echoes the marker, sums the numbers, and
identifies the source task and completion event. The trial starts the OpenClaw
worker first and records its dependency-waiting state before starting Hermes.

The script checks results, assignment ordering, dependency references, actual
adapter inputs, causal links, capability routing, and replayed terminal state.
It saves `events.json`, `deliveries.json`, `summary.json`, and service logs,
then stops its temporary bus, PM, and workers. Stop the subscription proxy
yourself afterward. The worker runtimes run in separate threads in this trial
process, while the actual agents run in subprocesses; this is not a test of
independently restarting worker processes.

Hermes runs in safe mode with only its clarification tool available; OpenClaw
has all tools disabled. Neither is given file-editing or shell tools. This
trial collects usage as sidecar evidence only, without telemetry sinks for
either worker: zero/missing costs in the workflow view are **not** evidence
that model calls were free.

## Boundaries and evidence

- A pinned config avoids ambient tool/plugin configuration. There is no Gateway,
  message delivery, or shared agent session. `agent exec` manages ephemeral state.
- Both OpenClaw's success envelope and a valid Logplane outcome are required.
  Text merely saying "done" is not treated as completion.
- The outcome must be bare JSON. GPT-6 Luna complied in both recorded runs;
  another model may need firmer prompting, because output wrapped in a Markdown
  code fence is rejected as `openclaw_invalid_output`, a permanent failure
  that is not retried.
- Prompt and output sizes are bounded; timeout/cancellation terminates the owned
  process group. Model charges already incurred cannot be undone.
- Usage/cost are saved as **OpenClaw-reported evidence**, not yet emitted into
  Logplane telemetry or workflow-budget accounting. Missing usage is not zero.
  OpenClaw may omit its optional tool-count field; the trial records that as
  unknown, while the explicit deny-all config is the tool-access control.
- `summary.json`, `events.json`, and local service logs describe the synthetic
  task. No provider credential is copied into the config or evidence.
- Credential-free tests: `.venv/bin/python -m pytest tests/test_openclaw_example.py -q`.

See [TRIAL_NOTES.md](TRIAL_NOTES.md) for the recorded result and trial-runner fix.
References: [OpenClaw agent exec](https://docs.openclaw.ai/cli/agent#agent-exec),
[Hermes subscription proxy](https://hermes-agent.nousresearch.com/docs/user-guide/features/subscription-proxy).
