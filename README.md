# logplane

**A local control plane for AI agents.** logplane assigns work, recovers from
crashes, holds risky actions for human approval, and enforces cost caps, and
it drives all of it from an append-only event log instead of a mutable task
board.

![logplane demo recovery: a worker and the coordinator are killed mid-task, then a new worker finishes the workflow from the log](https://raw.githubusercontent.com/dekizk/logplane/main/docs/demo-recovery.gif)

Agent workflows fail in ordinary ways. A worker crashes mid-task, the
coordinator restarts, two runners pick up the same job, an agent needs a human
answer, or a retry repeats an external action. logplane coordinates your agents
whatever framework each one uses, and records what actually happened as
immutable events, deriving current state by replaying them. Ownership,
retries, dependencies, and human decisions are all events, so a crash loses
nothing and every state can be explained.

### See it in a minute

```sh
python3 -m venv .venv && . .venv/bin/activate   # Python 3.10+
python -m pip install logplane

logplane demo recovery       # kill the worker and the coordinator mid-task
logplane demo approval       # an agent asks before an irreversible tool call
logplane demo budget         # a cost cap stops new work at the limit
logplane demo audit-triage   # parallel reviewers, a human severity call,
                              # findings submitted once despite a crash
```

Each demo starts its own bus, coordinator, and workers as real processes in a
throwaway directory, narrates what happens, and keeps the history for you to
inspect. The crashes are real `kill -9`s; the agents are scripted stand-ins, so
their outputs, token counts, and costs are simulated. None of it touches your other
logplane data.

<details>
<summary>Watch <code>logplane demo approval</code>: an agent asks before an irreversible tool call</summary>

![logplane demo approval: a support agent blocks before calling issue_refund, a human approves, and the refund runs once](https://raw.githubusercontent.com/dekizk/logplane/main/docs/demo-approval.gif)

</details>

<details>
<summary>Watch <code>logplane demo budget</code>: a cost cap stops new work at the limit</summary>

![logplane demo budget: ten summaries under a $2 cost cap stop at $1.60, then finish after the cap is raised](https://raw.githubusercontent.com/dekizk/logplane/main/docs/demo-budget.gif)

</details>

### What you get

- **Crash recovery by design.** Workers hold leases. A dead worker's attempt is
  expired and reassigned, and a restarted coordinator rebuilds state by replay.
- **One owner per task.** Only the current attempt can advance a task, and late
  results from a lost worker are rejected.
- **Dependencies.** Tasks form a DAG, wait for upstream results, and propagate
  failure downstream.
- **Humans in the loop.** An agent can block with a question, such as approval
  for a risky tool call. `logplane decide` answers it, and the answer is on
  record.
- **Spend limits.** Model calls report tokens and cost per task and attempt.
  Workflow budgets stop admitting new work at a cap, and raising the cap is an
  event too. They gate admission; they don't cut off a model call already
  running.
- **Retry-safe external effects.** `assignment.effect_id(...)` is stable across
  attempts. Pass it as the idempotency key to the system you call, and that
  system can reject a retry's repeat.
- **Bring your own agents.** Wrap a Python object, a CLI, or an HTTP service.
  logplane never needs your agent's internals.
- **Explainable state.** `logplane task`, `explain`, and `workflow --mermaid`
  answer "why is this waiting?" from the log.

It is local-first: one machine, SQLite, loopback only. See
[trust and scope](#trust-and-scope) for what it does not do yet.

## How it works

The bus never stores a mutable card as truth. Every action (a task created, an
attempt assigned, a worker's result, a human decision) is an immutable SQLite
event. The project manager (PM) derives current state by replaying those
events and reconciles anything missing, so any process can crash and restart
without losing ownership state.

- **You and your tools publish intent:** tasks, dependencies, policies,
  decisions, cancellations.
- **The PM** replays the log, assigns work to live workers, and records lease
  expiries, retries, decision requests, and terminal states.
- **Workers** execute only assignments addressed to their current process and
  report one outcome per attempt.
- **Every view** (`task`, `explain`, `workflow`) is rebuilt from the log.

Four identities keep retries and replays unambiguous: `correlation_id` (one
workflow), `task_id` (one logical outcome), `assignment_id` (one execution
attempt), and `instance_id` (one worker process). The normal lifecycle is
`task.created → task.assigned → task.started → task.completed`; recovery,
human input, cancellation, and pauses are explicit events too.

[How logplane works](https://github.com/dekizk/logplane/blob/main/docs/architecture.md) covers the full lifecycle,
components, invariants, crash recovery, and trust model.

## Install

Use macOS or Linux with Python 3.10+; native Windows is not supported (see the
[tested-platform policy](https://github.com/dekizk/logplane/blob/main/COMPATIBILITY.md#supported-deployment-target)).

```sh
python3 -m venv .venv && . .venv/bin/activate   # python3 --version must be 3.10+
python -m pip install logplane
logplane init
```

`logplane init` writes `logplane.local.json` and a `.logplane/` data directory
in the current directory, so run it in the folder that should keep the bus's
history. If your default `python3` is older than 3.10, create `.venv` with a
supported interpreter such as `python3.11`.

To run the examples or work on logplane itself, install from source instead:

```sh
git clone https://github.com/dekizk/logplane.git && cd logplane
python3 -m venv .venv && . .venv/bin/activate
python -m pip install -e .
```

Create the environment in `.venv`, not in the repository root. The editable
install keeps this checkout as the source of the `logplane` command, so rerun
`python -m pip install -e .` after pulling changes that add modules. Record
`git rev-parse HEAD` when reporting trial results.

## Run your own bus

Run these in three terminals from the folder where you ran `logplane init`,
with `.venv` activated in each. `logplane up` runs the bus and the coordinator
(PM) together and stops both on Ctrl-C. The generated configuration binds only
to loopback and keeps runtime data, including the bus and PM logs, in the
ignored `.logplane/` directory.

```sh
# terminal 1
logplane up

# terminal 2
logplane demo-worker alice

# terminal 3
RESPONSE=$(logplane submit "Verify the logplane quick start" --json)
TASK_ID=$(printf '%s' "$RESPONSE" | python -c \
  'import json,sys; print(json.load(sys.stdin)["payload"]["task_id"])')
CORRELATION_ID=$(printf '%s' "$RESPONSE" | python -c \
  'import json,sys; print(json.load(sys.stdin)["correlation_id"])')
printf 'Task: %s\nWorkflow: %s\n' "$TASK_ID" "$CORRELATION_ID"
```

After about one second, verify the bus, task, and derived workflow:

```sh
logplane doctor
logplane task "$TASK_ID"
logplane workflow "$CORRELATION_ID"
```

The expected state is a healthy schema-v2 bus with one healthy worker and a
running coordinator, a
`completed` task whose last attempt belongs to `alice`, and a one-task
`completed` workflow. The demo worker emits no model telemetry, so
`tokens not reported` and `cost not reported` are expected rather than errors.

<details>
<summary>Machine-readable pass/fail checks</summary>

```sh
logplane task "$TASK_ID" --json | python -c '
import json, sys
task = json.load(sys.stdin)
assert task["status"] == "completed", task
assert task["assignment_active"] is False, task
print("PASS: task is completed with no active assignment")
'

logplane workflow "$CORRELATION_ID" --json | python -c '
import json, sys
workflow = json.load(sys.stdin)
assert workflow["status"] == "completed", workflow
assert workflow["task_count"] == 1, workflow
print("PASS: workflow reports one completed task")
'
```

</details>

If the task is still `assigned` or `started`, wait briefly and check again. If
it stays `open`, check the `Coordinator:` line in `logplane doctor`: open work
is only assigned while a coordinator is running. If `logplane doctor` keeps
warning, the PM, worker, or bus is probably using a different URL; see [connecting to the bus](https://github.com/dekizk/logplane/blob/main/docs/operations.md#connecting-to-the-bus).
If `logplane` is not found, check that `.venv` is activated in that terminal
before reinstalling. `logplane tasks` and
`logplane workflows` find IDs again if you lose them, and
`logplane workflow "$CORRELATION_ID" --mermaid` prints the workflow as a
Mermaid flowchart: another projection over the log, never an editable board.

These checks inspect projected state; they do not prove exactly-once execution
or external effects. To stop, press Ctrl-C in the worker terminal, then in the
`logplane up` terminal, which stops the coordinator and then the bus. Local
history is preserved. `logplane serve` and `logplane pm` still run the two
separately when you want them in different terminals or under a supervisor.

### Try dependencies, decisions, and failures

The demo worker can deliberately block, fail, or work slowly so you can watch
the other paths without writing an agent. Use one outcome flag per worker; the
outcome happens once for that task ID.

```sh
logplane demo-worker alice --block 2           # task 2 asks a human for a decision
logplane demo-worker alice --fail 2            # one retryable failure
logplane demo-worker alice --fail-permanently 2
logplane demo-worker alice --delay 30          # slow enough to kill mid-task
```

`submit --depends-on` builds a DAG from the command line; repeat it for
fan-in. Dependents inherit the upstream workflow automatically (IDs below
assume a fresh bus):

```sh
logplane submit "Research options"                                  # task 1
logplane submit "Design storage" --depends-on 1                     # task 2
logplane submit "Write docs" --depends-on 1                         # task 3
logplane submit "Ship it" --depends-on 2 --depends-on 3             # task 4
```

A lease expiry or retryable failure consumes one retry, and `submit` defaults
to no retries. If you plan to use `--fail` or kill a worker mid-task, submit
with `--max-retries 2` (or higher) so the next attempt is allowed; otherwise
the task correctly ends `failed`.

## Everyday commands

| Command | What it does |
|---|---|
| `logplane submit TITLE` | Create a task (`--depends-on`, `--capability`, `--max-retries`, `--priority`, `--context`) |
| `logplane tasks` / `workflows` / `decisions` | List work, workflows, and pending questions with next-step commands |
| `logplane task ID` / `explain ID` | Show one task's state and exactly why it is waiting or finished |
| `logplane workflow ID` | Show a workflow's tasks, dependencies, policy, and token/cost usage (`--mermaid` for a diagram) |
| `logplane tail ID` | Follow a workflow's events live |
| `logplane decide` / `retry` / `cancel` | Answer a decision, grant more retries, or cancel a task |
| `logplane pause` / `resume` / `supersede` | Pause or resume a task or workflow, or replace a task with new intent |
| `logplane policy` / `agent-policy` | Set workflow concurrency and budgets, or per-agent limits |
| `logplane up` | Run the bus and coordinator together; Ctrl-C stops both (`serve` and `pm` run them separately) |
| `logplane workers` / `doctor` | Check worker leases, overall bus health, and whether a coordinator is running |
| `logplane adapter check` / `adapter run` | Verify and run an existing agent as a worker |
| `logplane backup` / `verify-backup` / `restore` | Verified local backups ([recovery operations](https://github.com/dekizk/logplane/blob/main/RECOVERY.md)) |

Finite commands accept `--json` for scripts. [Operating logplane](https://github.com/dekizk/logplane/blob/main/docs/operations.md)
covers every command, scheduling, fairness, budgets, and controls in detail.

## Connect your own agent

Wrap an existing agent instead of rewriting it. A Python object with `run()`
becomes a worker:

```python
from logplane import BusClient, Completed, PythonAgentAdapter, WorkerRuntime

class ExistingAgent:
    def run(self, assignment):
        result = do_existing_work(
            goal=assignment.goal,
            context=assignment.context,
            idempotency_key=assignment.effect_id("do-existing-work"),
        )
        return Completed("agent finished", {"result_ref": result.ref})

bus = BusClient("http://127.0.0.1:8765", actor="existing-agent")
WorkerRuntime(
    bus,
    name="existing-agent",
    executor=PythonAgentAdapter(ExistingAgent()),
    capacity=2,
    capabilities=["python"],
).run()
```

Check the contract before connecting it to a live bus:

```sh
logplane adapter check --python-target my_package.agent:ExistingAgent
```

CLI and HTTP agents use the same versioned protocol through a small JSON
config. Pass `assignment.effect_id(...)` as the idempotency key for any
irreversible external effect, so the receiving system can reject a repeat when a
task is retried. [Integrating agents](https://github.com/dekizk/logplane/blob/main/docs/integration.md) covers the
adapters, the safety contract, telemetry, and safe adoption alongside an
existing system; [`examples/`](https://github.com/dekizk/logplane/blob/main/examples/README.md) has copyable Python, CLI,
HTTP, and real-agent integrations.

To see real agents under logplane, the [code review example](https://github.com/dekizk/logplane/blob/main/examples/hermes/README.md#review-a-codebase-with-a-fleet-of-reviewers)
runs [Hermes Agent](https://github.com/NousResearch/hermes-agent) with your
own provider in one command. It fans a review of a codebase out to a fleet of
Hermes reviewers, kills one mid-call, trips and raises a real cost cap, has
Hermes merge the findings, and files the ones you choose through a Python
agent, once each. Point it at your own repository:

```sh
python -m examples.hermes.code_review --provider PROVIDER --model MODEL \
  --path ~/src/my-service --include '*.py' --max-chunks 40 --workers 6
```

<details>
<summary>Watch the code review: 15 real model calls, a reviewer killed mid-call, a cost cap tripped and raised, findings merged and filed</summary>

![logplane code review example: four Hermes reviewers review logplane's own source, one is killed mid-call and its chunk reassigned, a cost cap stops new work and is raised, Hermes merges the findings, and a Python agent files the chosen ones once](https://raw.githubusercontent.com/dekizk/logplane/main/docs/demo-code-review.gif)

</details>

## Documentation

| Page | Contents |
|---|---|
| [How logplane works](https://github.com/dekizk/logplane/blob/main/docs/architecture.md) | Event log, identities, lifecycle, components, invariants, crash recovery, trust model |
| [Operating logplane](https://github.com/dekizk/logplane/blob/main/docs/operations.md) | Commands, decisions, retries, cancellation, pauses, DAGs, scheduling, fairness, budgets |
| [Integrating agents](https://github.com/dekizk/logplane/blob/main/docs/integration.md) | Python, CLI, and HTTP adapters, safety contract, telemetry, adoption modes |
| [Event contracts and the HTTP API](https://github.com/dekizk/logplane/blob/main/docs/events.md) | Envelope, topics, limits, reading and publishing events |
| [Local recovery operations](https://github.com/dekizk/logplane/blob/main/RECOVERY.md) | Artifact audits, backup and restore, task-index rebuilds, PM snapshots |
| [Compatibility and support](https://github.com/dekizk/logplane/blob/main/COMPATIBILITY.md) | Platforms, versioned surfaces, upgrades, release checks |
| [Roadmap](https://github.com/dekizk/logplane/blob/main/ROADMAP.md) and [release notes](https://github.com/dekizk/logplane/blob/main/RELEASE_NOTES.md) | Direction, remaining v1.0 gates, and what changed in each release |

## Trust and scope

logplane is for local, single-user orchestration on one trusted host:

- **Perimeter auth is opt-in.** Set `LOGPLANE_TOKEN` to require a bearer token
  on every data route; without it, anyone who can reach the port can publish.
- **Actors are self-reported.** The token authenticates clients, not
  identities, so any authorized client may publish as any actor, including
  `pm`.
- **It is not distributed.** Run one PM per bus and one uvicorn worker; the PM
  lock is per machine and event wake-ups are per process.

See the [trust model and scope](https://github.com/dekizk/logplane/blob/main/docs/architecture.md#trust-model) for details.

## Development

```sh
python -m pip install -r requirements-dev.txt
python -m pytest -q
python -m build
```

The suite focuses on orchestration failures (restarts, stale attempts, retry
exhaustion, races, DAG propagation, adapter contracts, telemetry isolation)
and includes live runs of every demo. See
[running the release checks](https://github.com/dekizk/logplane/blob/main/COMPATIBILITY.md#running-the-release-checks) for
the full pre-release procedure.

## Status and license

logplane was called agent-bus before v0.13.0; if you used it under that name,
follow the [migration steps](https://github.com/dekizk/logplane/blob/main/RELEASE_NOTES.md#migrating-from-agent-bus).

Current release: **v0.14.1**, `logplane up` and a coordinator check in `doctor`, hardened. See the
[release notes](https://github.com/dekizk/logplane/blob/main/RELEASE_NOTES.md) and the [roadmap](https://github.com/dekizk/logplane/blob/main/ROADMAP.md) for the path to
v1.0. Licensed under the [MIT License](https://github.com/dekizk/logplane/blob/main/LICENSE). Copyright (c) 2026 deki. See
[credits and acknowledgements](https://github.com/dekizk/logplane/blob/main/CONTRIBUTORS.md).
