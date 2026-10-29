# Compatibility and support policy

This document defines the pre-v1.0 baseline and the intended v1.x contract.
It does not declare v1.0 released or retroactively certify every old version.
Release status and outstanding evidence live in [ROADMAP.md](ROADMAP.md).

## Supported deployment target

logplane targets one trusted local host running macOS or Linux, using CPython
3.10 or newer. The CI matrix covers 3.10, 3.13, and 3.14; results for a new
matrix entry must pass before that entry is treated as verified. Other Python
versions are not verified merely because package metadata permits installation.
Native Windows is not supported: process locking uses `fcntl`. WSL, alternative
interpreters, network filesystems, and distributed deployments are not certified.

CI tests the declared direct dependency minimums on Python 3.10 separately from
latest-compatible resolution. The floor constraint file is a test input, not
a production lockfile or a claim to test every transitive dependency combination.
Resolved dependency versions are retained with each run. Supported releases
require a passing package-install smoke, not just tests in an editable checkout.

## Versioned surfaces

| Surface | Current baseline | Compatibility boundary |
| --- | --- | --- |
| Package | `0.14.1` release | Pre-1.0 changes must be documented; do not infer a 1.x guarantee yet |
| Python integration | Names exported by `logplane`, `logplane.adapters`, `logplane.protocol` | Documented constructors, methods, typed outcomes, and effect identity |
| Executor wire format | `agent-bus.executor` v1 | Selected envelope version is explicit; headers are strict |
| Event envelope | Built-in writes use schema v2 | History is immutable; acceptance and replay semantics are part of the contract |
| Local/adapter configuration | Each uses schema v1 | Version and supported keys must match that component |
| CLI | Documented commands, options, `--json` fields, and exit statuses | Human-readable wording/layout is not an API |
| Database indexes / PM snapshots | Derived local caches | No public table layout or snapshot-file compatibility promise |
| Recovery bundles | Versioned verified local format | Unsupported formats must fail safely; hashes are integrity checks, not signatures |

Implementation modules live under `logplane.*` (for example `logplane.bus`),
but integrations should import from `logplane`, `logplane.adapters`, and
`logplane.protocol`. Private helpers, reducer dataclass internals,
SQLite tables, test fixtures, and benchmark output are not public integration
APIs. The `demo` module behind `logplane demo` is a CLI implementation detail,
not an integration API. Importable implementation modules are not a promise
to freeze their private members. Repository examples are source assets, not installed packages.

### Additions and unknown values

- Do not remove or change the meaning/type of an existing public field silently.
  A new optional field needs a default and a compatibility test. Adding a
  required field or changing interpretation is a breaking change.
- Do not assume every JSON object permits arbitrary keys. Executor envelopes
  and protocol headers, local/adapter configs, and some payloads use strict
  shapes. A change rejected by an older supported reader needs a new format
  version or an explicit coordinated upgrade, not the label "additive".
- Executor v1 `supported_versions` may advertise future versions as long as the
  selected version is included. It is not automatic negotiation; use the same
  selected version at both ends. Legacy unversioned v0 is limited to the existing
  legacy subprocess path; new configured CLI/HTTP integrations use v1.
- New built-in writes use event schema v2. Legacy stored rows remain readable;
  readable history does not mean old events satisfy every modern reducer rule.
  Existing migrations and release fixtures, not a blanket "all history works"
  statement, define demonstrated coverage.
- Unknown custom topics are storage extensions, not permission to influence the
  PM. Keep custom readers tolerant of unrelated topics. Raw telemetry topics
  are not replayed by the PM; compact `workflow.usage_recorded` coordination
  events may affect budget admission.
- CLI JSON consumers should tolerate additional object fields and avoid parsing
  human output. Existing fields must retain their meaning. Unknown status/enum
  values should be displayed as unknown, not treated as completed/successful.
  Newly emitted enum values require release notes and a consumer review.

### CLI exit statuses

| Code | Meaning |
| ---: | --- |
| 0 | Command succeeded; inspect the result for task/diagnostic state |
| 2 | Invalid usage/configuration, transport failure, or rejected operation |
| 3 | Lookup did not find the requested task/workflow |
| 4 | An adapter probe fails/times out, a storage audit fails, or replay rejects a recorded operator intent |
| 130 | Interrupted by the operator |

Zero does not mean a submitted task completed, or that all doctor checks are
healthy: read `--json` and the check/state fields. Errors currently go to stderr
and need not be JSON even when `--json` was selected. Unexpected crashes are not
a stable error protocol. Tail output is a stream, not one JSON document.

## Upgrade, downgrade, and deployment versions

1. Read release notes. Stop PMs, workers, submitters, and artifact publishers.
2. Preserve configuration and a verified database/artifact backup using
   [RECOVERY.md](RECOVERY.md). Do not delete history or copy a live SQLite file
   without its supported backup mechanism.
3. Upgrade bus, PM, workers, and operator CLI together. Mixed coordination
   package versions are not a supported rolling-upgrade strategy. External
   adapters can remain on their explicitly supported wire version.
4. Start the bus to run transactional migrations, then PM/workers; verify
   doctor, pending work, and representative task/workflow output.

Indexes may be rebuilt and incompatible snapshots discarded. These operations
must not rewrite original events. Downgrading binaries over an upgraded live
database is unsupported and is not universally rejected by the current code.
For rollback, stop new processes and restore a pre-upgrade backup into a separate
directory with its matching version. Later work and external side effects do
not disappear or roll back automatically; reconcile them before resuming.

## Release and deprecation rules

Before v1.0, releases may change contracts but must document known breaks and
provide tests/upgrade guidance. The readiness baseline protects v0.11.0 public
exports and representative adapter messages; it is not exhaustive coverage of
all old releases, CLI outputs, or historical database semantics.

The released-database baseline now also captures v0.11.0's actual SQLite
schema/history/snapshot as SQL, plus release-produced CLI and adapter outputs.
Tests reconstruct disposable databases, run current initialization repeatedly,
and verify raw event values/identity, idempotency, counters, snapshot fallback,
index reconstruction, and continued reconciliation. Cases include DAG result
propagation, human decisions, retry, pause/resume, cancellation, lease/deadline
expiry, and persisted policy with missing usage. Public JSON comparisons permit
additional fields but preserve existing field types and values.

This demonstrates **v0.11.0 → current development** for the captured cases.
v0.12.0 adds CLI commands and options only (`demo`, `submit --depends-on`, and
`demo-worker` outcome flags), corrects `serve`'s startup-failure status to 2,
and changes no event, executor, configuration, or `--json` field contract.
It adds no schema migration and does not certify physical WAL recovery, every
historical release, or downgrades. See the [fixture provenance](tests/fixtures/compatibility/v0.11.0-upgrade/README.md).

v0.13.0 renames the project from agent-bus to logplane: the distribution,
import package, `logplane` command, `LOGPLANE_*` environment variables, and
`logplane.local.json` config file. The pre-0.13 flat modules and the
`agent_bus` package are removed, and old `AGENT_BUS_*` variables or an
`agent-bus.local.json` without a `logplane.local.json` fail with a rename
message instead of being ignored. Identifiers stored in history or shared with
external adapters keep their original spelling, so existing databases,
backups, and adapters work unchanged: the `agent-bus` ownership owner, the
effect-key salts behind `effect_id()`, the `agent-bus.executor` protocol name,
the `X-Agent-Bus-*` HTTP headers, the `agent_bus_conformance_probe` context
key, and the backup bundle kinds. See the
[migration steps](RELEASE_NOTES.md#migrating-from-agent-bus).

The owner-approved v1.0 upgrade baseline is **v0.11.0**: this is the oldest
starting release for a supported direct upgrade to v1.0. Direct upgrades from
earlier releases are not promised. Any intervening release included in the
supported path needs its own release-derived regression coverage; do not infer
coverage solely from its version number. The final v1.0 build must pass these
upgrade checks before release. This is a selected support scope, not a claim
that v1.0 has shipped or that every historical database has been tested.

For v1.x, patches are intended for compatible fixes; minor releases may add
compatible capabilities. Removing supported APIs or changing existing wire/
event semantics requires a major version, except urgent security corrections
whose impact and mitigation must be called out explicitly. Deprecations should
be documented in release notes and remain available through the current major
series; use runtime warnings where actionable and safe for protocol output.
Bug fixes can change behavior outside the documented contract.

Maintenance targets the latest release; no LTS/backport window or response-time
SLA is promised. The project uses the [MIT License](LICENSE), with copyright
held by deki and development acknowledgements in [CONTRIBUTORS.md](CONTRIBUTORS.md).
Trials continue using GitHub source installations; PyPI publication, native
Windows, and a distributed service remain separate decisions. No package is
published by selecting a license. The public 1.x promise requires
the remaining roadmap gates, including independent onboarding evidence.

Building from source requires setuptools 77.0.3 or newer for standard SPDX
license metadata. Isolated builds install that build dependency automatically;
the runtime Python and dependency requirements are unchanged.

## Running the release checks

From a development checkout with a supported Python:

```sh
python -m pip install -r requirements-dev.txt
python -m pip install -e .
python -m pytest -q
python -m build
python scripts/check_release.py --wheel dist/logplane-0.13.0-py3-none-any.whl
```

Use the wheel filename produced by the build if its version differs. The build
creates an sdist and builds the wheel from it. The check creates a fresh venv,
installs the wheel/dependencies, verifies import origins, and exercises a
successful and timed-out CLI adapter check plus a credential-free live task
outside the checkout. It also checks discovery, human response, retry, and
cancellation through a small live worker, including duplicate commands, and
runs released-database upgrade tests against the installed package. It downloads dependencies and
uses only a disposable loopback server; it makes no model calls. It reports the
retained evidence directory and stops its own processes on success or failure.
Pass `--constraints ci/constraints-min.txt` to exercise direct dependency floors.
GitHub CI uses the same check and uploads its diagnostics; configuring CI is
not evidence that remote jobs have already passed.

The workflow follows GitHub's [Python build/test guidance](https://docs.github.com/en/actions/tutorials/build-and-test-code/python),
uses read-only repository permissions, and pins official actions to commit IDs.
