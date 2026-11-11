# Contest hunt: several models, one Logplane workflow

Runs a bug bounty contest hunt as a Logplane workflow:

1. **Map**: one model reads the rules, known issues, design documents and the
   in-scope diff, and splits the changed code into hunting areas with the
   invariants funds depend on.
2. **Hunt**: every area goes to every hunter model. Each hunter may report up to
   three Critical candidates with a concrete attack path, or nothing.
3. **Triage**: one model checks each candidate against the code, merges
   duplicates, rejects known, excluded or sub-Critical ones, and ranks the rest.
4. **You**: read `report.md`. Nothing is submitted to any contest. Every lead
   needs a proof of concept at the scope commit, and you submit it yourself.

Precision is the hard part. Published studies put the share of AI audit
findings that are real at 20 to 35%, even for the best models. The models widen
the search; they do not replace your review.

## What each model can do

| Runner | Authentication | Access |
|---|---|---|
| `claude` | your Claude Code login, checked with `claude auth status`; `--safe-mode` | Read, Grep, Glob inside the run's workspace; nothing else |
| `codex` | your Codex login with ChatGPT, checked with `codex login status`; API keys and endpoints removed from its environment | `--sandbox read-only` in the workspace: may read files and run read-only commands; cannot write or reach the network from commands; runs that change files are discarded |
| `hermes` | the provider you name, for example `nous` | no tools: the rules, known issues, primer and the area's diff are given inline |

Workers only ever see the run's workspace: a fresh export of the contest
repository at the scope commit, plus the contest material. Your own checkout is
never touched.

## Confidentiality

The contest's terms make your findings, proofs of concept and the
vulnerabilities themselves the organiser's confidential information. Model
providers see what the models produce. Check that your plans do not train on
your data, and think about which providers you are comfortable with: Nous
routes some open models to third-party hosts.

## Set up

1. Clone the contest repository somewhere of your own:
   ```sh
   git clone https://github.com/GuardianOrg/templar-protocol-precept-bounty-contest-guardian ~/audits/precept
   ```
2. Copy `models.example.json` to a file of your own and set the model names.
   Use the names your tools show, not guesses:
   - Codex: the model id for Astra, as `codex` lists it. Run
     `codex exec --help` and check it offers `--ignore-user-config` and
     `--output-schema`; if not, update Codex.
   - Claude Code: `fable` and `opus` are aliases; a full model name also works.
   - Hermes: the GLM-5.3 id your Nous provider uses.
   Set `command` on a model if its CLI is not on your PATH.
3. Check each login: `claude auth status`, `codex login status` (must say
   ChatGPT), and that Hermes works with your provider.

## Run a pilot first

One area, every hunter, no mapping. It checks every runner end to end and shows
you what the output is like, for a small fraction of a full run:

```sh
.venv/bin/python -m examples.contest_hunt.run --yes \
  --contest examples/contest_hunt/contests/precept/contest.json \
  --models ~/audits/precept-models.json --repo ~/audits/precept \
  --skip-map --areas A02
```

Then the full hunt:

```sh
.venv/bin/python -m examples.contest_hunt.run --yes \
  --contest examples/contest_hunt/contests/precept/contest.json \
  --models ~/audits/precept-models.json --repo ~/audits/precept
```

Useful options: `--hunters astra,opus` to use some hunters only;
`--areas A01,A03` to hunt chosen areas; `--max-tokens N` for a Logplane token
budget across the run; `--output DIR` (default `hunt-<time>/` in the current
folder); `--stage-timeout-hours`.

A full run is roughly areas × hunters tasks (ten seed areas × three hunters is
30), each taking minutes to half an hour. Subscriptions have usage limits; keep
`capacity` low per model and start with the pilot.

## What you get

In the output directory:

- `report.md`: the triaged findings with proof-of-concept plans, what triage
  rejected and why, and every candidate with the area and model that found it;
- `results/`: map, hunts (including what each hunter checked and found safe),
  candidates and triage as JSON;
- `records/`: each worker's record of what its model received and returned,
  with usage;
- `workspace/`: exactly what the models could read, including every brief;
- `logs/` and the Logplane history (`.logplane/`): every task, attempt and
  model call. If a worker crashes mid-task, Logplane reassigns it.

## Add another contest

Make a folder under `contests/` with `contest.json` (repository, commits, scope
files, seed areas, extra documents), `rules.md`, `known_issues.md` and
`primer.md`. `contests/precept/` is the worked example.
