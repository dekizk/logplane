# Contest hunt trial notes

Findings stay out of these notes: the contest's terms make them confidential.
Only counts, timings and failures are recorded.

## 2026-11-22 — Precept pilot, area A02, three runs

Setup: `feat/contest-hunt` after the approval-gate merge, `--skip-map --areas A02`,
workers through the user's own logins (Claude Code with a Max plan, Codex with
ChatGPT, Hermes with Nous). Estimated costs below are each tool's own estimate
at API prices, not what a subscription is charged.

| Run | Hunters | Result |
|---|---|---|
| 1 | astra (Codex `gpt-6.2-astra`), opus, glm | Codex hunt and triage refused by OpenAI's cyber safeguards with 0 tokens; opus 0 candidates ($1.96 est.), glm 3; triage failed, so the report was untriaged |
| 2 | fable, astra with `daybreak`, opus, glm | Codex exited 1 before any event: Daybreak is unavailable for `gpt-6.2-astra`, but the worker had discarded stderr; fable 0 ($7.07 est.), opus 0, glm 1; triage failed |
| 3 | fable, astra (`gpt-5.6-sol` with `daybreak`), opus, glm | All four hunts completed with 0 candidates, so triage did not run |

Run 3 in detail:

| Hunter | Minutes | Turns used / cap | Estimated cost |
|---|---:|---:|---:|
| opus (`opus`) | 3.2 | 23 / 120 | $1.51 |
| astra (Codex `gpt-5.6-sol`, Daybreak) | 3.8 | 1 turn | not reported |
| glm (Hermes, Nous) | 6.1 | — | $0.10 |
| fable (`claude-fable-5-2`) | 11.0 | 31 / 150 | $6.15 |

No lease expiries, crashes, stale results or retries in any run. Every hunt
finished far inside its timeout and turn cap, so the limits did not shape the
result.

What the runs changed:

- The Codex worker passes `-c daybreak=true` when a model sets
  `"daybreak": true`, and checks the CLI version first.
- A refusal by OpenAI's cyber safeguards is reported as `codex_safety_refusal`.
- Codex's own error output is kept, so an early exit says why.

## 2026-11-22 — Precept full hunt and gap-filling runs

Same setup as the pilot. Hunters: fable (`claude-fable-5-2`), astra (Codex
`gpt-5.6-sol` with Daybreak), opus and glm; triage on astra.

| Run | Scope | Result |
|---|---|---|
| Full run | map, then 10 areas × 4 hunters (40 hunts), triage | Map done in 5.7 min. 32 hunts completed, 8 failed. 3 candidates, all from glm; triage rejected all 3 |
| fable-1 | fable on seed areas A05–A07 | 3 completed, 0 candidates (about $18 est.) |
| fable-2 | fable on A08–A10 | 3 completed, 0 candidates (about $14 est.) |
| opus-a10 | opus on A10 | Completed, 0 candidates |
| glm-a05 | glm on A05 | Failed again: malformed JSON about 3,000 characters in |
| astra-ultra | astra on all 10 seed areas at `ultra` effort, timeout 3600 | 10 completed, 0 candidates; 1.9 to 4.8 million tokens per area, against about 1.3 million at `high` |

Failures in the full run:

- Seven Claude hunts (fable on A05–A10, opus on A10) hit the Claude plan's
  5-hour usage limit. Each failed permanently in turn, about 40 minutes in,
  and the gaps had to be refilled by hand after the limit reset.
- glm on A05 returned malformed JSON, in this run and in its own rerun.

Coverage at the end: every area by fable, opus and astra (at two efforts), and
nine of ten by glm. No Critical candidate survived triage. The gap-filling
runs used the contest's seed areas, which differ a little from the areas the
map produced, so coverage does not line up exactly.

Four hunts ran at a time under the workflow's default concurrency of 4. No lease
expiries, crashes or stale results in any run.

What this asks of Logplane:

- **A usage limit should be retried after it resets**, not ended as a
  permanent failure. The message names the reset time, so the adapter could
  report a retryable failure with a matching delay. This is the retry backoff
  item in the reliability list.
- **One retry for a malformed model answer** may be worth having; it would not
  have saved glm on A05, which failed the same way twice.
- **Rerunning one stage** (triage, or a list of hunts) without a full run
  would have saved the gap-filling runs.

### Not yet shown

- A triage pass that accepts a candidate.
- Rerunning triage alone after it fails.
