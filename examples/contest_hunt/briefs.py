"""The instructions each stage receives, written as files in the workspace."""

from __future__ import annotations

import json

STAGE_MARKER = "STAGE: {stage}"

COMMON = """You are part of a team hunting for Critical vulnerabilities in a live
bug bounty contest. Read contest/rules.md first: only bugs that steal, lock or
compromise access to more than 20% of total funds under normal operating
conditions count. Everything else is worthless here, so do not report it.
contest/known_issues.md lists findings that are already known and ineligible.
contest/primer.md explains the platform; check your assumptions against it.

The code is in repo/ (read-only). contest/scope.diff holds the in-scope changes
(test modules removed); contest/scope_files.txt lists the in-scope files.
Background documents are in contest/docs/ and repo/docs/.

Be rigorous. A real Critical needs a concrete path an unprivileged actor can
follow under normal conditions, through code you have read, to a loss or lock of
more than 20% of funds. Never report a finding you have not traced through the
actual code. If you find nothing, say so: an empty result is normal and useful.
"""

MAP = """{marker}

{common}
Your task: map where value moves in the changed code, so the hunt can be split
into focused areas.

1. Read contest/rules.md, contest/known_issues.md, contest/primer.md, the
   documents in contest/docs/ and the ADRs in repo/docs/, then contest/scope.diff.
2. Identify the money-moving flows the changes touch, and the invariants that
   must hold for funds to be safe (for example: total liabilities never exceed
   what the backstop and reserves can cover; a user can never withdraw more than
   their position is worth).
3. Split the changed code into {min_areas} to {max_areas} hunting areas. Each
   area needs an id (A01, A02, ...), a title, the in-scope files to read (paths as
   in contest/scope_files.txt), a focus paragraph naming the specific functions
   and state to attack, and critical_paths: the routes by which a bug here could
   reach a >20% loss or lock. Together the areas must cover every changed file.

These seed areas are a starting point; improve on them:
{seed_areas}

Keep the whole result under 12 KB.
"""

HUNT = """{marker}

{common}
Your task: hunt for Critical vulnerabilities in one area.

AREA {area_id}: {title}
Files: {files}
Focus: {focus}
Critical paths to consider: {critical_paths}

Invariants identified by the mapping stage:
{invariants}

Method:
1. Read the area's files and its changes in contest/diffs/{area_id}.diff (all
   in-scope changes are in contest/scope.diff), then follow calls into unchanged
   code wherever the changes interact with it.
2. For each invariant and critical path, try to break it as an unprivileged
   attacker: borrower, lender, liquidator, backstop depositor, or anyone calling
   a public function. Consider ordering within one transaction, repeated calls,
   rounding on every conversion, edge values (zero, dust, i128 limits), and state
   left behind by earlier operations (defaults, frozen statuses, partial fills).
3. Discard anything excluded by contest/rules.md or substantially the same as an
   entry in contest/known_issues.md.

Report at most 3 candidates, each a concrete attack, not a pattern. For each:
title; location (file:line or function); root_cause; attack_path (numbered
steps); preconditions (and why they are normal operating conditions);
impact; funds_at_risk (how much of total funds, and why it exceeds 20%);
why_new (why it is not a known issue or exclusion); confidence (low, medium,
high); poc_outline (how a Rust test would demonstrate it). Keep each field under
600 characters. In checked, list briefly what you verified is safe. Keep the
whole result under 12 KB.
"""

TRIAGE = """{marker}

{common}
Your task: triage the candidates the hunters reported.

contest/candidates.json lists them, each with an id (C01, C02, ...), the area
and the model that found it. Hunters are wrong most of the time: the published
precision of AI audit findings is 20 to 35%. Your job is to separate the few
real ones from the rest, by checking each against the actual code.

For every candidate:
1. Verify the attack path step by step in repo/. Reject it if any step fails,
   needs a privileged role, depends on wrong oracle data, is excluded by
   contest/rules.md, or matches contest/known_issues.md.
2. Reject it if the impact cannot reach more than 20% of total funds under
   normal operating conditions.
3. Merge candidates with the same root cause into one finding.

Return findings ranked most promising first, at most 8, each with the
candidate_ids it merges, a verdict (likely_critical, needs_poc,
unlikely_critical), location, attack_path, why_new, poc_plan (the Rust test to
write, with setup, steps and the assertion that proves the loss), and
reviewer_notes (what is still uncertain). Put every other candidate in rejected
with a one-line reason (under 160 characters). End with a short summary. Keep
the whole result under 14 KB.
"""


def _seed_lines(areas: list[dict]) -> str:
    return "\n".join(f"- {a['id']} {a['title']}: {', '.join(a['files'])}. {a['focus']}" for a in areas)


def map_brief(seed_areas: list[dict], *, min_areas: int = 6, max_areas: int = 12) -> str:
    return MAP.format(marker=STAGE_MARKER.format(stage="map"), common=COMMON,
                      min_areas=min_areas, max_areas=max_areas, seed_areas=_seed_lines(seed_areas))


def hunt_brief(area: dict, invariants: list[str]) -> str:
    return HUNT.format(marker=STAGE_MARKER.format(stage="hunt"), common=COMMON,
                       area_id=area["id"], title=area["title"], files=", ".join(area["files"]),
                       focus=area["focus"], critical_paths=area.get("critical_paths") or "not mapped",
                       invariants="\n".join(f"- {i}" for i in invariants) or "- (none mapped)")


def triage_brief() -> str:
    return TRIAGE.format(marker=STAGE_MARKER.format(stage="triage"), common=COMMON)


def number_candidates(hunts: list[dict]) -> list[dict]:
    """Flatten hunt results into numbered candidates with provenance."""
    numbered = []
    for hunt in hunts:
        for candidate in hunt["result"].get("candidates", []):
            numbered.append({"id": f"C{len(numbered) + 1:02d}", "area": hunt["area"],
                             "model": hunt["model"], **candidate})
    return numbered


def candidates_json(candidates: list[dict]) -> str:
    return json.dumps(candidates, indent=2) + "\n"
