# Contest rules: Templar Precept bounty (Guardian Defender)

Precept is Templar's fork of Blend v2 (Soroban, Stellar) plus a SEP-40 pool
oracle adapter. Contest window: Oct 27 to Nov 26, 2026.

## What counts

- **Critical issues only.** Only vulnerabilities that allow stealing, locking,
  or compromising access to **more than 20% of total funds under normal
  operating conditions** are eligible. High, Medium, Low and Informational
  findings earn nothing.
- **Scope is the changes** from the audit baseline (b56a2fe2) to the final
  commit (05f38897) in the listed files, including their interactions with
  unchanged code. Findings must reproduce at the scope commit (3a4f2a84).
- **No duplicates.** Once a valid issue is approved, later submissions of
  substantially the same vulnerability earn nothing.
- Submissions need: affected code, root cause, realistic preconditions,
  reproduction steps, proof of concept, impact, assumptions and limitations.

## Out of scope

- Previously known or publicly disclosed issues, including findings from prior
  audits or reviews (see known_issues.md).
- Issues caused solely by incorrect external data or failures of off-chain
  services, oracles, or external systems.
- Issues caused solely by misuse or compromise of privileged roles (admin,
  owner, multisig, governance, key holders).
- Intended restrictions under ADR-0008 and ADR-0011: disabled functionality,
  irreversible status-4 freezes, and sealed reserve configuration.
- Correctly applied supplier losses, documented rounding and terminal residue.
  The synthetic activation shim provides no insurance or recovery value.
- Anything reversible through a standard upgrade, pause, or non-material
  recovery; intended behaviour; best practices; gas; style; documentation.

Implementation defects that violate the intended safeguards remain eligible.

## Testing

Only in local environments. Never against mainnet, public testnets or live
contracts.
