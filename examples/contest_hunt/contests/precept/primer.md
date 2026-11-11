# Soroban primer for auditing Precept

Models often reason about Soroban as if it were the EVM. Check every claim
against these points and the code itself.

- **Authorization.** There is no msg.sender. A contract checks that an address
  authorized the call with `address.require_auth()` (or
  `require_auth_for_args`). A state change on a user's behalf without the right
  `require_auth` is the Soroban analogue of a missing access check.
- **No re-entrancy.** The Soroban host does not let a contract be re-entered
  while it is already on the call stack. EVM re-entrancy patterns do not apply;
  cross-contract call ordering and stale reads still can.
- **Atomicity.** A panic or error reverts the whole transaction. With
  `overflow-checks = true` (set in this workspace's release profile), integer
  overflow panics rather than wrapping: overflow is a revert (denial of
  service), not silent corruption.
- **Amounts and fixed point.** Amounts are i128. Blend scales rates and factors
  by 1e7 and b_rate / d_rate by 1e12 (b_rate starts at 1e12 and falls when
  defaults are applied). Check rounding direction (`fixed_mul_floor` vs
  `fixed_mul_ceil`, `to_b_token_up` vs down) on every conversion: rounding in
  the user's favour on a repeatable path can drain value.
- **Storage and TTL.** Storage is instance, persistent or temporary. Entries
  have a time to live; anyone can extend an entry's TTL, so TTL is never a
  security deadline. Expired persistent entries are archived and must be
  restored; expired temporary entries are gone.
- **Time.** `env.ledger().timestamp()` and ledger sequence numbers.
- **Tokens and oracles.** Tokens follow SEP-41; prices come through the SEP-40
  adapter (`lastprice`). Wrong oracle data is out of scope, but how the code
  handles valid-but-adverse prices is in scope.
- **Resource limits.** Transactions have CPU, memory and footprint limits; a
  user with many positions or reserves can make operations exceed them.
- **Liquidations in Blend** run as Dutch auctions (user liquidation, bad debt,
  backstop interest), with the backstop covering bad debt. Precept changes how
  bad debt, defaults and auctions settle: read the ADRs before assuming Blend v2
  behaviour.
