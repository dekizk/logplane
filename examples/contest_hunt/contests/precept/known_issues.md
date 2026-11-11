# Known issues (ineligible)

A finding substantially the same as any of these, by root cause, exploit path,
impact or fix, is invalid.

## Guardian review of Blend Contracts V2 / Precept, Oct 2026 (0 Critical, 0 High, 0 Medium)

- L-01 Oracle failures block liquidation settlement: check_and_handle_user_bad_debt
  values the whole position, fetching debt prices even with no collateral, so an
  oracle failure reverts full liquidation fills and bad-debt settlement. Resolved.
- L-02 A borrower can block liquidation by adding dust collateral in a reserve whose
  price is unavailable (apply_supply_collateral loads no price). Resolved.
- L-03 Admin proposal TTL can be extended by anyone (temporary storage TTL used as a
  deadline in accept_admin). Resolved.
- L-04 Withdrawing with i128::MAX reverts after a default (to_b_token_up overflow once
  b_rate < 1e12). Resolved.
- L-05 Adapter timestamp flooring (bucket_timestamp) defeats the pool's future-dated
  price rejection; a frozen future-dated parent sample can be priced against for up
  to an hour. Acknowledged (not fixed).
- I-01 Stale ABI docs advertise unsupported functions. I-02 Positive balance changes
  remain unclaimable (docs). I-03 Freeze docs imply the pool can be reopened.
  I-04 Bad debt docs misstate loss allocation. I-05 Submit docs omit auction fill
  restrictions. I-06 Activation accepts an incompatible oracle. I-07 Factory accepts
  undeployable take rates. I-08 plan_repay refund comment. I-09 del_auction docs.
  I-10 Stale pool factory comment.

## Code4rena Blend V2 audit (upstream)

- H-01 A reserve's d_supply is incorrectly updated and stored after flash loan execution
- H-02 User can steal other users' emissions due to vulnerable claim implementation
- H-03 Utilization ratio can exceed 100% due to missing validation in withdrawal functions
- M-01 Flash loans allow borrowing from frozen pools, bypassing security controls
- M-02 Invalid utilization ratio check blocks flash loan submission
- M-03 A withdrawal after a bad debt auction is created can leave the auction stuck
- M-04 Overpriced bad debt and interest auctions via duplicate reserves
- M-05 Missing update_rz_emis_data calls in draw and donate
- M-06 Pools outside the reward zone keep receiving BLND
- M-07 Gulped emissions lost if a reserve has no supply
- M-08 Removing a pool from the reward zone loses ungulped emissions
- M-09 Defaulting remaining liability does not delete a partially filled auction
- M-10 Division before multiplication: division by zero DoS at low backstop supply
- M-11 Fee vault can be made insolvent by defaults
- M-12 Repeated dilution of emissions to a longer timeframe
- M-13 Missing reserve interest accrual before backstop take rate update
- M-14 Temporarily inflating total_supply to push utilization towards 100%
- M-15 Edge case breaks APR cap calculation, excessive fee extraction
- M-16 Removed reward-zone pool cannot gulp already distributed emissions
- M-17 Bad debt can be permanently blocked from moving to the backstop
- M-18 Interest auctions enable inflation attacks on backstop vaults

## Certora security assessment of Blend v2 (Apr 2025, upstream)

- H-01 Sybil attacks via unrestricted position size. H-02 Nearly unfillable auctions.
- M-01 Missing MAX_POSITIONS check in flash loans. M-02 Share inflation bypasses
  partial liquidation protection.
- L-01 No off-chain infrastructure for state-changing calls. L-02 Missing check that
  reserve util < 0.95. L-03 Token to shares ratio can be 1:1.

The full reports are in the repository's audits/ folder.
