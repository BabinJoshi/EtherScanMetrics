# Metrics Calculation and Pipeline Logic

This document explains how the repository calculates metrics, how those metrics are merged into MongoDB documents, and how the two main execution paths behave:

- `first-time` transaction fetch for a newly connected wallet
- `daily` incremental processing for an existing user

The implementation lives in `metrics_pipeline/`, with the orchestration in [metrics_pipeline/pipeline.py](metrics_pipeline/pipeline.py), the raw metric computation in [metrics_pipeline/calculator.py](metrics_pipeline/calculator.py), and the merge rules in [metrics_pipeline/merger.py](metrics_pipeline/merger.py).

## High-level data model

The pipeline stores metrics at three nested levels:

- chain level: one entry per blockchain seen in a wallet batch
- wallet level: one entry per wallet address
- user level: one entry per user_id

The nested documents carry the same core counters:

- `wallet_age_days`: days since the earliest known first transaction for that scope
- `active_days`: count of active calendar days for that scope
- `total_transactions_count`: number of transactions seen for that scope
- `_first_tx_date`: internal helper field used to preserve the earliest date across runs

Chain records also track `total_gas_burned`.

## What the pipeline reads

The code does not fetch transactions from the network. It reads local Parquet files staged under:

- `tmp/<user_id>/<wallet_address>/normal/*.parquet` for wallet-specific runs
- `tmp/<user_id>/*/normal/*.parquet` for batched user runs

The staging directory is the source of truth for a run. The pipeline only processes the files that currently exist there.

## Metric calculation in `calculator.py`

### `calculate_batch_metrics(parquet_dir, wallet_address)`

This function is used by the `first-time` and `daily` paths. It scans one wallet directory and returns:

- a list of chain metrics
- the wallet-level active day count
- the wallet-level distinct date set

#### Step 1: Lazy scan

The function uses `pl.scan_parquet()` on `parquet_dir / "normal" / "*.parquet"`. The scan is lazy, so the data is only materialized when a terminal aggregation is collected.

#### Step 2: Normalize columns

The scan adds or casts a few working columns:

- `timeStamp` is cast to `Int64`, then converted from Unix seconds to a calendar date in `tx_date`
- `gasUsed` is cast to `Int64`
- `gasPrice` is cast to `Int64`

This normalization ensures the same math works whether the source parquet stored those values as strings or integers.

#### Step 3: Per-chain transaction metrics

The first aggregation is grouped by `__chain` and computes:

- `first_tx_date` = minimum `tx_date`
- `active_days` = number of distinct `tx_date` values
- `total_transactions_count` = number of distinct `hash` values

This gives one `ChainBatchMetrics` object per chain.

#### Step 4: Gas burned

Gas is computed only for transactions where `from == wallet_address`, which means sender-side gas only.

The implementation then:

1. sorts by `timeStamp` descending
2. deduplicates by `(__chain, hash)` while keeping the newest row
3. computes `gas_cost = gasUsed * gasPrice / 1e18`
4. groups by `__chain`
5. sums the gas cost into `total_gas_burned`

The value is rounded to 6 decimal places before being stored in the chain metric.

The deduplication step matters because a transaction hash can appear more than once in staged data. Keeping the newest row ensures the gas computation is stable if duplicate rows exist.

#### Step 5: Wallet-level active days

The wallet-level active day count is the number of distinct `tx_date` values across all chains for the wallet batch.

The function returns that count as `wallet_active_days` and also returns the underlying `frozenset[date]` so the caller can union dates across multiple wallets at the user level.

### `calculate_user_batch_metrics(tmp_root, user_ids)`

This function is used by `daily_all_flow`. It is the batched version of the calculator and scans all wallets for a group of users in one Polars pass.

#### Input shape

The function builds one parquet glob per user:

- `tmp_root/<user_id>/*/normal/*.parquet`

It then calls `pl.scan_parquet()` with all of those patterns together.

#### File path extraction

The scan includes the source file path as `__path`, and the code extracts the `user_id` from the path with a regex. The wallet address comes directly from the parquet column `__walletaddress`.

#### Aggregations

The batched calculator produces four main summaries:

- `chain_stats`: grouped by `__user`, `__walletaddress`, `__chain`
- `gas_stats`: grouped by `__user`, `__walletaddress`, `__chain`
- `wallet_active`: grouped by `__user`, `__walletaddress`
- `user_active`: grouped by `__user`

The `gas_stats` path uses the same logic as the wallet calculator:

- filter to `from == __walletaddress`
- sort descending by `timeStamp`
- deduplicate by `__user`, `__walletaddress`, `__chain`, `hash`
- compute `gas_cost`
- sum by user, wallet, and chain

#### Returned structure

The result is a mapping of:

- `user_id -> UserBatchAggregate`

Each `UserBatchAggregate` contains:

- `wallets`: a mapping of wallet address to `WalletBatchAggregate`
- `delta_active_days`: the number of distinct active dates across all wallets for that user in the batch

The user-level `delta_active_days` is computed directly in Polars with `n_unique(tx_date)`. That means the code does not need to union date sets in Python for the batched path.

## Merge rules in `merger.py`

The calculator produces batch metrics only. The merger decides how those batch metrics are combined with the document already stored in MongoDB.

### Chain merge: `merge_chain(existing, new)`

If the chain does not exist yet:

- `chain` is set from the batch
- `wallet_age_days` is computed from the batch `first_tx_date`
- `active_days` is set to the batch count
- `total_transactions_count` is set to the batch count
- `total_gas_burned` is set to the batch value
- `_first_tx_date` is stored as an ISO string

If the chain already exists:

- the earliest first transaction date is preserved
- `active_days` is added to the existing value
- `total_transactions_count` is added to the existing value
- `total_gas_burned` is added and rounded to 6 decimals
- `wallet_age_days` is recalculated from the earliest first transaction date

### Wallet merge: `merge_wallet(existing_wallet, wallet_address, merged_chains, delta_active_days)`

Wallet merging works the same way, but at one level higher.

If the wallet does not exist yet:

- `active_days` is set to the batch wallet delta
- `total_transactions_count` is the sum of all merged chain totals
- `_first_tx_date` is the earliest chain first transaction date

If the wallet already exists:

- `active_days` is incremented by the batch wallet delta
- `wallet_age_days` is recomputed from the earliest known first transaction date
- `total_transactions_count` is recomputed from the merged chain set

The function does not try to diff individual transactions. It simply merges the new batch totals into the existing document.

### User merge: `merge_user(existing_doc, user_id, merged_wallets, delta_active_days)`

The user merge is the final top-level aggregation.

If the user document does not exist yet:

- `active_days` is set to the batch delta
- `total_transactions_count` is the sum of all wallet totals
- `wallet_age_days` is derived from the earliest wallet first transaction date
- `last_updated_date` is written with the current UTC timestamp

If the user already exists:

- `active_days` is incremented by the batch delta
- `total_transactions_count` is recomputed from the merged wallets
- `wallet_age_days` is recomputed from the earliest known first transaction date
- `last_updated_date` is updated to now

## First-time flow

### Entry point

The CLI command is:

- `python main.py first-time <user_id> <wallet_address> [--tmp-root TMP]`

The command is implemented in [main.py](main.py) and delegates to `first_time_flow()` in [metrics_pipeline/pipeline.py](metrics_pipeline/pipeline.py).

### Execution steps

1. Fetch the existing MongoDB document for the user with `fetch_user_doc(user_id)`.
2. Log the previous state.
3. Scan the wallet parquet directory with `calculate_batch_metrics()`.
4. Log the delta for the wallet.
5. Merge the wallet batch into the existing document structure.
6. Merge the final user document.
7. Replace the MongoDB document with `replace_user_doc()`.
8. Log the final result.

### Important behavior

Even though the command is named `first-time`, the code still fetches any existing user document first and merges into it if one exists. That means the flow is a merge-and-upsert path, not a pure create-only path.

This is intentional because it keeps the logic consistent with the daily flow. The first-time path differs mainly in scope: it processes one wallet only.

### How the metrics are interpreted in first-time mode

In this path:

- chain `active_days` is the number of distinct active dates in the current wallet batch for that chain
- wallet `active_days` is the number of distinct active dates across all chains in the wallet batch
- user `active_days` is the same as the wallet delta because only one wallet is being processed

The top-level `active_days` is therefore the size of the wallet-level date set, not a sum of chain day counts.

## Daily flow for one user

### Entry point

The CLI command is:

- `python main.py daily <user_id> [--wallets W1 W2 ...] [--tmp-root TMP]`

This delegates to `daily_flow()` in [metrics_pipeline/pipeline.py](metrics_pipeline/pipeline.py).

### Execution steps

1. Fetch the user document once.
2. Log the previous state once.
3. Build the wallet list.
   - If `--wallets` is provided, use it directly.
   - If it is omitted, read the wallet addresses already stored in MongoDB.
4. Process each wallet independently with `calculate_batch_metrics()`.
5. Log a delta block for each wallet.
6. Merge all updated wallets into a single user document.
7. Union the wallet date sets in Python to compute the correct user-level `active_days`.
8. Persist the merged user document once.
9. Log the final result.

### Why the date sets are unioned in Python

The code keeps the wallet-level distinct dates as a `frozenset[date]`. For the daily single-user flow, the user-level active day count must count each calendar day only once across all wallets.

If two wallets were active on the same calendar date, that day should contribute 1 to the user total, not 2. That is why `daily_flow()` unions the wallet sets before calling the final merge.

### Wallet preservation

When only some wallets are processed, the merge logic preserves any untouched wallets from MongoDB by appending them back into the final user document.

This means the daily run only updates the wallets it scanned, while keeping the rest of the user document intact.

## Daily flow for all users

The repository also includes `daily_all_flow(tmp_root, batch_size)` for the multi-user batch job.

This path is not the main focus of the doc, but the key idea is:

- discover user directories under `tmp_root`
- fetch all existing MongoDB documents once
- scan users in batches with `calculate_user_batch_metrics()`
- merge per-user results
- bulk write each batch back to MongoDB

It uses the same metric definitions and merge rules described above.

## Logging behavior

The logger prints three different phases:

- `PREVIOUS RUN`: the document already stored in MongoDB
- `FIRST-TIME BATCH` or `DELTA BATCH`: the newly calculated wallet metrics
- `FINAL RESULT`: the merged document that will be persisted

Logging is informational only. It does not affect the calculations.

## Practical interpretation of the counters

The same field name can mean different things depending on the level:

- chain `active_days` = active days for one chain only
- wallet `active_days` = unique active days across all chains in that wallet batch
- user `active_days` = unique active days across all wallets in that user batch

Likewise, `total_transactions_count` is always counted at the scope it is stored in and then summed during merge.

The system is additive across runs. It does not recompute historical totals from MongoDB; it merges the newly scanned batch onto the existing document.

## Summary

In short:

- `calculator.py` derives per-chain, per-wallet, and per-user batch metrics from parquet files
- `merger.py` combines the batch output with existing MongoDB documents
- `pipeline.py` decides whether the run is first-time, daily for one user, or daily for all users
- `main.py` exposes those flows as CLI commands

For the repository’s primary use cases, the important distinction is:

- `first-time` processes one wallet and creates or updates the user document around that wallet
- `daily` processes all relevant wallets for a user, unions their active dates, and updates the stored user document once