# Metrics Pipeline — Detailed Documentation

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Data Flow](#data-flow)
4. [Entry Point — `main.py`](#entry-point--mainpy)
5. [Module Reference](#module-reference)
   - [pipeline.py — Orchestration](#pipelinepy--orchestration)
   - [calculator.py — Metrics Calculation](#calculatorpy--metrics-calculation)
   - [merger.py — Incremental Merging](#mergerpy--incremental-merging)
   - [logger.py — Logging](#loggerpy--logging)
   - [mongo.py — Persistence](#mongopy--persistence)
6. [MongoDB Document Schema](#mongodb-document-schema)
7. [Input Parquet Files](#input-parquet-files)
8. [Active Days — How It Works](#active-days--how-it-works)
9. [Gas Burned — How It Is Calculated](#gas-burned--how-it-is-calculated)
10. [Configuration](#configuration)
11. [Dependencies](#dependencies)

---

## Overview

The metrics pipeline is an incremental aggregation system that reads Ethereum wallet transaction data from local Parquet files, computes on-chain activity metrics, merges them with any previously stored data, and persists the result in MongoDB.

It supports three modes:

| Mode | Trigger | Scope |
|---|---|---|
| `first-time` | User connects a new wallet | Single wallet, full history |
| `daily` | Targeted rerun / debugging | All (or selected) wallets for one user, incremental |
| `daily-all` | Scheduled cron job | Every user with staged data, batched single global Polars scan per chunk, bulk write per batch |

The system is designed so that each run only ever sees the **new transactions** in the Parquet directory. It never re-reads historical data — it reads new batch files and adds the computed deltas on top of the existing MongoDB document.

---

## Project Structure

```
EtherScanMetricsTest/
├── main.py                       CLI entry point
├── pyproject.toml
├── .env                          MongoDB credentials
├── TESTING.md                    Manual test scenarios
└── metrics_pipeline/
    ├── __init__.py
    ├── pipeline.py               Orchestration — first_time_flow, daily_flow, daily_all_flow
    ├── calculator.py             Reads Parquet files, outputs per-chain metrics
    ├── merger.py                 Merges new metrics with existing MongoDB data
    ├── logger.py                 Structured logging (stdout + file)
    └── mongo.py                  MongoDB read/write
```

---

## Data Flow

### `first-time` / `daily` (single user)

```
 CLI (main.py)
      │  user_id, wallet_address(es), tmp_root
      ▼
 pipeline.py  ─── fetch_user_doc() ──▶  MongoDB (existing state)
      │                                       │
      │  existing_user_doc ◀──────────────────┘
      │
      │  for each wallet:
      ▼
 calculator.py
      │  reads:  tmp/<user_id>/<wallet>/normal/*.parquet
      │  outputs: list[ChainBatchMetrics], wallet_active_days, active_date_set
      ▼
 merger.py
      │  merge_chain()   — new chain metrics + existing chain doc
      │  merge_wallet()  — merged chains → wallet doc
      │  merge_user()    — merged wallets → user doc
      ▼
 mongo.py
      │  replace_user_doc() — single upsert
      ▼
 logger.py  (called at each stage: previous, delta, final)
```

### `daily-all` (all users, batched global Polars scan)

```
 CLI (main.py)
      │  tmp_root, batch_size
      ▼
 pipeline.py
      ├── scans tmp_root/ for user directories (sorted)
      │
      ├── fetch_all_user_docs() ──▶  MongoDB   (ONE query for all users)
      │         │
      │  existing_docs dict ◀────────┘
      │
      ├── chunks user_ids into batches of `batch_size`
      │
      └── for each batch:
            │
            ├── calculate_user_batch_metrics(tmp_root, batch_user_ids)
            │      └── ONE pl.scan_parquet over every parquet in the batch
            │           (streaming engine, Polars handles intra-batch parallelism)
            │           → {user_id: UserBatchAggregate{wallets, delta_active_days}}
            │
            ├── for each user in the batch:
            │      ├── log_previous() — existing Mongo doc for the user
            │      ├── merger.py: _merge_one_wallet() per wallet → merge_user()
            │      ├── log_delta()  per wallet
            │      └── log_final()  per user
            │
            └── bulk_replace_user_docs(batch_docs) ──▶  MongoDB
                       (ONE bulk_write per batch)

 logger.py  (per-wallet log_delta, per-user log_final; failed batches logged and skipped)
```

Memory is bounded by `batch_size`, not by total user count. A failed batch is
logged and the remaining batches still run; the blast radius of one
corrupt parquet file is one batch, not the whole job.

---

## Entry Point — `main.py`

The CLI provides three subcommands.

### `first-time`

```
python main.py first-time <user_id> <wallet_address> [--tmp-root TMP]
```

Called when a user connects a wallet for the first time. Processes all Parquet files currently present in `tmp/<user_id>/<wallet_address>/normal/` and saves the result to MongoDB.

### `daily`

```
python main.py daily <user_id> [--wallets W1 W2 ...] [--tmp-root TMP]
```

Single-user targeted run. Useful for reruns, debugging, or backfilling one user. If `--wallets` is omitted, all wallet addresses stored in the existing MongoDB document are processed. Fetches once from MongoDB and writes once.

### `daily-all`

```
python main.py daily-all [--tmp-root TMP] [--batch-size N]
```

The standard production cron entry point. Discovers every user directory under `tmp_root`, fetches all existing MongoDB documents in a single query, then processes users in chunks of `batch_size`. Each chunk runs **one** `pl.scan_parquet` over every parquet under the chunk's user dirs and issues **one** `bulk_write` to MongoDB. A failed batch is logged and skipped — remaining batches still run.

| Flag | Default | Description |
|---|---|---|
| `--tmp-root` | `tmp` | Root directory containing staged user/wallet Parquet files |
| `--batch-size` | `1000` | Users processed per Polars scan + Mongo `bulk_write`. Memory scales with batch size, not total user count. |

All three commands delegate immediately to `pipeline.py`.

---

## Module Reference

### `pipeline.py` — Orchestration

This module is the glue layer. It fetches the current MongoDB state, drives the calculator and merger for each wallet, then saves the result. All three public entry points share the same internal helpers — the only difference is how they fetch, parallelise, and persist.

---

#### `first_time_flow(user_id, wallet_address, tmp_root)`

1. Calls `fetch_user_doc(user_id)` to get any existing MongoDB document.
2. Logs the previous state via `log_previous()`.
3. Calls `_process_wallet()` for the single wallet.
4. Calls `_assemble_and_save()` to merge and persist the result.

The `delta_active_days` passed to `_assemble_and_save` is `len(active_date_set)` — the full set of distinct dates seen in the batch.

---

#### `daily_flow(user_id, wallet_addresses, tmp_root)`

1. Calls `fetch_user_doc(user_id)`.
2. Logs the previous state.
3. If `wallet_addresses` is `None`, derives the list from the existing MongoDB document.
4. Iterates over each wallet, calling `_process_wallet()` and **unioning** the `active_date_set` results across wallets.
5. Calls `_assemble_and_save()` with the combined results.

The union of date sets is critical: if two wallets were both active on 2024-03-01, that day should count as only **one** user-level active day.

---

#### `daily_all_flow(tmp_root, batch_size=1000)`

The scalable production entry point. Processes every user with staged data via batched global Polars scans — **no Python-level thread pool**:

1. Scans `tmp_root/` for subdirectories — each directory name is a `user_id`. Only users with a staged directory are processed; users with no new data are untouched. The list is sorted for deterministic ordering.
2. Calls `fetch_all_user_docs()` — **one MongoDB query** loads all existing user documents into an in-memory dict keyed by `user_id`.
3. Chunks the discovered user list into batches of `batch_size`.
4. For each batch:
   - Calls `calculate_user_batch_metrics(tmp_root, batch_user_ids)` — **one** `pl.scan_parquet` over every parquet under the batch's user dirs, executed via Polars' streaming engine. Returns `{user_id: UserBatchAggregate}`.
   - For each user in the batch:
     - Looks up the existing Mongo doc from the pre-fetched dict and logs it via `log_previous()` so the run output reads top-to-bottom as **previous state → delta → final state** per user.
     - For each wallet: calls `_merge_one_wallet()` (re-using the same merge logic as the per-wallet flow), with `log_delta()`.
     - Calls `_assemble_user_doc()` to produce the final merged document; logs it via `log_final()`.
   - Calls `bulk_replace_user_docs(batch_docs)` — **one `bulk_write`** per batch with `ordered=False`.
5. Logs a final summary: total documents written and any failed batches.

**Why no `ThreadPoolExecutor`?** Polars already parallelises across all CPU cores internally via its Rust thread pool. Wrapping it in a Python-level thread pool adds contention (workers fighting for the same cores), not parallelism. The per-wallet loop in the old implementation was the slow path because each wallet did its own `scan_parquet` and underused Polars; one global scan per batch lets the optimiser see the whole workload and stream through it efficiently.

**Why batched, not one single global scan?** Two reasons:
- **Memory.** A single scan over millions of parquets would push intermediate state through Polars' streaming engine — workable, but uncapped. Batching gives a hard memory ceiling controlled by `batch_size`.
- **Fault isolation.** If one parquet in a batch is corrupt and the scan errors out, only that batch is lost; the rest of the run still completes. With a single global scan, one bad file kills everything.

**Tuning `batch_size`:** Start at 1000. Larger batches amortise overhead better but use more memory; smaller batches give tighter fault isolation. The Polars streaming engine handles datasets larger than RAM, so the right knob to turn is usually fault isolation, not memory.

---

#### `_process_wallet(user_id, wallet_address, tmp_root, existing_user_doc, is_first_time)`

The per-wallet routine used by `first_time_flow` and `daily_flow`:

1. Constructs `parquet_dir = tmp_root / user_id / wallet_address`.
2. Calls `calculate_batch_metrics(parquet_dir, wallet_address)` to get raw metrics from Parquet.
3. Logs the batch via `log_delta()`.
4. Delegates to `_merge_one_wallet()` to perform the chain/wallet merge.
5. Returns `(merged_wallet, wallet_delta_active_days, active_date_set)`.

---

#### `_merge_one_wallet(existing_user_doc, wallet_address, chain_batch_list, wallet_delta_active_days) → dict`

Shared merge helper used by both the per-wallet flow (`_process_wallet`) and the batched flow (`daily_all_flow`):

1. Looks up the existing wallet and chain documents from `existing_user_doc`.
2. Calls `merge_chain()` for every chain in the new batch.
3. Appends any chains that existed in MongoDB but are **not** in the new batch (so they are preserved unchanged).
4. Calls `merge_wallet()` to build the merged wallet document and returns it.

Pulling this out of `_process_wallet` lets `daily_all_flow` apply the same merge logic to chain metrics produced by a global scan without re-scanning per wallet.

---

#### `_assemble_user_doc(existing_user_doc, user_id, updated_wallets, updated_addresses, user_delta) → dict`

Builds and **returns** the final user document without touching MongoDB:

1. Appends any wallets from MongoDB that were **not** in `updated_addresses` (preserves untouched wallets unchanged).
2. Calls `merge_user()` to produce the complete user document.
3. Returns the document — the caller decides when and how to persist it.

This separation is what allows `daily_all_flow` to collect all results first and then flush them in one bulk write.

---

#### `_assemble_and_save(existing_user_doc, user_id, updated_wallets, updated_addresses, user_delta)`

Thin wrapper used by `first_time_flow` and `daily_flow`:

1. Calls `_assemble_user_doc()` to build the document.
2. Calls `replace_user_doc()` to upsert into MongoDB immediately.
3. Calls `log_final()` to log the result.

---

### `calculator.py` — Metrics Calculation

Reads Parquet files and produces structured metrics. This is the only module that touches the filesystem (other than the logger).

---

#### `ChainBatchMetrics` (dataclass)

| Field | Type | Description |
|---|---|---|
| `chain` | `str` | Blockchain name (value of the `__chain` column) |
| `first_tx_date` | `date` | Earliest transaction date in this batch |
| `active_days` | `int` | Count of distinct calendar dates with at least one transaction |
| `total_transactions_count` | `int` | Count of distinct transaction hashes |
| `total_gas_burned` | `float` | Total ETH spent on gas (sender-side only) |

---

#### `WalletBatchAggregate` (dataclass)

| Field | Type | Description |
|---|---|---|
| `chain_metrics` | `list[ChainBatchMetrics]` | One entry per chain seen for this wallet in the batch |
| `delta_active_days` | `int` | Distinct calendar dates across all chains for this wallet in the batch |

---

#### `UserBatchAggregate` (dataclass)

| Field | Type | Description |
|---|---|---|
| `wallets` | `dict[wallet_address → WalletBatchAggregate]` | Per-wallet aggregates for this user in the batch |
| `delta_active_days` | `int` | Distinct calendar dates across **all** wallets for this user (union — not sum) |

`UserBatchAggregate.delta_active_days` is computed directly in Polars (`group_by("__user").agg(n_unique("tx_date"))`) so the union of dates across wallets never has to be shipped to Python. This is what replaces the `frozenset[date]` union that `daily_flow` performs in pure Python.

---

#### `calculate_batch_metrics(parquet_dir, wallet_address)`

**Input:** `parquet_dir / "normal" / "*.parquet"` — all Parquet files for one wallet.

**Steps:**

1. **Lazy scan** — uses `pl.scan_parquet()` for memory efficiency.
2. **Column casting:**
   - `timeStamp` (string or int) → cast to `Int64` → converted to a calendar `date` via `pl.from_epoch(..., time_unit="s")`.
   - `gasUsed` and `gasPrice` → cast to `Int64`.
3. **Per-chain aggregation** (group by `__chain`):
   - `first_tx_date` = earliest `tx_date`.
   - `active_days` = count of distinct `tx_date` values.
   - `total_transactions_count` = count of distinct `hash` values.
4. **Gas calculation** (separate query):
   - Filters rows where `from == wallet_address` (only count gas for transactions **sent** by this wallet).
   - Sorts by `timeStamp` descending and deduplicates on `(__chain, hash)` keeping the first (latest) row — handles any duplicate hash entries.
   - Computes `gas_cost = gasUsed * gasPrice / 1e18` (converts wei to ETH).
   - Groups by `__chain` and sums `gas_cost`.
5. **Wallet-level active dates** — selects all `tx_date` values across all chains, converts to a `frozenset[date]`. The length of this set is `wallet_active_days`.
6. Returns `(chain_metrics_list, wallet_active_days, active_date_set)`.

**Why `frozenset`?** So the caller can safely do `set_A | set_B` to union date sets from multiple wallets without double-counting overlapping days.

---

#### `calculate_user_batch_metrics(tmp_root, user_ids) → dict[str, UserBatchAggregate]`

The batched, multi-user equivalent of `calculate_batch_metrics`. Used by `daily_all_flow`. Reads every parquet under `tmp_root/<user_id>/<wallet>/normal/` for the given `user_ids` in a single Polars scan.

**Input:** A list of `user_ids` and the shared `tmp_root`. The function expands these into one glob pattern per user (`tmp_root/<uid>/*/normal/*.parquet`) and passes the list to `pl.scan_parquet`.

**Steps:**

1. **Lazy scan with file-path tracking** — `pl.scan_parquet(patterns, include_file_paths="__path", extra_columns="ignore", missing_columns="insert")`. The schema-tolerance flags handle real-world parquet drift (e.g. some files include `gasPriceBid`, others don't).
2. **Identity columns:**
   - `__user` — extracted from `__path` via a regex (`([^/]+)/[^/]+/normal/[^/]+\.parquet$`).
   - `__walletaddress` — already a column in the source parquet; no extraction needed.
3. **Column casting:** same as the per-wallet function (`tx_date`, `gasUsed`, `gasPrice`).
4. **Four streaming aggregations** (each collected via `.collect(engine="streaming")`):
   - `(user, wallet, chain)` → `first_tx_date`, `n_unique(tx_date)` (active_days), `n_unique(hash)` (tx count).
   - Filter `from == __walletaddress`, dedup on `(user, wallet, chain, hash)` by latest `timeStamp`, then `(user, wallet, chain)` → `sum(gas_cost)`.
   - `(user, wallet)` → `n_unique(tx_date)` — wallet-level delta active days.
   - `(user)` → `n_unique(tx_date)` — user-level delta active days (the union across wallets, computed directly in Polars).
5. **Assembly in Python** — joins the four small aggregate tables into the nested `{user_id: UserBatchAggregate{wallets: {wallet: WalletBatchAggregate}}}` shape.

**Why streaming engine?** It spills intermediate state to disk rather than holding everything in RAM, so the function scales to batches larger than memory. Aggregate outputs are small (one row per group), so the final result is always cheap to materialise.

**Schema drift tolerance:** Real Etherscan exports occasionally add or omit columns across batches. The `extra_columns="ignore"` and `missing_columns="insert"` flags make the scan resilient without requiring upstream schema repair.

Implements three-level hierarchical merging: chain → wallet → user. Every merge function handles both the **new record** case (no existing document) and the **update** case (document already exists).

---

#### `merge_chain(existing, new: ChainBatchMetrics) → dict`

| Field | New record | Update |
|---|---|---|
| `chain` | `new.chain` | `new.chain` |
| `_first_tx_date` | `new.first_tx_date` | `min(existing, new)` |
| `wallet_age_days` | `(today - first_tx_date).days` | `(today - first_tx_date).days` |
| `active_days` | `new.active_days` | `existing + new` |
| `total_transactions_count` | `new.total_transactions_count` | `existing + new` |
| `total_gas_burned` | `new.total_gas_burned` | `existing + new` (rounded to 6 dp) |

`wallet_age_days` is always **recomputed from today** so it is always current.

---

#### `merge_wallet(existing_wallet, wallet_address, merged_chains, delta_active_days) → dict`

| Field | New record | Update |
|---|---|---|
| `wallet_address` | passed in | passed in |
| `_first_tx_date` | `min(first_tx_date across all merged_chains)` | `min(existing, new)` |
| `wallet_age_days` | `(today - first_tx_date).days` | `(today - first_tx_date).days` |
| `active_days` | `delta_active_days` | `existing + delta` |
| `total_transactions_count` | `sum(tx_count across chains)` | `sum(tx_count across all chains)` |
| `chains` | `merged_chains` | `merged_chains` |

Note: `total_transactions_count` at the wallet level is always **re-summed** from the chain documents, not additively accumulated. This prevents double-counting when chain documents are rebuilt.

---

#### `merge_user(existing_doc, user_id, merged_wallets, delta_active_days) → dict`

| Field | New record | Update |
|---|---|---|
| `user_id` | passed in | passed in |
| `_first_tx_date` | `min(first_tx_date across all wallets)` | `min(existing, new)` |
| `wallet_age_days` | `(today - first_tx_date).days` | `(today - first_tx_date).days` |
| `active_days` | `delta_active_days` | `existing + delta` |
| `total_transactions_count` | `sum(tx_count across wallets)` | `sum(tx_count across all wallets)` |
| `wallets` | `merged_wallets` | `merged_wallets` |
| `last_updated_date` | UTC ISO timestamp | UTC ISO timestamp |

---

### `logger.py` — Logging

Sets up logging at **import time** (module-level code). Each process run (each `python main.py` invocation) creates a unique log file.

**Log file location:**
```
logs/
└── YYYY-MM-DD/
    └── run_HHMMSS_<8-hex-chars>.log
```

The 8-character hex suffix comes from `uuid.uuid4().hex[:8]`, ensuring no two runs in the same second collide.

**Output:** Logs go simultaneously to `stdout` and the log file using identical formatters.

**Format:**
```
2026-05-21 14:32:01  INFO      RUN START  id=143201_a3f2b1c0
```

**Public functions:**

| Function | When called | What it logs |
|---|---|---|
| `log_previous(existing_doc)` | Before processing | The full existing MongoDB document (or "first-time run" if none) |
| `log_delta(wallet, chain_batch_list, active_days, is_first_time)` | After `calculate_batch_metrics` | The raw metrics from the current batch |
| `log_final(user_doc)` | After document is assembled | The complete merged document (called before bulk write in `daily_all_flow`, after `replace_user_doc` in single-user flows) |

---

### `mongo.py` — Persistence

Manages the MongoDB connection and all database I/O.

**Collection:** `user_wise_metrics` in the database specified by `MONGODB_DB`.

**Connection:** Lazy — the `MongoClient` is created on first use and reused for all subsequent calls within the same process (module-level singleton `_client`).

**Functions:**

| Function | Used by | Operation |
|---|---|---|
| `fetch_user_doc(user_id)` | `first_time_flow`, `daily_flow` | `find_one({"user_id": ...}, {"_id": 0})` — single document lookup |
| `replace_user_doc(doc)` | `first_time_flow`, `daily_flow` | `replace_one({"user_id": ...}, doc, upsert=True)` — single upsert |
| `fetch_all_user_docs()` | `daily_all_flow` | `find({}, {"_id": 0})` — returns all documents as a `dict[user_id → doc]` in one query |
| `bulk_replace_user_docs(docs)` | `daily_all_flow` | `bulk_write([ReplaceOne(..., upsert=True), ...], ordered=False)` — all upserts in one round trip |
| `ensure_index()` | setup | Creates a unique index on `user_id` |

**Why full replacement?** The document is small and fully reconstructed each run. Using `replace_one` / `ReplaceOne` with `upsert=True` is simpler and less error-prone than partial `$set` updates, which would require tracking every field that changed.

**Why `ordered=False` on `bulk_write`?** It lets MongoDB execute the individual write operations in any order (and potentially in parallel on the server side), which is safe here because each operation targets a different `user_id`.

---

## MongoDB Document Schema

```json
{
  "user_id": "69d693b1ba9f20d582dae331",
  "wallet_age_days": 730,
  "active_days": 150,
  "total_transactions_count": 2500,
  "_first_tx_date": "2021-05-15",
  "last_updated_date": "2026-05-21T16:30:45.123456+00:00",
  "wallets": [
    {
      "wallet_address": "0xabc...def",
      "wallet_age_days": 730,
      "active_days": 120,
      "total_transactions_count": 1500,
      "_first_tx_date": "2021-05-15",
      "chains": [
        {
          "chain": "ethereum",
          "wallet_age_days": 730,
          "active_days": 80,
          "total_transactions_count": 900,
          "total_gas_burned": 12.345678,
          "_first_tx_date": "2021-05-15"
        },
        {
          "chain": "polygon",
          "wallet_age_days": 500,
          "active_days": 50,
          "total_transactions_count": 600,
          "total_gas_burned": 0.123456,
          "_first_tx_date": "2022-01-20"
        }
      ]
    }
  ]
}
```

**Field notes:**

- `_first_tx_date` — internal field (prefixed `_`) stored as ISO date string (`YYYY-MM-DD`). Used to compute `wallet_age_days` on every run so the age is always current relative to today.
- `wallet_age_days` — recomputed on every merge from `(today - _first_tx_date).days`. Not accumulated.
- `active_days` — **accumulated** by adding deltas from each run. At the user level this represents distinct calendar days across all wallets (union of date sets, not sum).
- `total_transactions_count` — at chain level: accumulated additively. At wallet/user level: re-summed from child documents.
- `total_gas_burned` — chain level only. Accumulated additively. Rounded to 6 decimal places.

---

## Input Parquet Files

**Directory layout:**
```
tmp/
└── <user_id>/
    └── <wallet_address>/
        └── normal/
            ├── combined_tx_batch_1.parquet
            ├── combined_tx_batch_2.parquet
            └── ...
```

**Required columns:**

| Column | Type | Description |
|---|---|---|
| `timeStamp` | int or string (castable to `Int64`) | Unix timestamp in seconds |
| `__chain` | string | Blockchain identifier, e.g. `"ethereum"`, `"polygon"` |
| `hash` | string | Transaction hash |
| `from` | string | Sender wallet address |
| `gasUsed` | int or string (castable to `Int64`) | Gas units consumed |
| `gasPrice` | int or string (castable to `Int64`) | Gas price in wei |

**Batching convention:**

- **First-time run:** All existing Parquet files in `normal/` represent the complete transaction history.
- **Daily run:** New Parquet files are added to `normal/` each day. The pipeline reads all files every time but the merger handles deduplication at the metric level — it simply adds deltas. The caller (whoever stages the files) is responsible for not putting old files back.

**Schema drift across files:** The `daily-all` flow scans many users' parquet files in a single Polars query, so columns that exist in some files but not others (e.g. `gasPriceBid`) would normally cause a `SchemaError`. The scan is configured with `extra_columns="ignore"` and `missing_columns="insert"` so the run silently tolerates this drift. The required columns listed above must always be present; everything else is best-effort.

---

## Active Days — How It Works

"Active days" means distinct calendar dates on which at least one transaction occurred.

**Chain level:** `n_unique("tx_date")` per chain — how many distinct days had a transaction on that chain.

**Wallet level (batch):** The union of all `tx_date` values across all chains in the batch — `frozenset[date]`. This correctly handles a wallet that was active on the same day across multiple chains.

**User level (daily flow):** The pipeline unions `active_date_set` across all wallets before computing the delta. This correctly handles two wallets that were both active on the same day.

**Accumulation:** Each run adds `len(active_date_set)` to the stored `active_days`. This means the stored value is the total number of distinct active days seen across **all runs combined**, not distinct days in the current batch alone.

---

## Gas Burned — How It Is Calculated

Gas cost in ETH = `(gasUsed × gasPrice) / 10^18`

The calculation applies only to transactions where `from == wallet_address` — i.e., only transactions **sent by** this wallet. Incoming transactions are excluded.

Before summing, rows are deduplicated on `(__chain, hash)` keeping the latest `timeStamp`. This guards against duplicate rows in the source data (which can happen if Parquet files overlap slightly).

Gas is accumulated across runs: each new batch's gas cost is added to the value already stored in MongoDB for that chain.

---

## Configuration

**`.env` file (required):**

```
MONGODB_URI=mongodb://user:pass@host:port/db?authSource=admin
MONGODB_DB=your_database_name
```

`MONGODB_URI` is required. `MONGODB_DB` defaults to `"nucleus"` if absent.

**`--tmp-root` flag:** Overrides the default `tmp/` directory for Parquet files. Useful in testing (e.g., `--tmp-root tmp_test`).

---

## Dependencies

| Package | Version | Purpose |
|---|---|---|
| `polars` | >=1.0.0 | Fast DataFrame engine for reading and aggregating Parquet files |
| `pymongo` | >=4.0.0 | MongoDB client |
| `python-dotenv` | >=1.0.0 | Loads `.env` file into environment variables |

Python 3.12+ is required (`pyproject.toml`).
