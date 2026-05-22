# Testing the Metrics Pipeline — CLI Guide

All commands are run from the project root (`EtherScanMetricsTest/`) using `uv run python`.

The test dataset lives under `tmp/user/` and is split into **Batch A** (files 1–3) and **Batch B** (files 4–5) to simulate a first-time ingestion followed by a daily incremental run.

| Batch | Files | Simulates |
|-------|-------|-----------|
| A | `combined_tx_batch_1,2,3` | First wallet connection — full history |
| B | `combined_tx_batch_4,5` | Daily run — new transactions only |

---

## Prerequisites

### 1. Confirm `.env` is set
```
MONGODB_URI=mongodb://<user>:<pass>@<host>:<port>/<db>?authSource=admin
MONGODB_DB=<database_name>
```

### 2. Install dependencies
```bash
uv sync
```

### 3. Understand the staging helper
Because the pipeline reads **all** files in `normal/`, you must copy only the
files for each batch into a staging directory before running each flow.
The commands below use `tmp_test/` as the staging root so the original
`tmp/user/` data is never modified.

```bash
# Define a staging function (bash)
stage() {
  USER_ID=$1; WALLET=$2; shift 2
  DEST="tmp_test/$USER_ID/$WALLET/normal"
  rm -rf "$DEST" && mkdir -p "$DEST"
  for BATCH in "$@"; do
    cp "tmp/user/$USER_ID/$WALLET/normal/combined_tx_batch_${BATCH}.parquet" "$DEST/"
  done
  echo "staged  $WALLET  batches=$*"
}
```

---

## CLI reference

```
# Triggered when a user connects a wallet (single wallet, first-time):
python main.py first-time <user_id> <wallet_address> [--tmp-root TMP]

# Daily job — processes all connected wallets in one pass by default:
python main.py daily <user_id> [--tmp-root TMP]

# Daily job — override to process only specific wallets:
python main.py daily <user_id> --wallets W1 W2 W3 [--tmp-root TMP]

# Daily job for every user with staged data (batched global Polars scan):
python main.py daily-all [--tmp-root TMP] [--batch-size N]
```

`daily-all` processes every user under `tmp_root/` in chunks of `--batch-size`
(default 1000). Each chunk is one Polars scan + one Mongo `bulk_write`. There
is no Python-level thread pool — Polars handles parallelism internally.

Logs are written to both stdout and a per-run file under `logs/YYYY-MM-DD/`.

---

## Data reference

```
Users
├── 69d693b1ba9f20d582dae331  (User 1)
│   ├── 0x02d650eea6458794b57492aca061fdbd26d97767  (Wallet A)
│   ├── 0x353479020cd3d3327af1589ad73d067c75f2dece  (Wallet B)
│   └── 0x9f56506dea67eb73f1f2887fbcceca223ee71a42  (Wallet C)
└── 69e29d7ebb75c92bdac43fe1  (User 2)
    ├── 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9  (Wallet D)
    └── 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691  (Wallet E)
```

---

## Scenario 1 — Single user, single wallet

### Step 1: Clean up any prior state
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import get_collection
r = get_collection().delete_one({'user_id': '69d693b1ba9f20d582dae331'})
print('deleted', r.deleted_count, 'document(s)')
"
rm -rf tmp_test/69d693b1ba9f20d582dae331
```

### Step 2: Stage Batch A and run first_time_flow
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 1 2 3

uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x02d650eea6458794b57492aca061fdbd26d97767 \
  --tmp-root tmp_test
```

**Expected log output:**
```
PREVIOUS RUN  no existing document found — this is a first-time run
FIRST-TIME BATCH  wallet=0x02d650...
    wallet-level active_days in this batch: <N>
      chain=ethereum  ...
      chain=polygon   ...
FINAL RESULT
  user=69d693...  wallet_age=...d  active_days=<N>  tx_count=<N>
    wallet=0x02d650...
      chain=ethereum  ...
      chain=polygon   ...
```

### Step 3: Verify MongoDB document
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import fetch_user_doc
doc = fetch_user_doc('69d693b1ba9f20d582dae331')
print('wallets:', len(doc['wallets']))
print('active_days:', doc['active_days'])
print('tx_count:', doc['total_transactions_count'])
for w in doc['wallets']:
    print('  wallet:', w['wallet_address'])
    for c in w['chains']:
        print(f\"    chain={c['chain']}  active_days={c['active_days']}  tx_count={c['total_transactions_count']}  gas={c['total_gas_burned']:.6f}\")
"
```

### Step 4: Stage Batch B and run daily_flow
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 4 5

uv run python main.py daily 69d693b1ba9f20d582dae331 --tmp-root tmp_test
```

`daily` reads all connected wallets from MongoDB automatically — no `--wallets` needed.

**Expected log output:**
```
PREVIOUS RUN  (shows Batch A metrics)
DELTA BATCH   wallet=0x02d650...
    wallet-level active_days in this batch: <M>
      chain=ethereum  ...
      chain=polygon   ...
FINAL RESULT  (Batch A + Batch B merged)
  user=69d693...  active_days=<N+M>  tx_count=<combined>
```

### Step 5: Verify updated document
Re-run the verify command from Step 3 and confirm:
- `active_days` increased by the Batch B delta
- `total_transactions_count` = Batch A + Batch B
- `total_gas_burned` increased per chain
- `_first_tx_date` unchanged

---

## Scenario 2 — Single user, multiple wallets

### Step 1: Clean up
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import get_collection
r = get_collection().delete_one({'user_id': '69d693b1ba9f20d582dae331'})
print('deleted', r.deleted_count, 'document(s)')
"
rm -rf tmp_test/69d693b1ba9f20d582dae331
```

### Step 2: first_time_flow — Wallet A
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 1 2 3

uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x02d650eea6458794b57492aca061fdbd26d97767 \
  --tmp-root tmp_test
```
→ Document created with 1 wallet.

### Step 3: first_time_flow — Wallet B
```bash
stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 1 2 3

uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x353479020cd3d3327af1589ad73d067c75f2dece \
  --tmp-root tmp_test
```
→ `PREVIOUS RUN` log shows Wallet A's data. Document now has 2 wallets.

### Step 4: first_time_flow — Wallet C
```bash
stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 1 2 3

uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x9f56506dea67eb73f1f2887fbcceca223ee71a42 \
  --tmp-root tmp_test
```
→ `PREVIOUS RUN` shows Wallets A + B. Document now has 3 wallets.

### Step 5: Verify all 3 wallets present
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import fetch_user_doc
doc = fetch_user_doc('69d693b1ba9f20d582dae331')
print('wallet count:', len(doc['wallets']))
for w in doc['wallets']:
    print(' ', w['wallet_address'], '— tx_count:', w['total_transactions_count'])
"
```
Expected: `wallet count: 3`

### Step 6: daily_flow — all 3 wallets in one pass (Batch B)
```bash
# Stage Batch B for all wallets first
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 4 5
stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 4 5
stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 4 5

# Single daily command processes all 3 wallets
uv run python main.py daily 69d693b1ba9f20d582dae331 --tmp-root tmp_test
```

**Expected log output:** one `PREVIOUS RUN` → three `DELTA BATCH` blocks → one `FINAL RESULT`.

---

## Scenario 3 — Multiple users, multiple wallets

### Step 1: Clean up both users
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import get_collection
col = get_collection()
for uid in ['69d693b1ba9f20d582dae331', '69e29d7ebb75c92bdac43fe1']:
    r = col.delete_one({'user_id': uid})
    print(f'deleted {r.deleted_count} doc for user={uid}')
"
rm -rf tmp_test/
```

### Step 2: first_time_flow — User 1, all 3 wallets (Batch A)
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 1 2 3
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 --tmp-root tmp_test

stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 1 2 3
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece --tmp-root tmp_test

stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 1 2 3
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 --tmp-root tmp_test
```

### Step 3: first_time_flow — User 2, both wallets (Batch A)
```bash
stage 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 1 2 3
uv run python main.py first-time 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 --tmp-root tmp_test

stage 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691 1 2 3
uv run python main.py first-time 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691 --tmp-root tmp_test
```

### Step 4: Verify both user documents
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import fetch_user_doc
for uid, expected in [('69d693b1ba9f20d582dae331', 3), ('69e29d7ebb75c92bdac43fe1', 2)]:
    doc = fetch_user_doc(uid)
    wc = len(doc['wallets'])
    status = 'OK' if wc == expected else 'MISMATCH'
    print(f'[{status}] user={uid}  wallets={wc}/{expected}  tx_count={doc[\"total_transactions_count\"]}')
"
```
Expected:
```
[OK] user=69d693b1ba9f20d582dae331  wallets=3/3  tx_count=<N>
[OK] user=69e29d7ebb75c92bdac43fe1  wallets=2/2  tx_count=<N>
```

### Step 5: daily_flow — User 1 (Batch B, all 3 wallets in one pass)
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 4 5
stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 4 5
stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 4 5

uv run python main.py daily 69d693b1ba9f20d582dae331 --tmp-root tmp_test
```

### Step 6: daily_flow — User 2 (Batch B, both wallets in one pass)
```bash
stage 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 4 5
stage 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691 4 5

uv run python main.py daily 69e29d7ebb75c92bdac43fe1 --tmp-root tmp_test
```

### Step 7: Final verification — both users fully updated
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import fetch_user_doc

for uid in ['69d693b1ba9f20d582dae331', '69e29d7ebb75c92bdac43fe1']:
    doc = fetch_user_doc(uid)
    print(f'user={uid}')
    print(f\"  wallets={len(doc['wallets'])}  active_days={doc['active_days']}  tx_count={doc['total_transactions_count']}  first_tx={doc['_first_tx_date']}\")
    for w in doc['wallets']:
        print(f\"  wallet={w['wallet_address']}\")
        for c in w['chains']:
            print(f\"    chain={c['chain']:<12} active_days={c['active_days']:<4} tx_count={c['total_transactions_count']:<5} gas={c['total_gas_burned']:.6f}\")
    print()
"
```

---

## Scenario 4 — `daily-all` (batched global scan)

`daily-all` discovers every user under `tmp_root/` and processes them in
chunks via a single `pl.scan_parquet` per chunk. This scenario exercises
the batched code path end-to-end and forces multiple batches via
`--batch-size 1` so you can observe the chunking behaviour.

The `stage` helper defined earlier already produces the exact layout
`daily-all` expects (`tmp_test/<user_id>/<wallet>/normal/`), so this
scenario reuses it.

### Step 1: Clean up both users and any prior staging
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import get_collection
col = get_collection()
for uid in ['69d693b1ba9f20d582dae331', '69e29d7ebb75c92bdac43fe1']:
    r = col.delete_one({'user_id': uid})
    print(f'deleted {r.deleted_count} doc for user={uid}')
"
rm -rf tmp_test/
```

### Step 2: Stage Batch A for every wallet of both users
```bash
# User 1 — 3 wallets
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 1 2 3
stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 1 2 3
stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 1 2 3

# User 2 — 2 wallets
stage 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 1 2 3
stage 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691 1 2 3
```

### Step 3: Run `daily-all` (single batch — default batch_size=1000)
```bash
uv run python main.py daily-all --tmp-root tmp_test
```

**Expected log output (key lines):**
```
daily_all_flow: found 2 users  batch_size=1000
daily_all_flow: batch [0..1]  users=2  scanning…
DELTA BATCH  wallet=0x...   (5 blocks, one per wallet)
FINAL RESULT (2 blocks, one per user)
daily_all_flow: batch [0..1] wrote 2 documents
daily_all_flow: complete  wrote=2  failed_batches=0
```

Note that even on a first-ever run, the `daily-all` log header reads
`DELTA BATCH`, not `FIRST-TIME BATCH`. The `is_first_time` flag is hard-coded
to `False` in this flow — first-time semantics are handled inside the
merger by detecting an absent existing document, not by the log label.

### Step 4: Verify both user documents
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import fetch_user_doc
for uid, expected in [('69d693b1ba9f20d582dae331', 3), ('69e29d7ebb75c92bdac43fe1', 2)]:
    doc = fetch_user_doc(uid)
    wc = len(doc['wallets'])
    status = 'OK' if wc == expected else 'MISMATCH'
    print(f'[{status}] user={uid}  wallets={wc}/{expected}  active_days={doc[\"active_days\"]}  tx_count={doc[\"total_transactions_count\"]}')
"
```
Expected: both users `OK` with the correct wallet counts and non-zero
`active_days` / `tx_count`.

### Step 5: Stage Batch B for every wallet (incremental daily run)
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 4 5
stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 4 5
stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 4 5
stage 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 4 5
stage 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691 4 5
```

### Step 6: Re-run `daily-all` with `--batch-size 1` to force chunking
This forces one batch per user so you can confirm the per-batch scan and
per-batch `bulk_write` actually happen separately.

```bash
uv run python main.py daily-all --tmp-root tmp_test --batch-size 1
```

**Expected log output:**
```
daily_all_flow: found 2 users  batch_size=1
daily_all_flow: batch [0..0]  users=1  scanning…
DELTA BATCH ... (wallets for user 1)
FINAL RESULT  user=69d693...
daily_all_flow: batch [0..0] wrote 1 documents
daily_all_flow: batch [1..1]  users=1  scanning…
DELTA BATCH ... (wallets for user 2)
FINAL RESULT  user=69e29d...
daily_all_flow: batch [1..1] wrote 1 documents
daily_all_flow: complete  wrote=2  failed_batches=0
```

Two `batch [..] scanning…` / `batch [..] wrote …` pairs confirm two
independent Polars scans + two independent Mongo `bulk_write`s ran.

### Step 7: Verify deltas accumulated correctly
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import fetch_user_doc
for uid in ['69d693b1ba9f20d582dae331', '69e29d7ebb75c92bdac43fe1']:
    doc = fetch_user_doc(uid)
    print(f'user={uid}  active_days={doc[\"active_days\"]}  tx_count={doc[\"total_transactions_count\"]}  first_tx={doc[\"_first_tx_date\"]}')
    for w in doc['wallets']:
        print(f\"  {w['wallet_address']}  active_days={w['active_days']}  tx_count={w['total_transactions_count']}\")
"
```

Confirm vs. Step 4 output:
- `total_transactions_count` increased (Batch A + Batch B).
- `active_days` increased (additive delta).
- `_first_tx_date` unchanged.
- Same wallet counts (3 and 2) — no wallets dropped or duplicated.

### Step 8: Test the empty-root path
With a staging directory that exists but contains no user subdirectories,
`daily-all` should exit cleanly.

```bash
rm -rf tmp_test/
mkdir tmp_test
uv run python main.py daily-all --tmp-root tmp_test
```

**Expected:** the run logs `daily_all_flow: no user directories found in tmp_test — nothing to do` (WARNING level) and exits without errors. No Mongo write happens.

Note: if `tmp_root` itself does not exist at all (no `mkdir`), `iterdir()` raises `FileNotFoundError` — the "nothing to do" branch is for an empty staging dir, not a missing one. Ensuring the directory exists before invocation is the responsibility of whatever cron job stages files into it.

---

## Cleanup

```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import get_collection
col = get_collection()
for uid in ['69d693b1ba9f20d582dae331', '69e29d7ebb75c92bdac43fe1']:
    r = col.delete_one({'user_id': uid})
    print(f'deleted {r.deleted_count} doc for user={uid}')
"
rm -rf tmp_test/
```

---

## What to look for in the logs

| Log section | `first_time_flow` | `daily_flow` | `daily_all_flow` |
|-------------|-------------------|--------------|------------------|
| `PREVIOUS RUN` | "no existing document" for very first wallet; shows existing data for subsequent wallets | Shows full current MongoDB state before merge | Not emitted (this flow skips `log_previous`) |
| `daily_all_flow: found N users  batch_size=…` | — | — | Once at start |
| `daily_all_flow: batch [start..end] scanning…` | — | — | Once per batch (before the Polars scan) |
| `FIRST-TIME BATCH` / `DELTA BATCH` | One block per wallet — metrics from Batch A only | One block per wallet — metrics from Batch B only | One `DELTA BATCH` per wallet (every wallet flagged `is_first_time=False`) |
| `FINAL RESULT` | Once after all wallets are processed | Once after all wallets are processed | Once per user inside each batch |
| `daily_all_flow: batch […] wrote N documents` | — | — | Once per batch (after `bulk_write`) |
| `daily_all_flow: complete  wrote=N  failed_batches=M` | — | — | Once at end |

### Key assertions after each daily_flow / daily_all_flow run

- `active_days` > previous value (additive)
- `total_transactions_count` = old count + new batch count
- `total_gas_burned` per chain increased
- `_first_tx_date` unchanged (always the earliest transaction seen)
- `wallet_age_days` recalculated as `(today - _first_tx_date).days`
- All wallets present in the document (none dropped)
- Logs written to both stdout and a per-run file under `logs/YYYY-MM-DD/run_<HHMMSS>_<id>.log`

### Additional assertions for `daily-all`

- Number of `batch […] wrote N` log lines = `ceil(total_users / batch_size)`
- Sum of `N` across all batch-wrote lines = `wrote=N` in the summary
- `failed_batches=0` for a healthy run; any non-zero value indicates one or more `pl.scan_parquet` calls errored (e.g. corrupt file, missing required column) and that batch was skipped while other batches continued
