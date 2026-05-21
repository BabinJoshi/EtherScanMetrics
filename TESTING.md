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
The commands below use the path `tmp_test/` as the staging root so the
original `tmp/user/` data is never modified.

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

## Staging helper (copy-paste once)

Run this Python snippet in your shell to define a `stage` helper for the
session, or just do the `cp` commands manually.

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

**Expected logger output:**
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
import json
from metrics_pipeline.mongo import fetch_user_doc
doc = fetch_user_doc('69d693b1ba9f20d582dae331')
print(json.dumps({k: v for k, v in doc.items() if k != 'wallets'}, indent=2))
for w in doc['wallets']:
    print('  wallet:', w['wallet_address'])
    for c in w['chains']:
        print(f\"    chain={c['chain']}  active_days={c['active_days']}  tx_count={c['total_transactions_count']}  gas={c['total_gas_burned']:.6f}\")
"
```

### Step 4: Stage Batch B and run daily_flow
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 4 5

uv run python main.py daily \
  69d693b1ba9f20d582dae331 \
  0x02d650eea6458794b57492aca061fdbd26d97767 \
  --tmp-root tmp_test
```

**Expected logger output:**
```
PREVIOUS RUN  (shows Batch A metrics from MongoDB)
DELTA BATCH   wallet=0x02d650...
    wallet-level active_days in this batch: <M>
      chain=ethereum  ...  (only new transactions)
      chain=polygon   ...
FINAL RESULT  (Batch A + Batch B merged)
  user=69d693...  active_days=<N+M>  tx_count=<combined>
```

### Step 5: Verify updated document
Re-run the verify command from Step 3 and confirm:
- `active_days` increased by the Batch B delta
- `total_transactions_count` is the sum of both batches
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

### Step 2: first_time_flow — Wallet A (Batch A)
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 1 2 3

uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x02d650eea6458794b57492aca061fdbd26d97767 \
  --tmp-root tmp_test
```
→ Document is created with 1 wallet.

### Step 3: first_time_flow — Wallet B (Batch A)
```bash
stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 1 2 3

uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x353479020cd3d3327af1589ad73d067c75f2dece \
  --tmp-root tmp_test
```
→ **Logger should show `PREVIOUS RUN` with Wallet A's data** (not "no existing document").
→ Document now has 2 wallets.

### Step 4: first_time_flow — Wallet C (Batch A)
```bash
stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 1 2 3

uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x9f56506dea67eb73f1f2887fbcceca223ee71a42 \
  --tmp-root tmp_test
```
→ Logger shows `PREVIOUS RUN` with Wallet A + B data.
→ Document now has 3 wallets.

### Step 5: Verify all 3 wallets are present
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

### Step 6: daily_flow for each wallet (Batch B)
```bash
# Wallet A
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 4 5
uv run python main.py daily 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 --tmp-root tmp_test

# Wallet B
stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 4 5
uv run python main.py daily 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece --tmp-root tmp_test

# Wallet C
stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 4 5
uv run python main.py daily 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 --tmp-root tmp_test
```

After each `daily` call, the PREVIOUS RUN log should always show **all 3 wallets** (the ones not being updated are preserved unchanged).

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
# Wallet A
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 1 2 3
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 --tmp-root tmp_test

# Wallet B
stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 1 2 3
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece --tmp-root tmp_test

# Wallet C
stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 1 2 3
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 --tmp-root tmp_test
```

### Step 3: first_time_flow — User 2, both wallets (Batch A)
```bash
# Wallet D
stage 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 1 2 3
uv run python main.py first-time 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 --tmp-root tmp_test

# Wallet E
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

### Step 5: daily_flow — User 1, all 3 wallets (Batch B)
```bash
stage 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 4 5
uv run python main.py daily 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767 --tmp-root tmp_test

stage 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece 4 5
uv run python main.py daily 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece --tmp-root tmp_test

stage 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 4 5
uv run python main.py daily 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42 --tmp-root tmp_test
```

### Step 6: daily_flow — User 2, both wallets (Batch B)
```bash
stage 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 4 5
uv run python main.py daily 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9 --tmp-root tmp_test

stage 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691 4 5
uv run python main.py daily 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691 --tmp-root tmp_test
```

### Step 7: Final verification — both users fully updated
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
import json
from metrics_pipeline.mongo import fetch_user_doc

for uid in ['69d693b1ba9f20d582dae331', '69e29d7ebb75c92bdac43fe1']:
    doc = fetch_user_doc(uid)
    print(f\"user={uid}\")
    print(f\"  wallets={len(doc['wallets'])}  active_days={doc['active_days']}  tx_count={doc['total_transactions_count']}  first_tx={doc['_first_tx_date']}\")
    for w in doc['wallets']:
        print(f\"  wallet={w['wallet_address']}\")
        for c in w['chains']:
            print(f\"    chain={c['chain']:<12} active_days={c['active_days']:<4} tx_count={c['total_transactions_count']:<5} gas={c['total_gas_burned']:.6f}\")
    print()
"
```

---

## Cleanup

Remove all test documents and the staging directory:

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

| Log section | first_time_flow | daily_flow |
|-------------|-----------------|------------|
| `PREVIOUS RUN` | "no existing document" for the very first wallet of a user; existing data for subsequent wallets | Always shows the current MongoDB state before merge |
| `FIRST-TIME BATCH` / `DELTA BATCH` | Per-chain metrics computed from Batch A only | Per-chain metrics computed from Batch B only |
| `FINAL RESULT` | Full merged document written to MongoDB | Full merged document with A+B values |

### Key assertions after each daily_flow

- `active_days` > Batch A value (additive)
- `total_transactions_count` = Batch A count + Batch B count
- `total_gas_burned` per chain increased
- `_first_tx_date` unchanged (always the earliest transaction seen)
- `wallet_age_days` recalculated as `(today - _first_tx_date).days`
- All wallets present in the document (none dropped)
