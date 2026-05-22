# Testing the Metrics Pipeline

This guide covers testing for both the parquet-based CLI pipeline and the CockroachDB function-based approach.

**Two Testing Modes:**
1. **CLI/Parquet Pipeline** - Using `python main.py` with staged parquet files and MongoDB
2. **CockroachDB Functions** - Using Python functions to query CockroachDB directly

---

## Parquet Pipeline Tests (CLI)

All CLI commands are run from the project root (`EtherScanMetrics/`) using `uv run python`.

## Data Organization

The test dataset is pre-organized into two separate folders:

- **`tmp/first_fetch/`** — Contains batches 1–3 (initial historical data)
  - Used by `first-time` command
  - Simulates first wallet connection — full transaction history
  
- **`tmp/daily_fetch/`** — Contains batches 4–5 (incremental daily data)
  - Used by `daily` and `daily-all` commands
  - Simulates new transactions discovered on daily runs

Both folders contain the same user/wallet structure:

```
tmp/first_fetch/                           tmp/daily_fetch/
├── 69d693b1ba9f20d582dae331/             ├── 69d693b1ba9f20d582dae331/
│   ├── 0x02d650eea6458794b57492aca.../   │   ├── 0x02d650eea6458794b57492aca.../
│   │   └── normal/                       │   │   └── normal/
│   │       ├── batch_1.parquet          │   │       ├── batch_4.parquet
│   │       ├── batch_2.parquet          │   │       └── batch_5.parquet
│   │       └── batch_3.parquet          │   │
│   └── ... (more wallets)               │   └── ... (more wallets)
└── 69e29d7ebb75c92bdac43fe1/             └── 69e29d7ebb75c92bdac43fe1/
    └── ... (more wallets)                   └── ... (more wallets)
```

| Folder | Files | Used By | Simulates |
|--------|-------|---------|-----------|
| `first_fetch` | batches 1–3 | `first-time` command | Initial wallet connection — full history |
| `daily_fetch` | batches 4–5 | `daily` & `daily-all` commands | Daily incremental updates |

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

### 3. Data is pre-organized
No staging required! Data is already separated:
- First-time data in `tmp/first_fetch/` (batches 1–3)
- Daily data in `tmp/daily_fetch/` (batches 4–5)

---

## CLI Reference

### First-Time Flow
Processes initial wallet data from `tmp/first_fetch/` (batches 1–3):
```bash
python main.py first-time <user_id> <wallet_address> [--tmp-root tmp]
```

### Daily Flow (Single User)
Processes incremental updates from `tmp/daily_fetch/` (batches 4–5):
```bash
python main.py daily <user_id> [--tmp-root tmp]
python main.py daily <user_id> --wallets W1 W2 W3 [--tmp-root tmp]
```

### Daily Flow (All Users)
Batched processing of all users from `tmp/daily_fetch/` (batches 4–5):
```bash
python main.py daily-all [--tmp-root tmp] [--batch-size N]
```

**Key Notes:**
- `daily-all` discovers all users under `tmp_root/daily_fetch/` and processes them in chunks of `--batch-size` (default 1000)
- Each chunk = one Polars scan + one Mongo `bulk_write`
- No Python-level thread pool — Polars handles internal parallelism
- Logs written to both stdout and per-run file under `logs/YYYY-MM-DD/`

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

## Scenario 1 — Single User, Single Wallet

### Step 1: Clean up any prior state
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import get_collection
r = get_collection().delete_one({'user_id': '69d693b1ba9f20d582dae331'})
print('deleted', r.deleted_count, 'document(s)')
"
```

### Step 2: Run first-time flow (reads from `tmp/first_fetch/`)
```bash
uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x02d650eea6458794b57492aca061fdbd26d97767
```

**Reads from:** `tmp/first_fetch/69d693b1ba9f20d582dae331/0x02d650.../normal/` (batches 1–3)

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

**Expected output:** First wallet document with metrics from batches 1–3

### Step 4: Run daily flow (reads from `tmp/daily_fetch/`)
```bash
uv run python main.py daily 69d693b1ba9f20d582dae331
```

**Reads from:** `tmp/daily_fetch/69d693b1ba9f20d582dae331/0x02d650.../normal/` (batches 4–5)

`daily` reads all connected wallets from MongoDB automatically.

**Expected log output:**
```
PREVIOUS RUN  (shows batches 1–3 metrics from Step 3)
DELTA BATCH   wallet=0x02d650...
    wallet-level active_days in this batch: <M>
      chain=ethereum  ...
      chain=polygon   ...
FINAL RESULT  (batches 1–3 + batches 4–5 merged)
  user=69d693...  active_days=<N+M>  tx_count=<combined>
```

### Step 5: Verify updated document
Re-run the verify command from Step 3 and confirm:
- `active_days` increased by the batches 4–5 delta
- `total_transactions_count` = batches 1–3 + batches 4–5
- `total_gas_burned` increased per chain
- `_first_tx_date` unchanged

---

## Scenario 2 — Single User, Multiple Wallets

### Step 1: Clean up
```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import get_collection
r = get_collection().delete_one({'user_id': '69d693b1ba9f20d582dae331'})
print('deleted', r.deleted_count, 'document(s)')
"
```

### Step 2: first-time flow — Wallet A
```bash
uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x02d650eea6458794b57492aca061fdbd26d97767
```
→ Document created with 1 wallet. Reads batches 1–3 from `tmp/first_fetch/`.

### Step 3: first-time flow — Wallet B
```bash
uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x353479020cd3d3327af1589ad73d067c75f2dece
```
→ `PREVIOUS RUN` shows Wallet A's data. Document now has 2 wallets.

### Step 4: first-time flow — Wallet C
```bash
uv run python main.py first-time \
  69d693b1ba9f20d582dae331 \
  0x9f56506dea67eb73f1f2887fbcceca223ee71a42
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

### Step 6: daily flow — all 3 wallets in one pass
```bash
uv run python main.py daily 69d693b1ba9f20d582dae331
```

Processes all 3 wallets from `tmp/daily_fetch/` (batches 4–5) in one pass.

**Expected log output:** one `PREVIOUS RUN` → three `DELTA BATCH` blocks → one `FINAL RESULT`.

---

## Scenario 3 — Multiple Users, Multiple Wallets

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
```

### Step 2: first-time flow — User 1, all 3 wallets
```bash
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42
```

All read from `tmp/first_fetch/` (batches 1–3)

### Step 3: first-time flow — User 2, both wallets
```bash
uv run python main.py first-time 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9
uv run python main.py first-time 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691
```

All read from `tmp/first_fetch/` (batches 1–3)

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

### Step 5: daily flow — User 1 (all 3 wallets in one pass)
```bash
uv run python main.py daily 69d693b1ba9f20d582dae331
```

Reads from `tmp/daily_fetch/` (batches 4–5, all 3 wallets)

### Step 6: daily flow — User 2 (both wallets in one pass)
```bash
uv run python main.py daily 69e29d7ebb75c92bdac43fe1
```

Reads from `tmp/daily_fetch/` (batches 4–5, both wallets)

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

## Scenario 4 — `daily-all` (Batched Global Scan)

`daily-all` discovers every user under `tmp_root/daily_fetch/` and processes them in
chunks via a single `pl.scan_parquet` per chunk. This scenario exercises
the batched code path end-to-end.

The data is already organized in `tmp/daily_fetch/` with the correct layout,
so no staging is required.

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
```

### Step 2: First populate data with first-time flows
Run the first-time flows from Scenario 3 above to set up initial data from `tmp/first_fetch/`:
```bash
# User 1 — 3 wallets
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x353479020cd3d3327af1589ad73d067c75f2dece
uv run python main.py first-time 69d693b1ba9f20d582dae331 0x9f56506dea67eb73f1f2887fbcceca223ee71a42

# User 2 — 2 wallets
uv run python main.py first-time 69e29d7ebb75c92bdac43fe1 0x0d53ab1ede05039f6b91b753ddca767cf9a2fad9
uv run python main.py first-time 69e29d7ebb75c92bdac43fe1 0x73d2a51ba95f1e05fb271b3f4140617c2bd9c691
```

### Step 3: Run `daily-all` (reads from `tmp/daily_fetch/`)
```bash
uv run python main.py daily-all
```

**Expected log output (key lines):**
```
daily_all_flow: found 2 users  batch_size=1000
daily_all_flow: batch [0..1]  users=2  scanning…
PREVIOUS RUN  (per user — shows batches 1–3 data from Step 2)
DELTA BATCH   (per wallet — 5 blocks total across both users, batches 4–5)
FINAL RESULT  (per user — 2 blocks total, batches 1–3 + 4–5 merged)
daily_all_flow: batch [0..1] wrote 2 documents
daily_all_flow: complete  wrote=2  failed_batches=0
```

For each user the order is **PREVIOUS RUN → DELTA BATCH (one per wallet) → FINAL RESULT**, so you can read top-to-bottom and verify that the final per-chain values equal previous + delta.

### Step 4: Verify both user documents (deltas accumulated)
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
Expected: both users `OK` with correct wallet counts and metrics from batches 1–3 + 4–5 combined.

### Step 5: Test with `--batch-size 1` to observe chunking
This forces one batch per user so you can confirm per-batch scans and writes:

```bash
uv run python main.py daily-all --batch-size 1
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

Two `batch [..] scanning…` / `batch [..] wrote …` pairs confirm independent Polars scans + Mongo `bulk_write`s.

---

## CockroachDB Function Tests

Testing the reusable Python functions for direct CockroachDB queries (without parquet pipeline).

### Prerequisites for CockroachDB Tests

```bash
# 1. Ensure .env has CockroachDB credentials
COCKROACHDB_HOST=localhost
COCKROACHDB_PORT=26257
COCKROACHDB_DB=nucleus
COCKROACHDB_USER=root
COCKROACHDB_PASSWORD=your_password

# 2. Verify CockroachDB is running with transaction data
# The nucleus.app.nucleus_users table must contain transaction data

# 3. Dependencies already installed
uv sync
```

### Test 1: Fetch Transactions for Specific Wallet

**Scenario:** Query a single wallet from CockroachDB

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection

wallet = '0x02d650eea6458794b57492aca061fdbd26d97767'

# Fetch transactions for specific wallet
transactions = fetch_wallet_transactions(wallet_addresses=[wallet])

print(f'Wallet: {wallet}')
print(f'Transactions found: {len(transactions)}')

if transactions:
    print(f'First transaction: {transactions[0]}')
    print(f'Columns: {list(transactions[0].keys())}')

close_connection()
"
```

**Expected Output:**
```
Wallet: 0x02d650eea6458794b57492aca061fdbd26d97767
Transactions found: 2500
First transaction: {'hash': '0xabc...', 'from': '0x02d650...', ...}
Columns: ['hash', 'from', 'timeStamp', 'gasUsed', 'gasPrice', '__chain', '__walletaddress']
```

### Test 2: Fetch Transactions for Multiple Wallets

**Scenario:** Query multiple wallets at once

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection

wallets = [
    '0x02d650eea6458794b57492aca061fdbd26d97767',
    '0x9f56506dea67eb73f1f2887fbcceca223ee71a42'
]

# Fetch transactions for multiple wallets
transactions = fetch_wallet_transactions(wallet_addresses=wallets)

print(f'Wallets queried: {len(wallets)}')
print(f'Total transactions: {len(transactions)}')

# Count by wallet
from collections import Counter
wallet_counts = Counter(t['__walletaddress'] for t in transactions)
for wallet, count in wallet_counts.items():
    print(f'  {wallet}: {count} transactions')

close_connection()
"
```

**Expected Output:**
```
Wallets queried: 2
Total transactions: 5000
  0x02d650eea6458794b57492aca061fdbd26d97767: 2500 transactions
  0x9f56506dea67eb73f1f2887fbcceca223ee71a42: 2500 transactions
```

### Test 3: Fetch All Wallets

**Scenario:** Query transactions for ALL wallets in database

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection

# Fetch without wallet_addresses = ALL wallets
transactions = fetch_wallet_transactions()

print(f'Total transactions (all wallets): {len(transactions)}')

# Get unique wallets
wallets = set(t['__walletaddress'] for t in transactions)
print(f'Unique wallets: {len(wallets)}')

# Get unique chains
chains = set(t['__chain'] for t in transactions)
print(f'Chains represented: {sorted(chains)}')

close_connection()
"
```

**Expected Output:**
```
Total transactions (all wallets): 12500
Unique wallets: 5
Chains represented: ['ethereum', 'polygon']
```

### Test 4: Calculate Metrics for Single Wallet

**Scenario:** Fetch and calculate metrics for one wallet

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

wallet = '0x02d650eea6458794b57492aca061fdbd26d97767'

# Fetch
transactions = fetch_wallet_transactions(wallet_addresses=[wallet])

# Calculate
metrics = calculate_metrics_from_transactions(transactions)

# Display
for wallet_addr, m in metrics.items():
    print(f'Wallet: {wallet_addr}')
    print(f'  Active Days: {m.active_days}')
    print(f'  Total Transactions: {m.total_transactions_count}')
    print(f'  Total Gas Burned: {m.total_gas_burned} ETH')
    print(f'  First TX Date: {m.first_tx_date}')
    print(f'  Chains:')
    for chain in m.wallets[0].chains:
        print(f'    {chain.chain}: {chain.active_days} days, {chain.total_gas_burned} ETH')

close_connection()
"
```

**Expected Output:**
```
Wallet: 0x02d650eea6458794b57492aca061fdbd26d97767
  Active Days: 450
  Total Transactions: 2500
  Total Gas Burned: 12.345678 ETH
  First TX Date: 2021-05-15
  Chains:
    ethereum: 350 days, 10.123456 ETH
    polygon: 300 days, 2.222222 ETH
```

### Test 5: Calculate Metrics for Multiple Wallets

**Scenario:** Fetch and calculate metrics for multiple wallets in one call

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

wallets = [
    '0x02d650eea6458794b57492aca061fdbd26d97767',
    '0x9f56506dea67eb73f1f2887fbcceca223ee71a42',
    '0x353479020cd3d3327af1589ad73d067c75f2dece'
]

# Fetch
transactions = fetch_wallet_transactions(wallet_addresses=wallets)

# Calculate
metrics = calculate_metrics_from_transactions(transactions)

# Display summary
print(f'Wallets processed: {len(metrics)}')
print(f'Total transactions: {len(transactions)}')
print()

for wallet_addr, m in metrics.items():
    print(f'{wallet_addr}:')
    print(f'  Active Days: {m.active_days}, Transactions: {m.total_transactions_count}')

close_connection()
"
```

**Expected Output:**
```
Wallets processed: 3
Total transactions: 7500

0x02d650eea6458794b57492aca061fdbd26d97767:
  Active Days: 450, Transactions: 2500
0x9f56506dea67eb73f1f2887fbcceca223ee71a42:
  Active Days: 420, Transactions: 2500
0x353479020cd3d3327af1589ad73d067c75f2dece:
  Active Days: 380, Transactions: 2500
```

### Test 6: Calculate Metrics for All Wallets

**Scenario:** Fetch and calculate metrics for ALL wallets

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

# Fetch all (no wallet_addresses parameter)
transactions = fetch_wallet_transactions()

# Calculate
metrics = calculate_metrics_from_transactions(transactions)

# Display
print(f'Total wallets: {len(metrics)}')
print()

total_active_days = 0
total_transactions = 0
total_gas = 0.0

for wallet_addr, m in metrics.items():
    print(f'{wallet_addr}:')
    print(f'  Active Days: {m.active_days}')
    print(f'  Transactions: {m.total_transactions_count}')
    print(f'  Gas Burned: {m.total_gas_burned} ETH')
    
    total_active_days += m.active_days
    total_transactions += m.total_transactions_count
    total_gas += m.total_gas_burned

print()
print(f'Totals:')
print(f'  Combined Active Days: {total_active_days}')
print(f'  Combined Transactions: {total_transactions}')
print(f'  Combined Gas Burned: {total_gas} ETH')

close_connection()
"
```

**Expected Output:**
```
Total wallets: 5

0x02d650eea6458794b57492aca061fdbd26d97767:
  Active Days: 450
  Transactions: 2500
  Gas Burned: 12.345678 ETH

... (3 more wallets) ...

Totals:
  Combined Active Days: 2100
  Combined Transactions: 12500
  Combined Gas Burned: 60.123456 ETH
```

### Test 7: Error Handling - No Data Found

**Scenario:** Query a non-existent wallet

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

wallet = '0x0000000000000000000000000000000000000000'

# Fetch
transactions = fetch_wallet_transactions(wallet_addresses=[wallet])

print(f'Transactions found: {len(transactions)}')

if not transactions:
    print('No data found - handling gracefully')
else:
    # Calculate
    metrics = calculate_metrics_from_transactions(transactions)
    print(f'Wallets with metrics: {len(metrics)}')

close_connection()
"
```

**Expected Output:**
```
Transactions found: 0
No data found - handling gracefully
```

### Test 8: Error Handling - Connection Error

**Scenario:** Test error handling when database is unavailable

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection

try:
    # This will fail if CockroachDB is not running
    transactions = fetch_wallet_transactions()
    print(f'Success: {len(transactions)} transactions')
except Exception as e:
    print(f'Error caught: {type(e).__name__}')
    print(f'Message: {str(e)}')
finally:
    close_connection()
"
```

**Expected Output (if DB running):**
```
Success: 12500 transactions
```

**Expected Output (if DB down):**
```
Error caught: OperationalError
Message: could not connect to server
```

### Test 9: Integration Test - Function Chain

**Scenario:** Complete workflow from fetch to formatted output

```python
python -c "
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions
import json

try:
    wallets = [
        '0x02d650eea6458794b57492aca061fdbd26d97767',
        '0x9f56506dea67eb73f1f2887fbcceca223ee71a42'
    ]
    
    # Step 1: Fetch
    transactions = fetch_wallet_transactions(wallet_addresses=wallets)
    print(f'✓ Fetched {len(transactions)} transactions')
    
    # Step 2: Calculate
    metrics = calculate_metrics_from_transactions(transactions)
    print(f'✓ Calculated metrics for {len(metrics)} wallets')
    
    # Step 3: Format
    result = {}
    for wallet_addr, m in metrics.items():
        result[wallet_addr] = {
            'active_days': m.active_days,
            'total_transactions': m.total_transactions_count,
            'total_gas_burned': m.total_gas_burned,
            'first_tx_date': str(m.first_tx_date),
        }
    
    print(f'✓ Formatted results')
    print(json.dumps(result, indent=2))
    
finally:
    close_connection()
    print('✓ Connection closed')
"
```

**Expected Output:**
```
✓ Fetched 5000 transactions
✓ Calculated metrics for 2 wallets
✓ Formatted results
{
  "0x02d650eea6458794b57492aca061fdbd26d97767": {
    "active_days": 450,
    "total_transactions": 2500,
    "total_gas_burned": 12.345678,
    "first_tx_date": "2021-05-15"
  },
  "0x9f56506dea67eb73f1f2887fbcceca223ee71a42": {
    "active_days": 420,
    "total_transactions": 2500,
    "total_gas_burned": 11.234567,
    "first_tx_date": "2021-06-10"
  }
}
✓ Connection closed
```

### Test 10: Performance Test - Batch Processing

**Scenario:** Process large number of wallets efficiently

```python
python -c "
import time
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

start = time.time()

# Fetch all wallets
transactions = fetch_wallet_transactions()
fetch_time = time.time()

# Calculate metrics
metrics = calculate_metrics_from_transactions(transactions)
calc_time = time.time()

print(f'Performance:')
print(f'  Fetch time: {fetch_time - start:.2f}s')
print(f'  Calculate time: {calc_time - fetch_time:.2f}s')
print(f'  Total time: {calc_time - start:.2f}s')
print(f'  Transactions processed: {len(transactions)}')
print(f'  Wallets processed: {len(metrics)}')
print(f'  Transactions/sec: {len(transactions) / (calc_time - start):.0f}')

close_connection()
"
```

**Expected Output:**
```
Performance:
  Fetch time: 0.34s
  Calculate time: 0.12s
  Total time: 0.46s
  Transactions processed: 12500
  Wallets processed: 5
  Transactions/sec: 27173
```

---

## Cleanup

After testing, clean up MongoDB documents:

```bash
uv run python -c "
from dotenv import load_dotenv; load_dotenv()
from metrics_pipeline.mongo import get_collection
col = get_collection()
for uid in ['69d693b1ba9f20d582dae331', '69e29d7ebb75c92bdac43fe1']:
    r = col.delete_one({'user_id': uid})
    print(f'deleted {r.deleted_count} doc for user={uid}')
"
```

(Note: `tmp/first_fetch/` and `tmp/daily_fetch/` data remains in place for future testing)

---

## What to look for in the logs

| Log section | `first_time_flow` | `daily_flow` | `daily_all_flow` |
|-------------|-------------------|--------------|------------------|
| `PREVIOUS RUN` | "no existing document" for very first wallet; shows existing data for subsequent wallets | Shows full current MongoDB state before merge | One block per user in each batch — shows that user's existing Mongo doc before merge (or "no existing document" for first-time users) |
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
