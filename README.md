# EtherScan Metrics

Blockchain wallet metrics calculation system with support for both parquet-based pipelines and direct CockroachDB queries.

---

## 📚 Documentation

### Core Concepts
- [METRICS_IMPLEMENTATION.md](METRICS_IMPLEMENTATION.md) - Metrics calculation details
- [PIPELINE_DOCS.md](PIPELINE_DOCS.md) - Parquet pipeline architecture

### Usage Guides
- [FUNCTIONS.md](FUNCTIONS.md) - **Python functions for external use** ⭐
- [TESTING.md](TESTING.md) - CLI test scenarios

---

## 🎯 Two Operational Modes

### 1. Parquet Pipeline (CLI-based)

For processing staged parquet files with MongoDB persistence.

**Data Organization:**
```
tmp/
├── first_fetch/    # Batches 1-3 (initial history)
└── daily_fetch/    # Batches 4-5 (daily increments)
```

**Commands:**
```bash
# First-time wallet processing
python main.py first-time <user_id> <wallet_address>

# Daily incremental update
python main.py daily <user_id>

# Batch processing all users
python main.py daily-all --batch-size 1000
```

### 2. Direct CockroachDB (Function-based)

For querying transaction data directly from CockroachDB and calculating metrics on-demand.

**Python Functions:**
```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

# Fetch and calculate metrics
transactions = fetch_wallet_transactions(wallet_addresses=["0x123..."])
metrics = calculate_metrics_from_transactions(transactions)
```

See [FUNCTIONS.md](FUNCTIONS.md) for complete reference.

---

## 🔧 Setup

### Prerequisites

```bash
# Install dependencies
uv sync
```

### Environment Configuration

Create `.env` file:

```
# CockroachDB (for direct queries)
COCKROACHDB_HOST=localhost
COCKROACHDB_PORT=26257
COCKROACHDB_DB=nucleus
COCKROACHDB_USER=root
COCKROACHDB_PASSWORD=your_password

# MongoDB (for pipeline persistence)
MONGODB_URI=mongodb://...
MONGODB_DB=your_db
```

---

## 📦 Modules

### `metrics_pipeline.cockroachdb`
Database connection and transaction queries
- `fetch_wallet_transactions(wallet_addresses=None)` - Query transactions
- `close_connection()` - Clean up database connection

### `metrics_pipeline.calculator_db`
Metrics calculation from raw transaction data
- `calculate_metrics_from_transactions(transactions)` - Compute metrics
- Data classes: `ChainMetrics`, `WalletMetrics`, `UserMetrics`

### `metrics_pipeline.pipeline` (Parquet-based)
High-level pipeline orchestration for file-based processing
- `first_time_flow()` - Process new wallet
- `daily_flow()` - Update existing user
- `daily_all_flow()` - Batch process all users

---

## 🚀 Quick Start

### Option A: Call Functions from Your App

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

# Get metrics for wallets
wallets = ["0x02d650eea6458794b57492aca061fdbd26d97767"]
transactions = fetch_wallet_transactions(wallet_addresses=wallets)
metrics = calculate_metrics_from_transactions(transactions)

# Access results
for wallet_addr, m in metrics.items():
    print(f"Active Days: {m.active_days}")
    print(f"Total Gas Burned: {m.total_gas_burned} ETH")

close_connection()
```

### Option B: Run CLI Commands

```bash
# Process new wallet
python main.py first-time 69d693b1ba9f20d582dae331 0x02d650eea6458794b57492aca061fdbd26d97767

# Update user's metrics
python main.py daily 69d693b1ba9f20d582dae331

# Batch process all users
python main.py daily-all
```

---

## 📖 Integration Examples

### FastAPI

```python
from fastapi import FastAPI, Query
from typing import Optional
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

app = FastAPI()

@app.get("/metrics")
async def get_metrics(wallets: Optional[list[str]] = Query(None)):
    try:
        transactions = fetch_wallet_transactions(wallet_addresses=wallets)
        metrics = calculate_metrics_from_transactions(transactions)
        return {"metrics": metrics}
    finally:
        close_connection()
```

### Flask

```python
from flask import Flask, request, jsonify
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

app = Flask(__name__)

@app.route("/metrics")
def get_metrics():
    wallets = request.args.getlist("wallets") or None
    try:
        transactions = fetch_wallet_transactions(wallet_addresses=wallets)
        metrics = calculate_metrics_from_transactions(transactions)
        return jsonify({"metrics": metrics})
    finally:
        close_connection()
```

See [FUNCTIONS.md](FUNCTIONS.md) for more examples and complete API reference.
