# Python Functions Reference

This document describes the reusable Python functions that can be imported and called from external applications.

---

## Setup

### Environment Configuration

Create a `.env` file with CockroachDB credentials:

```
COCKROACHDB_HOST=localhost
COCKROACHDB_PORT=26257
COCKROACHDB_DB=nucleus
COCKROACHDB_USER=root
COCKROACHDB_PASSWORD=your_password
```

### Installation

```bash
uv sync
```

---

## Database Functions

### Module: `metrics_pipeline.cockroachdb`

Functions for querying transaction data from CockroachDB.

#### `fetch_wallet_transactions(wallet_addresses=None)`

Fetch transactions from CockroachDB.

**Parameters:**
- `wallet_addresses` (list[str], optional): List of wallet addresses to fetch
  - If `None`: Fetches transactions for **all wallets**
  - If provided: Fetches transactions for only those wallets

**Returns:**
- `list[dict]`: List of transaction dictionaries with keys:
  - `hash`: Transaction hash
  - `from`: Sender wallet address
  - `timeStamp`: Unix timestamp (seconds)
  - `gasUsed`: Gas units consumed
  - `gasPrice`: Gas price in wei
  - `__chain`: Blockchain identifier
  - `__walletaddress`: Wallet address

**Example:**

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions

# Get transactions for specific wallets
transactions = fetch_wallet_transactions(
    wallet_addresses=[
        "0x02d650eea6458794b57492aca061fdbd26d97767",
        "0x9f56506dea67eb73f1f2887fbcceca223ee71a42"
    ]
)
print(f"Found {len(transactions)} transactions")

# Get transactions for all wallets
all_transactions = fetch_wallet_transactions()
print(f"Total transactions in DB: {len(all_transactions)}")
```

#### `close_connection()`

Close the CockroachDB connection.

**Example:**

```python
from metrics_pipeline.cockroachdb import close_connection

# ... use functions ...
close_connection()
```

---

## Metrics Calculation Functions

### Module: `metrics_pipeline.calculator_db`

Functions for calculating metrics from raw transaction data (any source: CockroachDB, parquet files, etc.)

#### Data Classes

**`ChainMetrics`**
```python
@dataclass
class ChainMetrics:
    chain: str                          # Blockchain name
    first_tx_date: date                 # Earliest transaction date
    active_days: int                    # Distinct calendar days with txs
    total_transactions_count: int       # Unique transaction count
    total_gas_burned: float             # Total ETH spent on gas
```

**`WalletMetrics`**
```python
@dataclass
class WalletMetrics:
    wallet_address: str                 # Wallet address
    first_tx_date: date                 # Earliest transaction date
    active_days: int                    # Distinct calendar days
    total_transactions_count: int       # Total transactions
    total_gas_burned: float             # Total ETH burned
    chains: list[ChainMetrics]          # Per-chain breakdown
```

**`UserMetrics`**
```python
@dataclass
class UserMetrics:
    user_id: Optional[str]              # User ID (if known)
    first_tx_date: date                 # Earliest transaction date
    active_days: int                    # Distinct calendar days
    total_transactions_count: int       # Total transactions
    total_gas_burned: float             # Total ETH burned
    wallets: list[WalletMetrics]        # Per-wallet breakdown
```

#### `calculate_metrics_from_transactions(transactions, sender_wallet=None)`

Calculate metrics from raw transaction data.

**Parameters:**
- `transactions` (list[dict]): List of transaction dictionaries
  - Must have keys: `hash`, `from`, `timeStamp`, `gasUsed`, `gasPrice`, `__chain`, `__walletaddress`
- `sender_wallet` (str, optional): Not currently used (reserved for future)

**Returns:**
- `dict[str, UserMetrics]`: Dictionary mapping wallet addresses to their metrics

**Example:**

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

# Get transactions for specific wallets
transactions = fetch_wallet_transactions(
    wallet_addresses=[
        "0x02d650eea6458794b57492aca061fdbd26d97767",
        "0x9f56506dea67eb73f1f2887fbcceca223ee71a42"
    ]
)

# Calculate metrics
metrics = calculate_metrics_from_transactions(transactions)

# Access metrics by wallet address
for wallet_addr, user_metrics in metrics.items():
    print(f"\nWallet: {wallet_addr}")
    print(f"  Active Days: {user_metrics.active_days}")
    print(f"  Total Transactions: {user_metrics.total_transactions_count}")
    print(f"  Total Gas Burned: {user_metrics.total_gas_burned} ETH")
    print(f"  First TX Date: {user_metrics.first_tx_date}")
    
    for wallet in user_metrics.wallets:
        print(f"  Wallet Metrics:")
        for chain in wallet.chains:
            print(f"    {chain.chain}: {chain.active_days} days, "
                  f"{chain.total_gas_burned} ETH burned")
```

---

## Complete Workflow Examples

### Example 1: Single Wallet Metrics

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

wallet = "0x02d650eea6458794b57492aca061fdbd26d97767"

# Fetch transactions for wallet
transactions = fetch_wallet_transactions(wallet_addresses=[wallet])

# Calculate metrics
metrics = calculate_metrics_from_transactions(transactions)

# Format response
result = {
    "wallet": wallet,
    "metrics": {
        "first_tx_date": str(metrics[wallet].first_tx_date),
        "active_days": metrics[wallet].active_days,
        "total_transactions": metrics[wallet].total_transactions_count,
        "total_gas_burned": metrics[wallet].total_gas_burned,
        "chains": [
            {
                "chain": c.chain,
                "first_tx_date": str(c.first_tx_date),
                "active_days": c.active_days,
                "total_transactions": c.total_transactions_count,
                "total_gas_burned": c.total_gas_burned,
            }
            for c in metrics[wallet].wallets[0].chains
        ]
    }
}

close_connection()
return result
```

### Example 2: Multiple Wallets Metrics

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

wallets = [
    "0x02d650eea6458794b57492aca061fdbd26d97767",
    "0x9f56506dea67eb73f1f2887fbcceca223ee71a42"
]

# Fetch transactions
transactions = fetch_wallet_transactions(wallet_addresses=wallets)

# Calculate metrics
metrics = calculate_metrics_from_transactions(transactions)

# Format response
result = {
    "wallets_requested": wallets,
    "wallets_found": len(metrics),
    "metrics": {}
}

for wallet_addr, user_metrics in metrics.items():
    result["metrics"][wallet_addr] = {
        "first_tx_date": str(user_metrics.first_tx_date),
        "active_days": user_metrics.active_days,
        "total_transactions": user_metrics.total_transactions_count,
        "total_gas_burned": user_metrics.total_gas_burned,
        "chains": [
            {
                "chain": c.chain,
                "active_days": c.active_days,
                "total_transactions": c.total_transactions_count,
                "total_gas_burned": c.total_gas_burned,
            }
            for c in user_metrics.wallets[0].chains
        ]
    }

close_connection()
return result
```

### Example 3: All Wallets Metrics

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

# Fetch transactions for all wallets (omit wallet_addresses parameter)
transactions = fetch_wallet_transactions()

# Calculate metrics
metrics = calculate_metrics_from_transactions(transactions)

# Format response
result = {
    "total_wallets": len(metrics),
    "metrics": {}
}

for wallet_addr, user_metrics in metrics.items():
    result["metrics"][wallet_addr] = {
        "active_days": user_metrics.active_days,
        "total_transactions": user_metrics.total_transactions_count,
        "total_gas_burned": user_metrics.total_gas_burned,
        "first_tx_date": str(user_metrics.first_tx_date),
    }

close_connection()
return result
```

### Example 4: Raw Transaction Data

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
import polars as pl

wallet = "0x02d650eea6458794b57492aca061fdbd26d97767"

# Fetch raw transactions
transactions = fetch_wallet_transactions(wallet_addresses=[wallet])

# Convert to Polars DataFrame (optional)
df = pl.DataFrame(transactions)

# Export as CSV
csv_data = df.write_csv()

close_connection()
return {
    "format": "csv",
    "rows": len(df),
    "data": csv_data
}
```

---

## Error Handling

### Connection Errors

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions

try:
    transactions = fetch_wallet_transactions()
except Exception as e:
    print(f"Database error: {e}")
    # Handle error appropriately
```

### No Data Found

```python
from metrics_pipeline.cockroachdb import fetch_wallet_transactions
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

wallets = ["0x0000000000000000000000000000000000000000"]
transactions = fetch_wallet_transactions(wallet_addresses=wallets)

if not transactions:
    return {
        "error": "No transaction data found",
        "wallets_queried": wallets
    }

metrics = calculate_metrics_from_transactions(transactions)
```

---

## Integration Guide

### FastAPI Example

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
        
        if not transactions:
            return {"error": "No data found", "wallets": wallets}
        
        metrics = calculate_metrics_from_transactions(transactions)
        
        # Format response
        result = {"metrics": {}}
        for wallet_addr, m in metrics.items():
            result["metrics"][wallet_addr] = {
                "active_days": m.active_days,
                "total_transactions": m.total_transactions_count,
                "total_gas_burned": m.total_gas_burned,
                "chains": [
                    {
                        "chain": c.chain,
                        "active_days": c.active_days,
                        "total_gas_burned": c.total_gas_burned,
                    }
                    for c in m.wallets[0].chains
                ]
            }
        
        return result
    finally:
        close_connection()
```

### Flask Example

```python
from flask import Flask, request, jsonify
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

app = Flask(__name__)

@app.route("/metrics", methods=["GET"])
def get_metrics():
    try:
        wallets = request.args.getlist("wallets") or None
        
        transactions = fetch_wallet_transactions(wallet_addresses=wallets)
        
        if not transactions:
            return jsonify({"error": "No data found"}), 404
        
        metrics = calculate_metrics_from_transactions(transactions)
        
        result = {"metrics": {}}
        for wallet_addr, m in metrics.items():
            result["metrics"][wallet_addr] = {
                "active_days": m.active_days,
                "total_transactions": m.total_transactions_count,
                "total_gas_burned": m.total_gas_burned,
            }
        
        return jsonify(result)
    finally:
        close_connection()
```

### Express.js / Node.js with Python Subprocess

```javascript
const { spawn } = require('child_process');

function getMetrics(wallets) {
    return new Promise((resolve, reject) => {
        const pythonScript = `
from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
from metrics_pipeline.calculator_db import calculate_metrics_from_transactions
import json

wallets = ${JSON.stringify(wallets)} if ${wallets ? 'True' : 'False'} else None
transactions = fetch_wallet_transactions(wallet_addresses=wallets)
metrics = calculate_metrics_from_transactions(transactions)

result = {}
for wallet_addr, m in metrics.items():
    result[wallet_addr] = {
        "active_days": m.active_days,
        "total_transactions": m.total_transactions_count,
        "total_gas_burned": m.total_gas_burned,
    }

close_connection()
print(json.dumps(result))
        `;
        
        const python = spawn('python', ['-c', pythonScript]);
        let output = '';
        
        python.stdout.on('data', (data) => {
            output += data.toString();
        });
        
        python.on('close', (code) => {
            if (code === 0) {
                resolve(JSON.parse(output));
            } else {
                reject(new Error('Python script failed'));
            }
        });
    });
}
```

---

## Performance Considerations

1. **Connection Pooling**: The connection persists between calls, so reuse when possible
2. **Batch Operations**: Fetch multiple wallets at once to reduce round trips
3. **Memory**: Large datasets (>10K wallets) may use significant memory - process in batches
4. **Close Connection**: Always call `close_connection()` when done to free resources

---

## Function Return Types Reference

| Function | Returns | Notes |
|----------|---------|-------|
| `fetch_wallet_transactions()` | `list[dict]` | List of transaction objects |
| `calculate_metrics_from_transactions()` | `dict[str, UserMetrics]` | Metrics by wallet address |
| `close_connection()` | `None` | Closes DB connection |

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'psycopg2'"

Install dependencies:
```bash
uv sync
```

### "COCKROACHDB_HOST not found"

Create `.env` file with credentials:
```
COCKROACHDB_HOST=localhost
COCKROACHDB_PORT=26257
COCKROACHDB_DB=nucleus
COCKROACHDB_USER=root
COCKROACHDB_PASSWORD=password
```

### "No transaction data found"

1. Verify wallet addresses are correct (include `0x` prefix)
2. Check if wallet has transactions in database
3. Try without wallet filter: `fetch_wallet_transactions()`

---

## See Also

- [README.md](README.md) - Project overview
- [PIPELINE_DOCS.md](PIPELINE_DOCS.md) - Parquet-based pipeline docs
- [TESTING.md](TESTING.md) - CLI test scenarios
