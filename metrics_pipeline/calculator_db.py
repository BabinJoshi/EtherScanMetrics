"""Calculate metrics from transaction data (any source: CockroachDB, parquet, etc.)."""

from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Any
import polars as pl


def _to_python(value: Any) -> Any:
    """Convert Polars scalar to Python type."""
    if hasattr(value, 'item'):
        return value.item()
    return value


@dataclass
class ChainMetrics:
    """Metrics for a single chain."""
    chain: str
    first_tx_date: date
    active_days: int
    total_transactions_count: int
    total_gas_burned: float


@dataclass
class WalletMetrics:
    """Metrics for a single wallet."""
    wallet_address: str
    first_tx_date: date
    active_days: int
    total_transactions_count: int
    total_gas_burned: float
    chains: list[ChainMetrics]


@dataclass
class UserMetrics:
    """Metrics for a single user (all wallets)."""
    user_id: Optional[str]
    first_tx_date: date
    active_days: int
    total_transactions_count: int
    total_gas_burned: float
    wallets: list[WalletMetrics]


def calculate_metrics_from_transactions(
    transactions: list[dict],
    sender_wallet: Optional[str] = None,
) -> dict[str, UserMetrics]:
    """Calculate metrics from raw transaction data.

    Works with any transaction source (CockroachDB, parquet files, etc.)

    Args:
        transactions: List of transaction dictionaries with keys:
            hash, from, timeStamp, gasUsed, gasPrice, __chain, __walletaddress
        sender_wallet: If provided, only count gas for this wallet as sender

    Returns:
        Dict mapping wallet_address to UserMetrics
    """
    if not transactions:
        return {}

    # Convert to Polars DataFrame
    df = pl.DataFrame(transactions)

    # Parse timeStamp if it's a string/int
    df = df.with_columns(
        pl.col("timeStamp").cast(pl.Int64).pipe(
            lambda x: pl.from_epoch(x, time_unit="s").dt.date()
        ).alias("tx_date"),
        pl.col("gasUsed").cast(pl.Int64),
        pl.col("gasPrice").cast(pl.Int64),
    )

    # Calculate per-chain metrics
    chain_metrics_df = df.group_by(["__walletaddress", "__chain"]).agg(
        pl.col("tx_date").min().alias("first_tx_date"),
        pl.col("tx_date").n_unique().alias("active_days"),
        pl.col("hash").n_unique().alias("total_transactions_count"),
    )

    # Calculate gas burned (for transactions where from == wallet)
    gas_df = (
        df.filter(pl.col("from") == pl.col("__walletaddress"))
        .sort("timeStamp", descending=True)
        .unique(subset=["__chain", "hash"], keep="first")
        .with_columns(
            (pl.col("gasUsed") * pl.col("gasPrice") / 1e18).alias("gas_cost")
        )
        .group_by(["__walletaddress", "__chain"])
        .agg(pl.col("gas_cost").sum().alias("total_gas_burned"))
    )

    # Calculate wallet-level active days (across all chains)
    wallet_active_df = df.group_by("__walletaddress").agg(
        pl.col("tx_date").n_unique().alias("active_days"),
        pl.col("tx_date").min().alias("first_tx_date"),
    )

    # Calculate total transactions per wallet
    wallet_tx_df = df.group_by("__walletaddress").agg(
        pl.col("hash").n_unique().alias("total_transactions_count"),
    )

    # Build result structure
    result: dict[str, UserMetrics] = {}

    for row in chain_metrics_df.iter_rows(named=True):
        wallet = row["__walletaddress"]
        chain = row["__chain"]

        # Get gas for this wallet-chain combo
        gas_rows = gas_df.filter(
            (pl.col("__walletaddress") == wallet) & (pl.col("__chain") == chain)
        )
        gas_burned = 0.0
        if gas_rows.height > 0:
            gas_burned = round(float(_to_python(gas_rows[0]["total_gas_burned"])), 6)

        # Convert row values to Python types
        first_tx_date = _to_python(row["first_tx_date"])
        active_days = _to_python(row["active_days"])
        total_txs_count = _to_python(row["total_transactions_count"])

        chain_metric = ChainMetrics(
            chain=chain,
            first_tx_date=first_tx_date,
            active_days=active_days,
            total_transactions_count=total_txs_count,
            total_gas_burned=gas_burned,
        )

        if wallet not in result:
            # Get wallet-level metrics
            wallet_active = wallet_active_df.filter(
                pl.col("__walletaddress") == wallet
            )[0]
            wallet_tx = wallet_tx_df.filter(
                pl.col("__walletaddress") == wallet
            )[0]

            # Calculate total gas for wallet
            wallet_gas_rows = gas_df.filter(
                pl.col("__walletaddress") == wallet
            )
            total_gas = 0.0
            if wallet_gas_rows.height > 0:
                total_gas = round(float(_to_python(wallet_gas_rows["total_gas_burned"].sum())), 6)

            # Convert Polars row values to Python types
            first_tx = _to_python(wallet_active["first_tx_date"])
            active_days = _to_python(wallet_active["active_days"])
            total_txs = _to_python(wallet_tx["total_transactions_count"])

            result[wallet] = UserMetrics(
                user_id=None,
                first_tx_date=first_tx,
                active_days=active_days,
                total_transactions_count=total_txs,
                total_gas_burned=total_gas,
                wallets=[
                    WalletMetrics(
                        wallet_address=wallet,
                        first_tx_date=first_tx,
                        active_days=active_days,
                        total_transactions_count=total_txs,
                        total_gas_burned=total_gas,
                        chains=[chain_metric],
                    )
                ],
            )
        else:
            # Add chain to existing wallet
            result[wallet].wallets[0].chains.append(chain_metric)

    return result
