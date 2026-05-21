from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl


@dataclass
class ChainBatchMetrics:
    chain: str
    first_tx_date: date
    active_days: int
    total_transactions_count: int
    total_gas_burned: float


def calculate_batch_metrics(
    parquet_dir: Path, wallet_address: str
) -> tuple[list[ChainBatchMetrics], int, int]:
    """Calculate per-chain metrics from parquet files in parquet_dir/normal/*.

    Returns:
        (chain_metrics, wallet_active_days, user_active_days)
        wallet_active_days = distinct dates across all chains in this batch
        user_active_days   = same as wallet for single-wallet call; caller
                             aggregates across wallets for user level
    """
    normal_dir = parquet_dir / "normal"
    pattern = str(normal_dir / "*.parquet")

    lf = (
        pl.scan_parquet(pattern)
        .with_columns(
            pl.from_epoch(pl.col("timeStamp").cast(pl.Int64), time_unit="s").dt.date().alias("tx_date"),
            pl.col("gasUsed").cast(pl.Int64),
            pl.col("gasPrice").cast(pl.Int64),
        )
    )

    # ── per-chain metrics ────────────────────────────────────────────────────

    # first_tx_date and active_days per chain
    chain_dates = (
        lf.group_by("__chain")
        .agg(
            pl.col("tx_date").min().alias("first_tx_date"),
            pl.col("tx_date").n_unique().alias("active_days"),
            pl.col("hash").n_unique().alias("total_transactions_count"),
        )
        .collect()
    )

    # gas_burned: filter from==wallet, dedup (__chain, hash) keeping latest ts
    gas_df = (
        lf.filter(pl.col("from") == wallet_address)
        .sort("timeStamp", descending=True)
        .unique(subset=["__chain", "hash"], keep="first")
        .with_columns(
            (pl.col("gasUsed") * pl.col("gasPrice") / 1e18).alias("gas_cost")
        )
        .group_by("__chain")
        .agg(pl.col("gas_cost").sum().alias("total_gas_burned"))
        .collect()
    )

    gas_lookup: dict[str, float] = {
        row["__chain"]: row["total_gas_burned"]
        for row in gas_df.iter_rows(named=True)
    }

    chain_metrics: list[ChainBatchMetrics] = []
    for row in chain_dates.iter_rows(named=True):
        chain_metrics.append(
            ChainBatchMetrics(
                chain=row["__chain"],
                first_tx_date=row["first_tx_date"],
                active_days=row["active_days"],
                total_transactions_count=row["total_transactions_count"],
                total_gas_burned=round(gas_lookup.get(row["__chain"], 0.0), 6),
            )
        )

    # ── wallet-level active days (distinct dates across all chains) ──────────
    wallet_active_days: int = (
        lf.select(pl.col("tx_date").n_unique()).collect().item()
    )

    return chain_metrics, wallet_active_days
