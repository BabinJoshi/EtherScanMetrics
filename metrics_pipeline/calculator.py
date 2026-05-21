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
) -> tuple[list[ChainBatchMetrics], int, frozenset[date]]:
    """Calculate per-chain metrics from parquet files in parquet_dir/normal/*.

    Returns:
        (chain_metrics, wallet_active_days, active_date_set)
        wallet_active_days = distinct dates across all chains in this batch
        active_date_set    = frozenset of those dates; caller unions across
                             wallets to compute the correct user-level delta
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
    active_date_set: frozenset[date] = frozenset(
        lf.select(pl.col("tx_date")).collect()["tx_date"].to_list()
    )
    wallet_active_days = len(active_date_set)

    return chain_metrics, wallet_active_days, active_date_set
