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


@dataclass
class WalletBatchAggregate:
    chain_metrics: list[ChainBatchMetrics]
    delta_active_days: int


@dataclass
class UserBatchAggregate:
    wallets: dict[str, WalletBatchAggregate]
    delta_active_days: int


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


def calculate_user_batch_metrics(
    tmp_root: Path, user_ids: list[str]
) -> dict[str, UserBatchAggregate]:
    """Scan every parquet for a batch of users in one Polars pass.

    Reads tmp_root/<user_id>/<wallet>/normal/*.parquet for the given user_ids
    and produces per-(user, wallet, chain) metrics plus wallet- and user-level
    distinct-date counts. The streaming engine handles datasets larger than
    memory by spilling intermediate state.

    user_id is extracted from the file path; wallet address comes from the
    parquet's __walletaddress column.
    """
    if not user_ids:
        return {}

    patterns = [str(tmp_root / uid / "*" / "normal" / "*.parquet") for uid in user_ids]

    lf = (
        pl.scan_parquet(
            patterns,
            include_file_paths="__path",
            extra_columns="ignore",
            missing_columns="insert",
        )
        .with_columns(
            pl.col("__path")
              .str.extract(r"([^/]+)/[^/]+/normal/[^/]+\.parquet$", 1)
              .alias("__user"),
            pl.from_epoch(pl.col("timeStamp").cast(pl.Int64), time_unit="s").dt.date().alias("tx_date"),
            pl.col("gasUsed").cast(pl.Int64),
            pl.col("gasPrice").cast(pl.Int64),
        )
    )

    chain_stats = (
        lf.group_by(["__user", "__walletaddress", "__chain"])
        .agg(
            pl.col("tx_date").min().alias("first_tx_date"),
            pl.col("tx_date").n_unique().alias("active_days"),
            pl.col("hash").n_unique().alias("total_transactions_count"),
        )
        .collect(engine="streaming")
    )

    gas_stats = (
        lf.filter(pl.col("from") == pl.col("__walletaddress"))
        .sort("timeStamp", descending=True)
        .unique(subset=["__user", "__walletaddress", "__chain", "hash"], keep="first")
        .with_columns((pl.col("gasUsed") * pl.col("gasPrice") / 1e18).alias("gas_cost"))
        .group_by(["__user", "__walletaddress", "__chain"])
        .agg(pl.col("gas_cost").sum().alias("total_gas_burned"))
        .collect(engine="streaming")
    )

    wallet_active = (
        lf.group_by(["__user", "__walletaddress"])
        .agg(pl.col("tx_date").n_unique().alias("delta_active_days"))
        .collect(engine="streaming")
    )

    user_active = (
        lf.group_by("__user")
        .agg(pl.col("tx_date").n_unique().alias("delta_active_days"))
        .collect(engine="streaming")
    )

    gas_lookup: dict[tuple[str, str, str], float] = {
        (r["__user"], r["__walletaddress"], r["__chain"]): r["total_gas_burned"]
        for r in gas_stats.iter_rows(named=True)
    }

    wallet_chains: dict[tuple[str, str], list[ChainBatchMetrics]] = {}
    for r in chain_stats.iter_rows(named=True):
        key = (r["__user"], r["__walletaddress"])
        wallet_chains.setdefault(key, []).append(
            ChainBatchMetrics(
                chain=r["__chain"],
                first_tx_date=r["first_tx_date"],
                active_days=r["active_days"],
                total_transactions_count=r["total_transactions_count"],
                total_gas_burned=round(
                    gas_lookup.get((r["__user"], r["__walletaddress"], r["__chain"]), 0.0),
                    6,
                ),
            )
        )

    wallet_active_lookup: dict[tuple[str, str], int] = {
        (r["__user"], r["__walletaddress"]): r["delta_active_days"]
        for r in wallet_active.iter_rows(named=True)
    }

    result: dict[str, UserBatchAggregate] = {}
    for (user_id, wallet_address), chains in wallet_chains.items():
        result.setdefault(user_id, UserBatchAggregate(wallets={}, delta_active_days=0))
        result[user_id].wallets[wallet_address] = WalletBatchAggregate(
            chain_metrics=chains,
            delta_active_days=wallet_active_lookup[(user_id, wallet_address)],
        )

    for r in user_active.iter_rows(named=True):
        if r["__user"] in result:
            result[r["__user"]].delta_active_days = r["delta_active_days"]

    return result
