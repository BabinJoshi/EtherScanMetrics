from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from .calculator import calculate_batch_metrics, calculate_user_batch_metrics
from .logger import log_delta, log_final, log_previous
from .merger import merge_chain, merge_wallet, merge_user
from .mongo import bulk_replace_user_docs, fetch_all_user_docs, fetch_user_doc, replace_user_doc


def _build_chain_lookup(wallet_doc: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if wallet_doc is None:
        return {}
    return {c["chain"]: c for c in wallet_doc.get("chains", [])}


def _build_wallet_lookup(user_doc: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if user_doc is None:
        return {}
    return {w["wallet_address"]: w for w in user_doc.get("wallets", [])}


def _merge_one_wallet(
    existing_user_doc: dict[str, Any] | None,
    wallet_address: str,
    chain_batch_list: list,
    wallet_delta_active_days: int,
) -> dict[str, Any]:
    """Merge a wallet's new chain metrics with whatever is already in Mongo."""
    wallet_lookup = _build_wallet_lookup(existing_user_doc)
    existing_wallet = wallet_lookup.get(wallet_address)
    chain_lookup = _build_chain_lookup(existing_wallet)

    merged_chains = [
        merge_chain(chain_lookup.get(cb.chain), cb)
        for cb in chain_batch_list
    ]
    new_chain_names = {cb.chain for cb in chain_batch_list}
    for chain_name, chain_doc in chain_lookup.items():
        if chain_name not in new_chain_names:
            merged_chains.append(chain_doc)

    return merge_wallet(
        existing_wallet, wallet_address, merged_chains, wallet_delta_active_days
    )


def _process_wallet(
    user_id: str,
    wallet_address: str,
    tmp_root: Path,
    existing_user_doc: dict[str, Any] | None,
    is_first_time: bool,
) -> tuple[dict[str, Any], int, frozenset[date]]:
    """Calculate + merge metrics for one wallet.

    Returns (merged_wallet_doc, delta_active_days, active_date_set).
    """
    parquet_dir = tmp_root / user_id / wallet_address

    chain_batch_list, wallet_delta_active_days, active_date_set = calculate_batch_metrics(
        parquet_dir, wallet_address
    )

    log_delta(wallet_address, chain_batch_list, wallet_delta_active_days, is_first_time)

    merged_wallet = _merge_one_wallet(
        existing_user_doc, wallet_address, chain_batch_list, wallet_delta_active_days
    )
    return merged_wallet, wallet_delta_active_days, active_date_set


def _assemble_user_doc(
    existing_user_doc: dict[str, Any] | None,
    user_id: str,
    updated_wallets: list[dict[str, Any]],
    updated_addresses: set[str],
    user_delta: int,
) -> dict[str, Any]:
    wallet_lookup = _build_wallet_lookup(existing_user_doc)
    all_merged_wallets = list(updated_wallets)
    for addr, wallet_doc in wallet_lookup.items():
        if addr not in updated_addresses:
            all_merged_wallets.append(wallet_doc)

    return merge_user(
        existing_doc=existing_user_doc,
        user_id=user_id,
        merged_wallets=all_merged_wallets,
        delta_active_days=user_delta,
    )


def _assemble_and_save(
    existing_user_doc: dict[str, Any] | None,
    user_id: str,
    updated_wallets: list[dict[str, Any]],
    updated_addresses: set[str],
    user_delta: int,
) -> None:
    user_doc = _assemble_user_doc(existing_user_doc, user_id, updated_wallets, updated_addresses, user_delta)
    replace_user_doc(user_doc)
    log_final(user_doc)


# ── public entry points ───────────────────────────────────────────────────────

def first_time_flow(user_id: str, wallet_address: str, tmp_root: Path = Path("tmp")) -> None:
    """First-time calculation for a single wallet."""
    existing_user_doc = fetch_user_doc(user_id)
    log_previous(existing_user_doc)

    merged_wallet, _, active_date_set = _process_wallet(
        user_id, wallet_address, tmp_root, existing_user_doc, is_first_time=True
    )
    _assemble_and_save(
        existing_user_doc, user_id,
        [merged_wallet], {wallet_address},
        len(active_date_set),
    )


def daily_flow(
    user_id: str,
    wallet_addresses: list[str] | None = None,
    tmp_root: Path = Path("tmp"),
) -> None:
    """Daily incremental update for a user's wallets in one pass.

    If wallet_addresses is omitted, all wallets stored in the existing
    MongoDB document are processed. Fetches once, writes once.
    """
    existing_user_doc = fetch_user_doc(user_id)
    log_previous(existing_user_doc)

    wallets_to_run = wallet_addresses or list(_build_wallet_lookup(existing_user_doc).keys())
    if not wallets_to_run:
        from .logger import logger
        logger.warning("daily_flow: no wallets found for user=%s — nothing to do", user_id)
        return

    all_active_dates: set[date] = set()
    updated_wallets: list[dict[str, Any]] = []

    for wallet_address in wallets_to_run:
        merged_wallet, _, active_date_set = _process_wallet(
            user_id, wallet_address, tmp_root, existing_user_doc, is_first_time=False
        )
        updated_wallets.append(merged_wallet)
        all_active_dates |= active_date_set

    _assemble_and_save(
        existing_user_doc, user_id,
        updated_wallets, set(wallets_to_run),
        len(all_active_dates),
    )


def daily_all_flow(tmp_root: Path = Path("tmp"), batch_size: int = 1000) -> None:
    """Daily incremental update for every user via batched global Polars scans.

    Users are discovered from subdirectories of tmp_root and processed in
    chunks of batch_size. Each batch is a single scan_parquet over every
    parquet under the batch's user dirs, computed via Polars' streaming
    engine, then bulk_write'd to Mongo. A failed batch is logged and the
    remaining batches continue.

    Memory is bounded by batch_size, not total user count; Polars handles
    intra-batch parallelism, so no Python-level thread pool is needed.
    """
    from .logger import logger

    user_dirs = sorted(d for d in tmp_root.iterdir() if d.is_dir())
    if not user_dirs:
        logger.warning("daily_all_flow: no user directories found in %s — nothing to do", tmp_root)
        return

    logger.info("daily_all_flow: found %d users  batch_size=%d", len(user_dirs), batch_size)

    existing_docs = fetch_all_user_docs()

    total_written = 0
    failed_batches: list[tuple[int, int, str]] = []

    for start in range(0, len(user_dirs), batch_size):
        batch = user_dirs[start:start + batch_size]
        batch_user_ids = [d.name for d in batch]
        end = start + len(batch) - 1

        logger.info("daily_all_flow: batch [%d..%d]  users=%d  scanning…", start, end, len(batch))

        try:
            user_batches = calculate_user_batch_metrics(tmp_root, batch_user_ids)
        except Exception as exc:
            logger.error("daily_all_flow: batch [%d..%d] scan failed: %s — skipping", start, end, exc)
            failed_batches.append((start, end, str(exc)))
            continue

        batch_docs: list[dict[str, Any]] = []
        for user_id in batch_user_ids:
            user_batch = user_batches.get(user_id)
            if user_batch is None:
                logger.warning("daily_all_flow: user=%s has no parquet data — skipping", user_id)
                continue

            existing_user_doc = existing_docs.get(user_id)
            updated_wallets: list[dict[str, Any]] = []

            for wallet_address, wallet_batch in user_batch.wallets.items():
                log_delta(
                    wallet_address,
                    wallet_batch.chain_metrics,
                    wallet_batch.delta_active_days,
                    is_first_time=False,
                )
                updated_wallets.append(
                    _merge_one_wallet(
                        existing_user_doc,
                        wallet_address,
                        wallet_batch.chain_metrics,
                        wallet_batch.delta_active_days,
                    )
                )

            user_doc = _assemble_user_doc(
                existing_user_doc, user_id,
                updated_wallets, set(user_batch.wallets.keys()),
                user_batch.delta_active_days,
            )
            log_final(user_doc)
            batch_docs.append(user_doc)

        bulk_replace_user_docs(batch_docs)
        total_written += len(batch_docs)
        logger.info("daily_all_flow: batch [%d..%d] wrote %d documents", start, end, len(batch_docs))

    logger.info(
        "daily_all_flow: complete  wrote=%d  failed_batches=%d",
        total_written, len(failed_batches),
    )
    for start, end, exc in failed_batches:
        logger.warning("daily_all_flow: failed batch [%d..%d]: %s", start, end, exc)
