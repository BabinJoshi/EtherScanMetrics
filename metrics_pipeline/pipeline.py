from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from .calculator import calculate_batch_metrics
from .logger import log_delta, log_final, log_previous
from .merger import merge_chain, merge_wallet, merge_user
from .mongo import fetch_user_doc, replace_user_doc


def _build_chain_lookup(wallet_doc: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if wallet_doc is None:
        return {}
    return {c["chain"]: c for c in wallet_doc.get("chains", [])}


def _build_wallet_lookup(user_doc: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if user_doc is None:
        return {}
    return {w["wallet_address"]: w for w in user_doc.get("wallets", [])}


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

    merged_wallet = merge_wallet(
        existing_wallet, wallet_address, merged_chains, wallet_delta_active_days
    )
    return merged_wallet, wallet_delta_active_days, active_date_set


def _assemble_and_save(
    existing_user_doc: dict[str, Any] | None,
    user_id: str,
    updated_wallets: list[dict[str, Any]],
    updated_addresses: set[str],
    user_delta: int,
) -> None:
    wallet_lookup = _build_wallet_lookup(existing_user_doc)
    all_merged_wallets = list(updated_wallets)
    for addr, wallet_doc in wallet_lookup.items():
        if addr not in updated_addresses:
            all_merged_wallets.append(wallet_doc)

    user_doc = merge_user(
        existing_doc=existing_user_doc,
        user_id=user_id,
        merged_wallets=all_merged_wallets,
        delta_active_days=user_delta,
    )
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
