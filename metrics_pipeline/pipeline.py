from __future__ import annotations

from pathlib import Path
from typing import Any

from .calculator import calculate_batch_metrics
from .logger import log_delta, log_final, log_previous
from .merger import merge_chain, merge_wallet, merge_user
from .mongo import fetch_user_doc, replace_user_doc


def _build_chain_lookup(wallet_doc: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Return {chain_name: chain_dict} from an existing wallet sub-document."""
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
) -> tuple[dict[str, Any], int]:
    """Calculate + merge metrics for one wallet.

    Returns (merged_wallet_doc, delta_active_days_for_this_wallet).
    """
    parquet_dir = tmp_root / user_id / wallet_address

    chain_batch_list, wallet_delta_active_days = calculate_batch_metrics(
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

    # Preserve chains that exist in MongoDB but had no new data in this batch
    new_chain_names = {cb.chain for cb in chain_batch_list}
    for chain_name, chain_doc in chain_lookup.items():
        if chain_name not in new_chain_names:
            merged_chains.append(chain_doc)

    merged_wallet = merge_wallet(
        existing_wallet, wallet_address, merged_chains, wallet_delta_active_days
    )
    return merged_wallet, wallet_delta_active_days


# ── public entry points ───────────────────────────────────────────────────────

def first_time_flow(user_id: str, wallet_address: str, tmp_root: Path = Path("tmp")) -> None:
    """Full calculation for a wallet connecting for the first time.

    Fetches the existing user doc so other already-connected wallets on the
    same account are preserved in the upserted document.
    """
    existing_user_doc = fetch_user_doc(user_id)
    log_previous(existing_user_doc, wallet_address)

    merged_wallet, wallet_delta = _process_wallet(
        user_id, wallet_address, tmp_root, existing_user_doc, is_first_time=True
    )

    wallet_lookup = _build_wallet_lookup(existing_user_doc)
    all_merged_wallets = [merged_wallet]
    for addr, wallet_doc in wallet_lookup.items():
        if addr != wallet_address:
            all_merged_wallets.append(wallet_doc)

    user_doc = merge_user(
        existing_doc=existing_user_doc,
        user_id=user_id,
        merged_wallets=all_merged_wallets,
        delta_active_days=wallet_delta,
    )
    replace_user_doc(user_doc)
    log_final(user_doc)


def daily_flow(user_id: str, wallet_address: str, tmp_root: Path = Path("tmp")) -> None:
    """Incremental update: merge new parquet batch with existing MongoDB state."""
    existing_user_doc = fetch_user_doc(user_id)
    log_previous(existing_user_doc, wallet_address)

    merged_wallet, wallet_delta = _process_wallet(
        user_id, wallet_address, tmp_root, existing_user_doc, is_first_time=False
    )

    # Preserve wallets that exist in MongoDB but aren't being updated now
    wallet_lookup = _build_wallet_lookup(existing_user_doc)
    all_merged_wallets = [merged_wallet]
    for addr, wallet_doc in wallet_lookup.items():
        if addr != wallet_address:
            all_merged_wallets.append(wallet_doc)

    # User-level active_days delta: distinct dates across this wallet's new batch
    # (For multi-wallet daily runs the caller should compute cross-wallet delta;
    #  single-wallet daily runs can use wallet_delta directly.)
    user_delta = wallet_delta

    user_doc = merge_user(
        existing_doc=existing_user_doc,
        user_id=user_id,
        merged_wallets=all_merged_wallets,
        delta_active_days=user_delta,
    )
    replace_user_doc(user_doc)
    log_final(user_doc)
