from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from .calculator import ChainBatchMetrics


def _date_from_iso(s: str) -> date:
    return date.fromisoformat(s)


def _wallet_age(first_tx_date: date) -> int:
    return (date.today() - first_tx_date).days


# ── chain level ─────────────────────────────────────────────────────────────

def merge_chain(existing: dict[str, Any] | None, new: ChainBatchMetrics) -> dict[str, Any]:
    if existing is None:
        first = new.first_tx_date
        return {
            "chain": new.chain,
            "wallet_age_days": _wallet_age(first),
            "active_days": new.active_days,
            "total_transactions_count": new.total_transactions_count,
            "total_gas_burned": new.total_gas_burned,
            "_first_tx_date": first.isoformat(),
        }

    existing_first = _date_from_iso(existing["_first_tx_date"])
    first = min(existing_first, new.first_tx_date)
    return {
        "chain": new.chain,
        "wallet_age_days": _wallet_age(first),
        "active_days": existing["active_days"] + new.active_days,
        "total_transactions_count": existing["total_transactions_count"] + new.total_transactions_count,
        "total_gas_burned": round(existing["total_gas_burned"] + new.total_gas_burned, 6),
        "_first_tx_date": first.isoformat(),
    }


# ── wallet level ─────────────────────────────────────────────────────────────

def merge_wallet(
    existing_wallet: dict[str, Any] | None,
    wallet_address: str,
    merged_chains: list[dict[str, Any]],
    delta_active_days: int,
) -> dict[str, Any]:
    first = min(_date_from_iso(c["_first_tx_date"]) for c in merged_chains)
    total_tx = sum(c["total_transactions_count"] for c in merged_chains)

    if existing_wallet is None:
        return {
            "wallet_address": wallet_address,
            "wallet_age_days": _wallet_age(first),
            "active_days": delta_active_days,
            "total_transactions_count": total_tx,
            "_first_tx_date": first.isoformat(),
            "chains": merged_chains,
        }

    existing_first = _date_from_iso(existing_wallet["_first_tx_date"])
    first = min(existing_first, first)
    return {
        "wallet_address": wallet_address,
        "wallet_age_days": _wallet_age(first),
        "active_days": existing_wallet["active_days"] + delta_active_days,
        "total_transactions_count": total_tx,
        "_first_tx_date": first.isoformat(),
        "chains": merged_chains,
    }


# ── user level ───────────────────────────────────────────────────────────────

def merge_user(
    existing_doc: dict[str, Any] | None,
    user_id: str,
    merged_wallets: list[dict[str, Any]],
    delta_active_days: int,
) -> dict[str, Any]:
    first = min(_date_from_iso(w["_first_tx_date"]) for w in merged_wallets)
    total_tx = sum(w["total_transactions_count"] for w in merged_wallets)
    now = datetime.now(timezone.utc).isoformat()

    if existing_doc is None:
        return {
            "user_id": user_id,
            "wallet_age_days": _wallet_age(first),
            "active_days": delta_active_days,
            "total_transactions_count": total_tx,
            "_first_tx_date": first.isoformat(),
            "wallets": merged_wallets,
            "last_updated_date": now,
        }

    existing_first = _date_from_iso(existing_doc["_first_tx_date"])
    first = min(existing_first, first)
    return {
        "user_id": user_id,
        "wallet_age_days": _wallet_age(first),
        "active_days": existing_doc["active_days"] + delta_active_days,
        "total_transactions_count": total_tx,
        "_first_tx_date": first.isoformat(),
        "wallets": merged_wallets,
        "last_updated_date": now,
    }
