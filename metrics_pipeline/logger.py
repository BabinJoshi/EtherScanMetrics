from __future__ import annotations

import logging
import sys
from typing import Any

from .calculator import ChainBatchMetrics

# ── module-level logger setup ─────────────────────────────────────────────────

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(
    logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
)

logger = logging.getLogger("metrics_pipeline")
logger.setLevel(logging.INFO)
logger.addHandler(_handler)
logger.propagate = False


# ── formatting helpers ────────────────────────────────────────────────────────

def _chain_line(c: dict[str, Any] | ChainBatchMetrics, indent: str = "      ") -> str:
    if isinstance(c, ChainBatchMetrics):
        return (
            f"{indent}chain={c.chain:<12} "
            f"first_tx={c.first_tx_date}  "
            f"active_days={c.active_days:<5} "
            f"tx_count={c.total_transactions_count:<6} "
            f"gas_burned={c.total_gas_burned:.6f} ETH"
        )
    return (
        f"{indent}chain={c['chain']:<12} "
        f"first_tx={c['_first_tx_date']}  "
        f"wallet_age={c['wallet_age_days']}d  "
        f"active_days={c['active_days']:<5} "
        f"tx_count={c['total_transactions_count']:<6} "
        f"gas_burned={c['total_gas_burned']:.6f} ETH"
    )


def _wallet_line(w: dict[str, Any], indent: str = "    ") -> str:
    return (
        f"{indent}wallet={w['wallet_address']}  "
        f"first_tx={w['_first_tx_date']}  "
        f"wallet_age={w['wallet_age_days']}d  "
        f"active_days={w['active_days']:<5} "
        f"tx_count={w['total_transactions_count']:<6}"
    )


def _user_line(u: dict[str, Any], indent: str = "  ") -> str:
    return (
        f"{indent}user={u['user_id']}  "
        f"first_tx={u['_first_tx_date']}  "
        f"wallet_age={u['wallet_age_days']}d  "
        f"active_days={u['active_days']:<5} "
        f"tx_count={u['total_transactions_count']:<6}"
    )


# ── public log functions ──────────────────────────────────────────────────────

def log_previous(existing_doc: dict[str, Any] | None, wallet_address: str) -> None:
    if existing_doc is None:
        logger.info("PREVIOUS RUN  no existing document found — this is a first-time run")
        return

    logger.info("PREVIOUS RUN  ─────────────────────────────────────────────────────────")
    logger.info(_user_line(existing_doc))

    for w in existing_doc.get("wallets", []):
        logger.info(_wallet_line(w))
        for c in w.get("chains", []):
            logger.info(_chain_line(c))

    logger.info("────────────────────────────────────────────────────────────────────────")


def log_delta(
    wallet_address: str,
    chain_batch_list: list[ChainBatchMetrics],
    wallet_active_days: int,
    is_first_time: bool,
) -> None:
    label = "FIRST-TIME BATCH" if is_first_time else "DELTA BATCH"
    logger.info("%s  wallet=%s  ──────────────────────────────────────", label, wallet_address)
    logger.info("    wallet-level active_days in this batch: %d", wallet_active_days)
    for cm in chain_batch_list:
        logger.info(_chain_line(cm))
    logger.info("────────────────────────────────────────────────────────────────────────")


def log_final(user_doc: dict[str, Any]) -> None:
    logger.info("FINAL RESULT  ──────────────────────────────────────────────────────────")
    logger.info(_user_line(user_doc))

    for w in user_doc.get("wallets", []):
        logger.info(_wallet_line(w))
        for c in w.get("chains", []):
            logger.info(_chain_line(c))

    logger.info("────────────────────────────────────────────────────────────────────────")
