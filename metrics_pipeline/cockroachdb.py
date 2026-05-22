"""CockroachDB connection and query utilities."""

import os
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

_connection = None


def get_connection():
    """Get or create CockroachDB connection."""
    global _connection
    if _connection is None:
        _connection = psycopg2.connect(
            host=os.getenv("COCKROACHDB_HOST", "localhost"),
            port=int(os.getenv("COCKROACHDB_PORT", "26257")),
            database=os.getenv("COCKROACHDB_DB", "defaultdb"),
            user=os.getenv("COCKROACHDB_USER", "root"),
            password=os.getenv("COCKROACHDB_PASSWORD", ""),
        )
    return _connection


def fetch_wallet_transactions(
    wallet_addresses: Optional[list[str]] = None,
) -> list[dict]:
    """Fetch transactions from CockroachDB.

    Args:
        wallet_addresses: List of wallet addresses to fetch. If None, fetches all.

    Returns:
        List of transaction dictionaries.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    db_name = os.getenv("COCKROACHDB_DB", "nucleus")

    try:
        if wallet_addresses:
            placeholders = ",".join(["%s"] * len(wallet_addresses))
            query = f"""
                SELECT
                    hash,
                    from_address as "from",
                    time_stamp as "timeStamp",
                    gas_used as "gasUsed",
                    gas_price as "gasPrice",
                    chain as "__chain",
                    __wallet_address as "__walletaddress"
                FROM {db_name}.blockchain.normal_transactions
                WHERE __wallet_address IN ({placeholders})
                ORDER BY time_stamp ASC
            """
            cursor.execute(query, wallet_addresses)
        else:
            query = f"""
                SELECT
                    hash,
                    from_address as "from",
                    time_stamp as "timeStamp",
                    gas_used as "gasUsed",
                    gas_price as "gasPrice",
                    chain as "__chain",
                    __wallet_address as "__walletaddress"
                FROM {db_name}.blockchain.normal_transactions
                ORDER BY time_stamp ASC
            """
            cursor.execute(query)

        return cursor.fetchall()
    finally:
        cursor.close()


def close_connection():
    """Close the CockroachDB connection."""
    global _connection
    if _connection:
        _connection.close()
        _connection = None
