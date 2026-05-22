"""
Integration Examples - Copy and paste these into your application
"""

# ============================================================================
# EXAMPLE 1: Simple Function Call
# ============================================================================

def get_wallet_metrics(wallet_addresses: list[str] = None):
    """Get metrics for wallets."""
    from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
    from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

    try:
        # Fetch transactions
        transactions = fetch_wallet_transactions(wallet_addresses=wallet_addresses)

        if not transactions:
            return {"error": "No data found"}

        # Calculate metrics
        metrics = calculate_metrics_from_transactions(transactions)

        # Format response
        result = {}
        for wallet_addr, m in metrics.items():
            result[wallet_addr] = {
                "active_days": m.active_days,
                "total_transactions": m.total_transactions_count,
                "total_gas_burned": m.total_gas_burned,
                "first_tx_date": str(m.first_tx_date),
            }

        return result
    finally:
        close_connection()


# Usage:
# metrics = get_wallet_metrics(wallet_addresses=["0x123..."])
# metrics = get_wallet_metrics()  # All wallets


# ============================================================================
# EXAMPLE 2: FastAPI Integration
# ============================================================================

def fastapi_example():
    """Example FastAPI endpoint."""
    from fastapi import FastAPI, Query
    from typing import Optional
    from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
    from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

    app = FastAPI()

    @app.get("/metrics")
    async def get_metrics(wallets: Optional[list[str]] = Query(None)):
        """
        Get metrics for wallets.

        Parameters:
            wallets: Optional list of wallet addresses
                - If provided: returns metrics for those wallets only
                - If omitted: returns metrics for all wallets
        """
        try:
            transactions = fetch_wallet_transactions(wallet_addresses=wallets)

            if not transactions:
                return {"error": "No data found", "wallets": wallets}

            metrics = calculate_metrics_from_transactions(transactions)

            result = {}
            for wallet_addr, m in metrics.items():
                result[wallet_addr] = {
                    "active_days": m.active_days,
                    "total_transactions": m.total_transactions_count,
                    "total_gas_burned": m.total_gas_burned,
                    "first_tx_date": str(m.first_tx_date),
                    "chains": [
                        {
                            "chain": c.chain,
                            "active_days": c.active_days,
                            "total_gas_burned": c.total_gas_burned,
                        }
                        for c in m.wallets[0].chains
                    ]
                }

            return {"query": {"wallets": wallets or "all"}, "metrics": result}
        finally:
            close_connection()

    return app


# Usage:
# app = fastapi_example()
# uvicorn app:app --reload


# ============================================================================
# EXAMPLE 3: Flask Integration
# ============================================================================

def flask_example():
    """Example Flask endpoint."""
    from flask import Flask, request, jsonify
    from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
    from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

    app = Flask(__name__)

    @app.route("/metrics", methods=["GET"])
    def get_metrics():
        """Get metrics for wallets."""
        try:
            wallets = request.args.getlist("wallets") or None

            transactions = fetch_wallet_transactions(wallet_addresses=wallets)

            if not transactions:
                return jsonify({"error": "No data found"}), 404

            metrics = calculate_metrics_from_transactions(transactions)

            result = {}
            for wallet_addr, m in metrics.items():
                result[wallet_addr] = {
                    "active_days": m.active_days,
                    "total_transactions": m.total_transactions_count,
                    "total_gas_burned": m.total_gas_burned,
                }

            return jsonify({"metrics": result})
        finally:
            close_connection()

    return app


# Usage:
# app = flask_example()
# app.run(debug=True)


# ============================================================================
# EXAMPLE 4: Django Integration
# ============================================================================

def django_view_example():
    """Example Django view."""
    from django.http import JsonResponse
    from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
    from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

    def get_metrics(request):
        """Django view for metrics."""
        try:
            wallets = request.GET.getlist("wallets") or None

            transactions = fetch_wallet_transactions(wallet_addresses=wallets)

            if not transactions:
                return JsonResponse({"error": "No data found"}, status=404)

            metrics = calculate_metrics_from_transactions(transactions)

            result = {}
            for wallet_addr, m in metrics.items():
                result[wallet_addr] = {
                    "active_days": m.active_days,
                    "total_transactions": m.total_transactions_count,
                    "total_gas_burned": m.total_gas_burned,
                }

            return JsonResponse({"metrics": result})
        finally:
            close_connection()

    return get_metrics


# ============================================================================
# EXAMPLE 5: Background Task / Celery Integration
# ============================================================================

def celery_example():
    """Example Celery task."""
    from celery import shared_task
    from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
    from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

    @shared_task
    def calculate_wallet_metrics(wallet_addresses=None):
        """Calculate metrics for wallets asynchronously."""
        try:
            transactions = fetch_wallet_transactions(wallet_addresses=wallet_addresses)

            if not transactions:
                return {"error": "No data found"}

            metrics = calculate_metrics_from_transactions(transactions)

            result = {}
            for wallet_addr, m in metrics.items():
                result[wallet_addr] = {
                    "active_days": m.active_days,
                    "total_transactions": m.total_transactions_count,
                    "total_gas_burned": m.total_gas_burned,
                }

            return result
        finally:
            close_connection()

    return calculate_wallet_metrics


# Usage:
# from tasks import calculate_wallet_metrics
# calculate_wallet_metrics.delay(wallet_addresses=["0x123..."])


# ============================================================================
# EXAMPLE 6: Error Handling
# ============================================================================

def error_handling_example():
    """Example with comprehensive error handling."""
    from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
    from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

    def get_metrics_safe(wallet_addresses=None):
        """Get metrics with error handling."""
        try:
            # Validate input
            if wallet_addresses and not isinstance(wallet_addresses, list):
                return {
                    "error": "wallet_addresses must be a list",
                    "status": 400
                }

            # Fetch data
            transactions = fetch_wallet_transactions(wallet_addresses=wallet_addresses)

            # Check if data exists
            if not transactions:
                return {
                    "error": "No transaction data found",
                    "wallets_queried": wallet_addresses,
                    "status": 404
                }

            # Calculate metrics
            metrics = calculate_metrics_from_transactions(transactions)

            # Format results
            result = {
                "status": 200,
                "wallets_found": len(metrics),
                "metrics": {}
            }

            for wallet_addr, m in metrics.items():
                result["metrics"][wallet_addr] = {
                    "active_days": m.active_days,
                    "total_transactions": m.total_transactions_count,
                    "total_gas_burned": round(m.total_gas_burned, 6),
                    "first_tx_date": str(m.first_tx_date),
                }

            return result

        except ConnectionError as e:
            return {"error": f"Database connection failed: {str(e)}", "status": 500}
        except ValueError as e:
            return {"error": f"Invalid data: {str(e)}", "status": 400}
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}", "status": 500}
        finally:
            close_connection()

    return get_metrics_safe


# ============================================================================
# EXAMPLE 7: Batch Processing
# ============================================================================

def batch_processing_example():
    """Process multiple wallet batches efficiently."""
    from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
    from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

    def process_wallets_in_batches(all_wallets, batch_size=100):
        """Process wallets in batches to manage memory."""
        results = {}

        try:
            for i in range(0, len(all_wallets), batch_size):
                batch = all_wallets[i:i + batch_size]

                # Process batch
                transactions = fetch_wallet_transactions(wallet_addresses=batch)
                metrics = calculate_metrics_from_transactions(transactions)

                # Store results
                for wallet_addr, m in metrics.items():
                    results[wallet_addr] = {
                        "active_days": m.active_days,
                        "total_transactions": m.total_transactions_count,
                        "total_gas_burned": m.total_gas_burned,
                    }

            return results
        finally:
            close_connection()

    return process_wallets_in_batches


# Usage:
# wallets = ["0x123...", "0x456...", "0x789...", ...]
# results = process_wallets_in_batches(wallets, batch_size=50)


# ============================================================================
# EXAMPLE 8: Caching with Decorator
# ============================================================================

def caching_example():
    """Example with caching to reduce database calls."""
    from functools import lru_cache
    from metrics_pipeline.cockroachdb import fetch_wallet_transactions, close_connection
    from metrics_pipeline.calculator_db import calculate_metrics_from_transactions

    @lru_cache(maxsize=128)
    def get_metrics_cached(wallet_tuple):
        """Get metrics with caching."""
        try:
            wallets = list(wallet_tuple) if wallet_tuple else None
            transactions = fetch_wallet_transactions(wallet_addresses=wallets)

            if not transactions:
                return None

            metrics = calculate_metrics_from_transactions(transactions)
            return metrics
        finally:
            close_connection()

    def get_metrics(wallet_addresses=None):
        """Public function that uses caching."""
        wallet_tuple = tuple(wallet_addresses) if wallet_addresses else None
        metrics = get_metrics_cached(wallet_tuple)

        if metrics is None:
            return {"error": "No data found"}

        return {
            wallet: {
                "active_days": m.active_days,
                "total_transactions": m.total_transactions_count,
                "total_gas_burned": m.total_gas_burned,
            }
            for wallet, m in metrics.items()
        }

    return get_metrics


if __name__ == "__main__":
    # Run examples
    print("See examples above for integration patterns")
