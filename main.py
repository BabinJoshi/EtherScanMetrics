"""CLI entry point for the metrics pipeline.

Usage:
    python main.py first-time <user_id> <wallet_address> [--tmp-root TMP]
    python main.py daily      <user_id> <wallet_address> [--tmp-root TMP]
"""

import argparse
from pathlib import Path

from metrics_pipeline.pipeline import daily_flow, first_time_flow


def main() -> None:
    parser = argparse.ArgumentParser(description="EtherScan metrics pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd in ("first-time", "daily"):
        p = sub.add_parser(cmd)
        p.add_argument("user_id")
        p.add_argument("wallet_address")
        p.add_argument("--tmp-root", default="tmp", help="Root directory for parquet dumps")

    args = parser.parse_args()
    tmp_root = Path(args.tmp_root)

    if args.command == "first-time":
        first_time_flow(args.user_id, args.wallet_address, tmp_root)
        print(f"first-time flow complete: user={args.user_id} wallet={args.wallet_address}")
    else:
        daily_flow(args.user_id, args.wallet_address, tmp_root)
        print(f"daily flow complete: user={args.user_id} wallet={args.wallet_address}")


if __name__ == "__main__":
    main()
