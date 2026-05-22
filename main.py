"""CLI entry point for the metrics pipeline.

Triggered on wallet connect (single wallet):
    python main.py first-time <user_id> <wallet_address> [--tmp-root TMP]

Daily job — one user (targeted rerun):
    python main.py daily <user_id>                           [--tmp-root TMP]
    python main.py daily <user_id> --wallets W1 W2 ...      [--tmp-root TMP]

Daily job — all users via batched Polars scans:
    python main.py daily-all                                 [--tmp-root TMP] [--batch-size N]
"""

import argparse
from pathlib import Path

from metrics_pipeline.pipeline import daily_all_flow, daily_flow, first_time_flow


def main() -> None:
    parser = argparse.ArgumentParser(description="EtherScan metrics pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("first-time")
    p.add_argument("user_id")
    p.add_argument("wallet_address")
    p.add_argument("--tmp-root", default="tmp", help="Tmp root directory (default: tmp)")

    p = sub.add_parser("daily")
    p.add_argument("user_id")
    p.add_argument("--wallets", nargs="+", default=None, metavar="WALLET",
                   help="Wallets to process (default: all wallets in MongoDB)")
    p.add_argument("--tmp-root", default="tmp", help="Tmp root directory (default: tmp)")

    p = sub.add_parser("daily-all")
    p.add_argument("--tmp-root", default="tmp", help="Tmp root directory (default: tmp)")
    p.add_argument("--batch-size", type=int, default=1000, metavar="N",
                   help="Users per Polars scan batch (default: 1000)")

    args = parser.parse_args()
    tmp_root = Path(args.tmp_root)

    if args.command == "first-time":
        first_fetch_root = tmp_root / "first_fetch"
        first_time_flow(args.user_id, args.wallet_address, first_fetch_root)
        print(f"first-time flow complete: user={args.user_id} wallet={args.wallet_address}")
    elif args.command == "daily":
        daily_fetch_root = tmp_root / "daily_fetch"
        daily_flow(args.user_id, args.wallets, daily_fetch_root)
        wallets_label = args.wallets or "all"
        print(f"daily flow complete: user={args.user_id} wallets={wallets_label}")
    elif args.command == "daily-all":
        daily_fetch_root = tmp_root / "daily_fetch"
        daily_all_flow(daily_fetch_root, batch_size=args.batch_size)
        print(f"daily-all flow complete: tmp_root={tmp_root} batch_size={args.batch_size}")


if __name__ == "__main__":
    main()
