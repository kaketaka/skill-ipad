from __future__ import annotations

import argparse
import json

from .db import init_db
from .export_site import export_static_site
from .service import get_dashboard, run_market_cycle, run_review, sync_universe


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper trading system for US and Japan equities.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="Fetch data, generate signals, and paper trade.")
    run_parser.add_argument("--markets", nargs="*", choices=["US", "JP"], default=["US", "JP"])
    subparsers.add_parser("review", help="Create a daily review and bounded strategy update.")
    subparsers.add_parser("init", help="Initialize the local SQLite database.")
    subparsers.add_parser("status", help="Print a compact dashboard JSON.")
    export_parser = subparsers.add_parser("export", help="Export a static iPad dashboard.")
    export_parser.add_argument("--output", default="docs", help="Output directory, defaults to docs.")
    universe_parser = subparsers.add_parser("sync-universe", help="Download full US/JP stock universes.")
    universe_parser.add_argument("--markets", nargs="*", choices=["US", "JP"], default=["US", "JP"])
    args = parser.parse_args()

    if args.command == "init":
        init_db()
        print(json.dumps({"ok": True, "message": "database initialized"}, ensure_ascii=False, indent=2))
    elif args.command == "run":
        print(json.dumps(run_market_cycle(args.markets), ensure_ascii=False, indent=2, default=str))
    elif args.command == "review":
        print(json.dumps(run_review(), ensure_ascii=False, indent=2, default=str))
    elif args.command == "status":
        print(json.dumps(get_dashboard(), ensure_ascii=False, indent=2, default=str))
    elif args.command == "export":
        print(json.dumps(export_static_site(args.output), ensure_ascii=False, indent=2, default=str))
    elif args.command == "sync-universe":
        print(json.dumps(sync_universe(args.markets), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
