import argparse
import json
import logging
import os
from pathlib import Path
import sqlite3

from .core import Watcher


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Poll per-user Slurm failures once; schedule externally.")
    parser.add_argument("--config", type=Path, default=Path("~/.config/slurm-failure-watcher/config.json").expanduser())
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--retry-uncertain", action="store_true", help="retry ambiguous deliveries; may duplicate Slack posts")
    mode.add_argument("--retry-blocked", action="store_true", help="retry rejected Slack deliveries after fixing the webhook")
    mode.add_argument("--status", action="store_true", help="show delivery counts and scan times without querying Slurm")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        with args.config.open() as stream:
            config = json.load(stream)
        if not isinstance(config, dict):
            raise ValueError("configuration must be a JSON object")
        watcher = Watcher(config)
        if args.status:
            with watcher.database() as db:
                if db is None:
                    logging.info("another watcher holds the lock; try status again later")
                    return 1
                print(json.dumps({"scan_times_epoch": dict(db.execute("SELECT * FROM meta")),
                    "chains": [dict(row) for row in db.execute("""SELECT status,count(*) AS count,
                        SUM(error IS NOT NULL) AS errors FROM chain_roots GROUP BY status""")],
                    "chain_targets": [dict(row) for row in db.execute("""SELECT status,count(*) AS count
                        FROM chain_members GROUP BY status""")],
                    "deliveries": [dict(row) for row in db.execute("""SELECT slack_status,
                        report_done, count(*) AS count FROM jobs GROUP BY slack_status, report_done""")]}, indent=2))
            return 0
        retry = "uncertain" if args.retry_uncertain else "blocked" if args.retry_blocked else None
        return 0 if watcher.run(retry=retry) else 1
    except (OSError, ValueError, sqlite3.Error) as error:
        # Configuration parsing may contain secrets: do not echo values or URLs.
        logging.error("watcher could not run (%s); check configuration, permissions, and state storage", type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
