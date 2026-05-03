from __future__ import annotations

import argparse
from typing import Iterable

from .errors import WorkNotFoundError
from .runtime import config, runtime
from .serialization import loads
from .worker import Worker


def _print_rows(rows: Iterable[object]) -> None:
    for row in rows:
        print(
            f"{row['id'][:12]}  {row['status']:<9}  "
            f"{row['source']:<8}  {row['task_id']:<24}  attempts={row['attempts']}"
        )


def start_command(_args: argparse.Namespace) -> int:
    try:
        Worker().run_forever()
    except KeyboardInterrupt:
        return 0
    return 0


def status_command(_args: argparse.Namespace) -> int:
    config()
    counts = runtime.ledger.counts()
    if not counts:
        print("no work")
        return 0
    for status, count in sorted(counts.items()):
        print(f"{status}: {count}")
    return 0


def work_command(args: argparse.Namespace) -> int:
    config()
    _print_rows(runtime.ledger.list_work(limit=args.limit))
    return 0


def why_command(args: argparse.Namespace) -> int:
    config()
    try:
        row = runtime.ledger.get_work(args.work_id)
    except WorkNotFoundError as exc:
        print(exc)
        return 1

    print(f"id: {row['id']}")
    print(f"task: {row['task_id']}")
    print(f"source: {row['source']}")
    print(f"status: {row['status']}")
    print(f"attempts: {row['attempts']}/{row['max_attempts']}")
    if row["error_message"]:
        print(f"error: {row['error_kind']}: {row['error_message']}")
    if row["status"] == "queued":
        print("reason: waiting for the local Spun worker to claim it")
    elif row["status"] == "running":
        print("reason: running in the local Spun process")
    elif row["status"] == "succeeded":
        print("reason: completed successfully")
    elif row["status"] == "failed":
        print("reason: task raised an exception")
    elif row["status"] == "cancelled":
        print("reason: cancellation was requested before completion")
    return 0


def tail_command(args: argparse.Namespace) -> int:
    config()
    try:
        events = runtime.ledger.events_for_work(args.work_id)
    except WorkNotFoundError as exc:
        print(exc)
        return 1

    for event in events:
        line = f"{event['created_at']}  {event['type']}"
        if event["message"]:
            line += f"  {event['message']}"
        print(line)
    return 0


def result_command(args: argparse.Namespace) -> int:
    config()
    row = runtime.ledger.get_work(args.work_id)
    if row["result"] is None:
        print(f"{row['id']} has no result")
        return 1
    print(repr(loads(row["result"])))
    return 0


def orphans_command(args: argparse.Namespace) -> int:
    config()
    rows = runtime.ledger.list_orphans(
        scope=args.scope,
        name=args.name,
        key=args.key,
        limit=args.limit,
    )
    for row in rows:
        print(
            f"{row['work_id'][:12]}  {row['status']:<9}  "
            f"{row['call_name']:<24}  scope={row['return_scope'] or '-'}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="spun")
    sub = parser.add_subparsers(dest="command")

    start = sub.add_parser("start")
    start.set_defaults(func=start_command)

    status = sub.add_parser("status")
    status.set_defaults(func=status_command)

    work = sub.add_parser("work")
    work.add_argument("--limit", type=int, default=50)
    work.set_defaults(func=work_command)

    why = sub.add_parser("why")
    why.add_argument("work_id")
    why.set_defaults(func=why_command)

    tail = sub.add_parser("tail")
    tail.add_argument("work_id")
    tail.set_defaults(func=tail_command)

    result = sub.add_parser("result")
    result.add_argument("work_id")
    result.set_defaults(func=result_command)

    orphans = sub.add_parser("orphans")
    orphans.add_argument("--scope")
    orphans.add_argument("--name")
    orphans.add_argument("--key")
    orphans.add_argument("--limit", type=int, default=50)
    orphans.set_defaults(func=orphans_command)

    return parser


def main(argv: object = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    return args.func(args)
