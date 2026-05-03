from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Iterator, List, Optional

from .errors import CallFailedError, CallTimeoutError
from .runtime import runtime
from .serialization import dumps, loads


class CallPromise:
    """
    Promise-like handle for a durable Spun call invocation.
    """

    def __init__(self, call_id: str) -> None:
        self.id = call_id

    @classmethod
    def submit(
        cls,
        *,
        name: str,
        args: tuple,
        kwargs: Dict[str, Any],
        args_hash: Optional[str] = None,
        scope: Optional[str] = None,
        key: Optional[str] = None,
    ) -> "CallPromise":
        runtime.ledger.ensure_schema()
        call_id = runtime.ledger.submit_call(
            call_name=name,
            payload=dumps(
                {
                    "type": "spun_call",
                    "name": name,
                    "args": args,
                    "kwargs": kwargs,
                }
            ),
            args_hash=args_hash,
            return_scope=scope,
            return_key=key,
        )
        return cls(call_id)

    @property
    def status(self) -> str:
        runtime.ledger.ensure_schema()
        return str(runtime.ledger.get_work(self.id)["status"])

    def result(self, timeout: Optional[float] = None) -> Any:
        runtime.ledger.ensure_schema()
        started = time.monotonic()
        while True:
            row = runtime.ledger.get_work(self.id)
            status = row["status"]
            if status == "succeeded":
                runtime.ledger.acknowledge_orphan(self.id)
                return loads(row["result"])
            if status == "failed":
                runtime.ledger.acknowledge_orphan(self.id)
                raise CallFailedError(
                    f"Spun call '{row['task_id']}' failed: {row['error_kind']}: {row['error_message']}"
                )
            if status == "cancelled":
                runtime.ledger.acknowledge_orphan(self.id)
                raise CallFailedError(f"Spun call '{row['task_id']}' was cancelled.")

            if timeout is not None and time.monotonic() - started >= timeout:
                raise CallTimeoutError(f"Timed out waiting for Spun call {self.id}.")
            time.sleep(0.05)

    wait = result

    def cancel(self) -> None:
        runtime.ledger.cancel_by_run_id(self.id)

    def ack(self) -> None:
        runtime.ledger.acknowledge_orphan(self.id)

    def claim(self) -> "CallPromise":
        runtime.ledger.claim_orphan(self.id)
        return self

    @property
    def events(self) -> List[Dict[str, Any]]:
        runtime.ledger.ensure_schema()
        return [
            {
                "id": row["id"],
                "type": row["type"],
                "message": row["message"],
                "created_at": row["created_at"],
            }
            for row in runtime.ledger.events_for_work(self.id)
        ]

    def stream(self, *, poll_interval: float = 0.05) -> Iterator[Dict[str, Any]]:
        seen = 0
        while True:
            events = self.events
            for event in events:
                if int(event["id"]) <= seen:
                    continue
                seen = int(event["id"])
                yield event

            status = self.status
            if status in {"succeeded", "failed", "cancelled"}:
                return
            time.sleep(poll_interval)

    def __await__(self) -> Any:
        async def wait_async() -> Any:
            return await asyncio.to_thread(self.result)

        return wait_async().__await__()

    def __repr__(self) -> str:
        return f"CallPromise(id={self.id!r}, status={self.status!r})"
