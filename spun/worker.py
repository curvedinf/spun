from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event, Thread
from typing import Dict, Optional
import inspect
import os
import time
import uuid

from wove import Weave, weave

from .ledger import WorkItem, utc_now
from .runtime import config, runtime
from .schedule import next_after, registry as schedule_registry
from .serialization import dumps, loads


class Worker:
    """
    In-process Spun worker for the MVP runtime.
    """

    def __init__(
        self,
        *,
        project_dir: Optional[str] = None,
        poll_interval: float = 0.05,
        max_workers: Optional[int] = None,
    ) -> None:
        self.project_dir = project_dir
        self.poll_interval = poll_interval
        self.max_workers = max_workers or min(32, (os.cpu_count() or 4) * 4)
        self._stop = Event()
        self._thread: Optional[Thread] = None

    def run_forever(self) -> None:
        config(project_dir=self.project_dir, discover=True, reset=True)
        runtime.ledger.recover_running()
        self._run_loop(stop_when_idle=False)

    def run_until_idle(self) -> None:
        config(project_dir=self.project_dir, discover=True, reset=True)
        runtime.ledger.recover_running()
        self._run_loop(stop_when_idle=True)

    def start_background(self) -> Thread:
        thread = Thread(target=self.run_forever, daemon=True)
        thread.start()
        self._thread = thread
        return thread

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def _run_loop(self, *, stop_when_idle: bool) -> None:
        futures: Dict[Future, WorkItem] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            while not self._stop.is_set():
                self._enqueue_due_schedules()
                self._reap(futures)

                claimed = False
                while len(futures) < self.max_workers:
                    item = runtime.ledger.claim_next()
                    if item is None:
                        break
                    claimed = True
                    futures[pool.submit(self._execute_item, item)] = item

                if stop_when_idle and not futures and not claimed:
                    self._enqueue_due_schedules()
                    item = runtime.ledger.claim_next()
                    if item is None:
                        break
                    futures[pool.submit(self._execute_item, item)] = item

                time.sleep(self.poll_interval)

            while futures:
                self._reap(futures)
                time.sleep(self.poll_interval)

    def _reap(self, futures: Dict[Future, WorkItem]) -> None:
        for future, item in list(futures.items()):
            if not future.done():
                continue
            futures.pop(future, None)
            try:
                result = future.result()
            except Exception as exc:
                runtime.ledger.fail(item.id, exc)
            else:
                runtime.ledger.complete(item.id, dumps(result))

    def _execute_item(self, item: WorkItem) -> object:
        payload = loads(item.payload)
        payload_type = payload.get("type")
        if payload_type == "wove_task":
            return self._execute_callable(payload["callable"], payload["args"])
        if payload_type == "scheduled_work":
            return self._execute_scheduled(payload["work"])
        raise ValueError(f"Unknown Spun payload type: {payload_type}")

    def _execute_callable(self, func: object, args: dict) -> object:
        async def run_value() -> object:
            value = func(**args)
            if inspect.isawaitable(value):
                return await value
            return value

        with weave() as w:
            @w.do
            async def value():
                return await run_value()

        return w.result.value

    def _execute_scheduled(self, work: object) -> object:
        if inspect.isclass(work) and issubclass(work, Weave):
            with weave(work) as w:
                pass
            return w.result.final

        if isinstance(work, Weave):
            with weave(type(work)) as w:
                pass
            return w.result.final

        value = self._execute_callable(work, {})
        result = getattr(value, "result", None)
        if result is not None and hasattr(result, "final"):
            return result.final
        return value

    def _enqueue_due_schedules(self) -> None:
        now = utc_now()
        for entry in schedule_registry.entries():
            next_run = runtime.ledger.get_schedule_next_run(entry.id)
            if next_run is None:
                runtime.ledger.set_schedule_next_run(entry.id, next_after(entry.trigger, now))
                continue
            if next_run > now:
                continue

            run_id = f"schedule:{entry.id}:{uuid.uuid4().hex}"
            runtime.ledger.submit(
                payload=dumps({"type": "scheduled_work", "work": entry.work}),
                run_id=run_id,
                task_id=entry.name,
                source="schedule",
                schedule_id=entry.id,
            )
            runtime.ledger.set_schedule_next_run(
                entry.id,
                next_after(entry.trigger, now),
                last_enqueued_at=now,
            )
