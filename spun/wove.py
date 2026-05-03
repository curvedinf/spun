from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, Set

from wove.environment import EnvironmentExecutor

from .runtime import config, runtime
from .serialization import loads, dumps


class SpunEnvironmentExecutor(EnvironmentExecutor):
    """
    Wove environment executor that submits task frames into Spun's ledger.
    """

    def __init__(self, *, project_dir: Optional[str] = None) -> None:
        self.project_dir = project_dir
        self._run_ids: Set[str] = set()
        self._last_event_id = 0

    async def start(
        self,
        *,
        environment_name: str,
        environment_config: Dict[str, Any],
        run_config: Dict[str, Any],
    ) -> None:
        del environment_name, run_config
        project_dir = environment_config.get("project_dir") or self.project_dir
        config(project_dir=project_dir, discover=False)

    async def send(self, frame: Dict[str, Any]) -> None:
        frame_type = frame.get("type")
        if frame_type == "run_task":
            payload = dumps(
                {
                    "type": "wove_task",
                    "callable": frame["callable"],
                    "args": frame["args"],
                }
            )
            run_id = frame["run_id"]
            self._run_ids.add(run_id)
            runtime.ledger.submit(
                payload=payload,
                run_id=run_id,
                task_id=frame["task_id"],
                source="wove",
            )
            return

        if frame_type == "cancel_task":
            runtime.ledger.cancel_by_run_id(frame["run_id"])
            return

        if frame_type == "shutdown":
            return

        raise ValueError(f"Unsupported frame type: {frame_type}")

    async def recv(self) -> Dict[str, Any]:
        wove_event_types = {"task_started", "heartbeat", "log", "task_result", "task_cancelled", "task_error"}
        while True:
            event = runtime.ledger.next_event(after_id=self._last_event_id, run_ids=tuple(self._run_ids))
            if event is not None:
                self._last_event_id = int(event["id"])
                if event["type"] not in wove_event_types:
                    continue
                return self._frame_from_event(event)
            await asyncio.sleep(0.05)

    async def stop(self) -> None:
        self._run_ids.clear()

    def _frame_from_event(self, event: Any) -> Dict[str, Any]:
        frame: Dict[str, Any] = {
            "type": event["type"],
            "run_id": event["run_id"],
            "task_id": event["task_id"],
        }
        if event["type"] == "task_result":
            frame["result"] = loads(event["result"])
        elif event["type"] == "task_error":
            frame["error"] = {
                "kind": event["error_kind"] or "ExecutionError",
                "message": event["error_message"] or "",
                "traceback": event["error_traceback"] or "",
                "retryable": False,
                "source": "spun",
            }
        return frame


_registered = False
_original_build_executor_from_name = None


def register() -> None:
    """
    Register the `spun` Wove executor name without modifying Wove itself.
    """

    global _registered, _original_build_executor_from_name
    if _registered:
        return

    import wove.environment as environment

    _original_build_executor_from_name = environment.build_executor_from_name

    def build_executor_from_name(name: str) -> EnvironmentExecutor:
        if name == "spun":
            return SpunEnvironmentExecutor()
        return _original_build_executor_from_name(name)

    environment.build_executor_from_name = build_executor_from_name
    _registered = True
