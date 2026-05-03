from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from .calls import call
from .code import discover as discover_code
from .env import load_env
from .ledger import Ledger
from .schedule import registry as schedule_registry


class Runtime:
    """
    Process-wide Spun runtime state.
    """

    def __init__(self) -> None:
        self.project_dir = Path.cwd()
        self.ledger = Ledger(self.project_dir / ".spun" / "spun.sqlite3")
        self.env: Dict[str, str] = {}
        self.loaded_files = []

    def configure(
        self,
        *,
        project_dir: Optional[str] = None,
        discover: bool = False,
        reset: bool = False,
    ) -> "Runtime":
        if project_dir is not None:
            self.project_dir = Path(project_dir).expanduser().resolve()
        else:
            self.project_dir = Path.cwd().resolve()

        self.ledger = Ledger(self.project_dir / ".spun" / "spun.sqlite3")
        self.ledger.ensure_schema()
        self.env = load_env(self.project_dir)

        if reset:
            call.clear()
            schedule_registry.clear()

        if discover:
            self.loaded_files = discover_code(self.project_dir)
        return self


runtime = Runtime()


def config(
    *,
    project_dir: Optional[str] = None,
    discover: bool = False,
    reset: bool = False,
) -> Runtime:
    return runtime.configure(project_dir=project_dir, discover=discover, reset=reset)
