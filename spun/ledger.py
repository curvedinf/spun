from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence
import sqlite3
import traceback
import uuid

from .errors import WorkNotFoundError


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: Optional[datetime] = None) -> str:
    value = utc_now() if value is None else value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


@dataclass(frozen=True)
class WorkItem:
    id: str
    source: str
    run_id: str
    task_id: str
    status: str
    payload: bytes
    attempts: int
    max_attempts: int


class Ledger:
    """
    SQLite durable ledger for MVP Spun.
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS work (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    schedule_id TEXT,
                    status TEXT NOT NULL,
                    payload BLOB NOT NULL,
                    result BLOB,
                    error_kind TEXT,
                    error_message TEXT,
                    error_traceback TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    queued_at TEXT NOT NULL,
                    not_before TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_work_run_id ON work(run_id);
                CREATE INDEX IF NOT EXISTS idx_work_status ON work(status, not_before, queued_at);

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    work_id TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    message TEXT,
                    result BLOB,
                    error_kind TEXT,
                    error_message TEXT,
                    error_traceback TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(work_id) REFERENCES work(id)
                );

                CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id, id);
                CREATE INDEX IF NOT EXISTS idx_events_work_id ON events(work_id, id);

                CREATE TABLE IF NOT EXISTS schedule_state (
                    schedule_id TEXT PRIMARY KEY,
                    next_run_at TEXT NOT NULL,
                    last_enqueued_at TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def submit(
        self,
        *,
        payload: bytes,
        run_id: Optional[str] = None,
        task_id: str = "work",
        source: str = "wove",
        schedule_id: Optional[str] = None,
        not_before: Optional[datetime] = None,
        max_attempts: int = 3,
    ) -> str:
        work_id = uuid.uuid4().hex
        run_id = run_id or work_id
        now = utc_iso()
        not_before_value = utc_iso(not_before)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO work (
                    id, source, run_id, task_id, schedule_id, status, payload,
                    attempts, max_attempts, queued_at, not_before, updated_at
                )
                VALUES (?, ?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?, ?)
                """,
                (
                    work_id,
                    source,
                    run_id,
                    task_id,
                    schedule_id,
                    payload,
                    int(max_attempts),
                    now,
                    not_before_value,
                    now,
                ),
            )
            self._insert_event(
                conn,
                work_id=work_id,
                run_id=run_id,
                task_id=task_id,
                event_type="queued",
                message=f"queued {task_id}",
            )
        return work_id

    def claim_next(self) -> Optional[WorkItem]:
        now = utc_iso()
        conn = self.connect()
        try:
            conn.isolation_level = None
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT * FROM work
                WHERE status = 'queued' AND not_before <= ?
                ORDER BY queued_at, id
                LIMIT 1
                """,
                (now,),
            ).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None

            attempts = int(row["attempts"]) + 1
            conn.execute(
                """
                UPDATE work
                SET status = 'running', attempts = ?, started_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (attempts, now, now, row["id"]),
            )
            self._insert_event(
                conn,
                work_id=row["id"],
                run_id=row["run_id"],
                task_id=row["task_id"],
                event_type="task_started",
                message=f"started {row['task_id']}",
            )
            conn.execute("COMMIT")
            return WorkItem(
                id=row["id"],
                source=row["source"],
                run_id=row["run_id"],
                task_id=row["task_id"],
                status="running",
                payload=row["payload"],
                attempts=attempts,
                max_attempts=int(row["max_attempts"]),
            )
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def complete(self, work_id: str, result: bytes) -> None:
        now = utc_iso()
        with self.connect() as conn:
            row = self._require_work(conn, work_id)
            conn.execute(
                """
                UPDATE work
                SET status = 'succeeded', result = ?, finished_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (result, now, now, work_id),
            )
            self._insert_event(
                conn,
                work_id=work_id,
                run_id=row["run_id"],
                task_id=row["task_id"],
                event_type="task_result",
                message=f"completed {row['task_id']}",
                result=result,
            )

    def fail(self, work_id: str, exc: BaseException) -> None:
        now = utc_iso()
        error_kind = type(exc).__name__
        error_message = str(exc)
        error_traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        with self.connect() as conn:
            row = self._require_work(conn, work_id)
            conn.execute(
                """
                UPDATE work
                SET status = 'failed', error_kind = ?, error_message = ?,
                    error_traceback = ?, finished_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (error_kind, error_message, error_traceback, now, now, work_id),
            )
            self._insert_event(
                conn,
                work_id=work_id,
                run_id=row["run_id"],
                task_id=row["task_id"],
                event_type="task_error",
                message=error_message,
                error_kind=error_kind,
                error_message=error_message,
                error_traceback=error_traceback,
            )

    def cancel_by_run_id(self, run_id: str) -> None:
        now = utc_iso()
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM work WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return
            if row["status"] == "queued":
                conn.execute(
                    """
                    UPDATE work
                    SET status = 'cancelled', cancel_requested = 1, finished_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, now, row["id"]),
                )
                event_type = "task_cancelled"
            else:
                conn.execute(
                    """
                    UPDATE work
                    SET cancel_requested = 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (now, row["id"]),
                )
                event_type = "cancel_requested"
            self._insert_event(
                conn,
                work_id=row["id"],
                run_id=row["run_id"],
                task_id=row["task_id"],
                event_type=event_type,
                message=f"cancel requested for {row['task_id']}",
            )

    def recover_running(self) -> int:
        now = utc_iso()
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM work WHERE status = 'running'").fetchall()
            for row in rows:
                if int(row["attempts"]) >= int(row["max_attempts"]):
                    conn.execute(
                        """
                        UPDATE work
                        SET status = 'failed', error_kind = 'WorkerLost',
                            error_message = 'Work was running when Spun stopped.',
                            finished_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (now, now, row["id"]),
                    )
                    event_type = "task_error"
                    message = "work failed after restart recovery exhausted attempts"
                else:
                    conn.execute(
                        """
                        UPDATE work
                        SET status = 'queued', started_at = NULL, updated_at = ?
                        WHERE id = ?
                        """,
                        (now, row["id"]),
                    )
                    event_type = "requeued"
                    message = "requeued after restart"
                self._insert_event(
                    conn,
                    work_id=row["id"],
                    run_id=row["run_id"],
                    task_id=row["task_id"],
                    event_type=event_type,
                    message=message,
                )
            return len(rows)

    def next_event(self, *, after_id: int, run_ids: Sequence[str]) -> Optional[sqlite3.Row]:
        if not run_ids:
            return None
        placeholders = ",".join("?" for _ in run_ids)
        with self.connect() as conn:
            return conn.execute(
                f"""
                SELECT * FROM events
                WHERE id > ? AND run_id IN ({placeholders})
                ORDER BY id
                LIMIT 1
                """,
                (after_id, *run_ids),
            ).fetchone()

    def counts(self) -> Dict[str, int]:
        with self.connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS count FROM work GROUP BY status").fetchall()
            return {row["status"]: int(row["count"]) for row in rows}

    def list_work(self, limit: int = 50) -> List[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT id, source, run_id, task_id, status, attempts, max_attempts,
                           queued_at, started_at, finished_at, updated_at
                    FROM work
                    ORDER BY queued_at DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            )

    def get_work(self, work_id_or_prefix: str) -> sqlite3.Row:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM work WHERE id = ?", (work_id_or_prefix,)).fetchone()
            if row is not None:
                return row
            row = conn.execute("SELECT * FROM work WHERE run_id = ?", (work_id_or_prefix,)).fetchone()
            if row is not None:
                return row
            row = conn.execute(
                "SELECT * FROM work WHERE id LIKE ? ORDER BY queued_at DESC LIMIT 1",
                (f"{work_id_or_prefix}%",),
            ).fetchone()
            if row is None:
                raise WorkNotFoundError(f"Work not found: {work_id_or_prefix}")
            return row

    def events_for_work(self, work_id_or_prefix: str) -> List[sqlite3.Row]:
        work = self.get_work(work_id_or_prefix)
        with self.connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM events WHERE work_id = ? ORDER BY id",
                    (work["id"],),
                ).fetchall()
            )

    def get_schedule_next_run(self, schedule_id: str) -> Optional[datetime]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT next_run_at FROM schedule_state WHERE schedule_id = ?",
                (schedule_id,),
            ).fetchone()
            if row is None:
                return None
            return parse_iso(row["next_run_at"])

    def set_schedule_next_run(
        self,
        schedule_id: str,
        next_run_at: datetime,
        *,
        last_enqueued_at: Optional[datetime] = None,
    ) -> None:
        now = utc_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO schedule_state (
                    schedule_id, next_run_at, last_enqueued_at, updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(schedule_id) DO UPDATE SET
                    next_run_at = excluded.next_run_at,
                    last_enqueued_at = COALESCE(excluded.last_enqueued_at, schedule_state.last_enqueued_at),
                    updated_at = excluded.updated_at
                """,
                (
                    schedule_id,
                    utc_iso(next_run_at),
                    utc_iso(last_enqueued_at) if last_enqueued_at is not None else None,
                    now,
                ),
            )

    def _require_work(self, conn: sqlite3.Connection, work_id: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM work WHERE id = ?", (work_id,)).fetchone()
        if row is None:
            raise WorkNotFoundError(f"Work not found: {work_id}")
        return row

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        *,
        work_id: str,
        run_id: str,
        task_id: str,
        event_type: str,
        message: Optional[str] = None,
        result: Optional[bytes] = None,
        error_kind: Optional[str] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO events (
                work_id, run_id, task_id, type, message, result,
                error_kind, error_message, error_traceback, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                work_id,
                run_id,
                task_id,
                event_type,
                message,
                result,
                error_kind,
                error_message,
                error_traceback,
                utc_iso(),
            ),
        )
