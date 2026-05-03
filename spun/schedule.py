from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, List, Optional
from zoneinfo import ZoneInfo


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _zone(value: Optional[str]) -> timezone:
    if value is None:
        return timezone.utc
    return ZoneInfo(value)


def _matches(value: int, expected: Any) -> bool:
    if expected is None or expected == "*":
        return True
    if isinstance(expected, int):
        return value == expected
    if isinstance(expected, str):
        return value == int(expected)
    if isinstance(expected, Iterable):
        return value in {int(item) for item in expected}
    return False


@dataclass(frozen=True)
class Cron:
    """
    Discoverable cron-shaped schedule.
    """

    minute: Any = None
    hour: Any = None
    weekday: Any = None
    monthday: Any = None
    month: Any = None
    zone: Optional[str] = None
    second: Any = 0

    def next_after(self, after: datetime) -> datetime:
        local_zone = _zone(self.zone)
        candidate = after.astimezone(local_zone) + timedelta(seconds=1)
        if self.second is None:
            candidate = candidate.replace(microsecond=0)
        else:
            candidate = candidate.replace(microsecond=0)

        for _ in range(370 * 24 * 60 * 60):
            if (
                _matches(candidate.second, self.second)
                and _matches(candidate.minute, self.minute)
                and _matches(candidate.hour, self.hour)
                and _matches(candidate.weekday(), self.weekday)
                and _matches(candidate.day, self.monthday)
                and _matches(candidate.month, self.month)
            ):
                return candidate.astimezone(timezone.utc)
            candidate += timedelta(seconds=1)

        raise RuntimeError("Could not find next Cron run within one year.")


@dataclass(frozen=True)
class Calendar:
    """
    Wall-clock recurrence for cases where cron fields read poorly.
    """

    at: Optional[time] = None
    weekday: Any = None
    monthday: Any = None
    month: Any = None
    zone: Optional[str] = None

    def next_after(self, after: datetime) -> datetime:
        local_zone = _zone(self.zone)
        candidate = after.astimezone(local_zone) + timedelta(seconds=1)
        candidate = candidate.replace(microsecond=0)

        for _ in range(370 * 24 * 60 * 60):
            if self.at is not None:
                if (
                    candidate.hour != self.at.hour
                    or candidate.minute != self.at.minute
                    or candidate.second != self.at.second
                ):
                    candidate += timedelta(seconds=1)
                    continue
            if (
                _matches(candidate.weekday(), self.weekday)
                and _matches(candidate.day, self.monthday)
                and _matches(candidate.month, self.month)
            ):
                return candidate.astimezone(timezone.utc)
            candidate += timedelta(seconds=1)

        raise RuntimeError("Could not find next Calendar run within one year.")


@dataclass(frozen=True)
class ScheduleEntry:
    id: str
    trigger: Any
    work: Any
    name: str


class ScheduleRegistry:
    def __init__(self) -> None:
        self._entries: List[ScheduleEntry] = []

    def add(self, trigger: Any, work: Any) -> Any:
        name = getattr(work, "__qualname__", getattr(work, "__name__", type(work).__name__))
        module = getattr(work, "__module__", "__main__")
        entry_id = f"{module}.{name}:{repr(trigger)}"
        self._entries.append(ScheduleEntry(entry_id, trigger, work, name))
        return work

    def clear(self) -> None:
        self._entries.clear()

    def entries(self) -> List[ScheduleEntry]:
        return list(self._entries)


registry = ScheduleRegistry()


def schedule(trigger: Any, work: Optional[Any] = None) -> Any:
    """
    Register scheduled Wove-shaped work.
    """

    def decorator(obj: Any) -> Any:
        return registry.add(trigger, obj)

    if work is not None:
        return decorator(work)
    return decorator


def next_after(trigger: Any, after: Optional[datetime] = None) -> datetime:
    after = _utc_now() if after is None else after
    if after.tzinfo is None:
        after = after.replace(tzinfo=timezone.utc)
    if isinstance(trigger, timedelta):
        return after + trigger
    if isinstance(trigger, (date, datetime)) and not isinstance(trigger, datetime):
        return datetime.combine(trigger, time.min, timezone.utc)
    if isinstance(trigger, datetime):
        return trigger if trigger.tzinfo is not None else trigger.replace(tzinfo=timezone.utc)
    if isinstance(trigger, time):
        today = datetime.combine(after.date(), trigger, timezone.utc)
        if today > after:
            return today
        return today + timedelta(days=1)
    if hasattr(trigger, "next_after"):
        return trigger.next_after(after)
    raise TypeError("schedule trigger must be a datetime object, timedelta, Cron, or Calendar.")
