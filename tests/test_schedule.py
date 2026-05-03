from datetime import datetime, timedelta, timezone

from wove import Weave

from spun import Cron, schedule
from spun.schedule import next_after, registry


def test_schedule_decorator_preserves_function():
    registry.clear()

    @schedule(timedelta(minutes=5))
    def refresh():
        return "ok"

    assert refresh() == "ok"
    assert registry.entries()[0].work is refresh


def test_schedule_decorator_preserves_weave_class():
    registry.clear()

    @schedule(timedelta(minutes=5))
    class Nightly(Weave):
        @Weave.do
        def value(self):
            return 1

    assert Nightly.__name__ == "Nightly"
    assert registry.entries()[0].work is Nightly


def test_direct_schedule_registration_preserves_work():
    registry.clear()

    def refresh():
        return "ok"

    assert schedule(timedelta(minutes=5), refresh) is refresh
    assert registry.entries()[0].work is refresh


def test_cron_next_after_is_discoverable_object():
    trigger = Cron(second=3)
    after = datetime(2026, 5, 3, 12, 0, 1, tzinfo=timezone.utc)

    assert next_after(trigger, after) == datetime(2026, 5, 3, 12, 0, 3, tzinfo=timezone.utc)
