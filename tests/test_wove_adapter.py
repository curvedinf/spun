import time

from wove import config as wove_config
from wove import weave

from spun import Worker
from spun.runtime import runtime


def test_wove_spun_executor_round_trip(tmp_path):
    worker = Worker(project_dir=str(tmp_path), poll_interval=0.01)
    worker.start_background()
    try:
        wove_config(
            default_environment="local",
            environments={
                "local": {"executor": "local"},
                "spun": {
                    "executor": "spun",
                    "executor_config": {"project_dir": str(tmp_path)},
                },
            },
        )

        with weave(seed=4) as w:
            @w.do
            def local(seed):
                return seed + 1

            @w.do(environment="spun")
            def remote(local):
                return local * 10

        assert w.result.remote == 50
    finally:
        worker.stop()


def test_wove_task_can_use_worker_space_calls(tmp_path):
    (tmp_path / "calls.py").write_text(
        "from spun import call\n"
        "@call\n"
        "def check_db():\n"
        "    return 'ok'\n"
    )
    worker = Worker(project_dir=str(tmp_path), poll_interval=0.01)
    worker.start_background()
    try:
        wove_config(
            default_environment="local",
            environments={
                "local": {"executor": "local"},
                "spun": {
                    "executor": "spun",
                    "executor_config": {"project_dir": str(tmp_path)},
                },
            },
        )

        with weave() as w:
            @w.do(environment="spun")
            def health():
                from spun import call

                return call.check_db()

        assert w.result.health == "ok"
    finally:
        worker.stop()


def test_worker_discovers_and_runs_due_schedule(tmp_path):
    (tmp_path / "schedule.py").write_text(
        "from datetime import timedelta\n"
        "from spun import schedule\n"
        "@schedule(timedelta(milliseconds=10))\n"
        "def scheduled_value():\n"
        "    return 99\n"
    )

    worker = Worker(project_dir=str(tmp_path), poll_interval=0.01)
    worker.start_background()
    try:
        time.sleep(0.08)
    finally:
        worker.stop()

    rows = runtime.ledger.list_work()
    assert any(row["source"] == "schedule" and row["status"] == "succeeded" for row in rows)
