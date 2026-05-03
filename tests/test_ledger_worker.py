from spun import Worker, config
from spun.runtime import runtime
from spun.serialization import dumps, loads


def test_worker_executes_queued_callable(tmp_path):
    config(project_dir=str(tmp_path), discover=False, reset=True)
    runtime.ledger.submit(
        payload=dumps(
            {
                "type": "wove_task",
                "callable": lambda: 7,
                "args": {},
            }
        ),
        run_id="run-1",
        task_id="value",
    )

    Worker(project_dir=str(tmp_path), poll_interval=0.01).run_until_idle()

    row = runtime.ledger.get_work("run-1")
    assert row["status"] == "succeeded"
    assert loads(row["result"]) == 7


def test_worker_requeues_running_work_on_restart(tmp_path):
    config(project_dir=str(tmp_path), discover=False, reset=True)
    runtime.ledger.submit(
        payload=dumps({"type": "wove_task", "callable": lambda: 8, "args": {}}),
        run_id="run-2",
        task_id="value",
    )
    item = runtime.ledger.claim_next()
    assert item is not None

    Worker(project_dir=str(tmp_path), poll_interval=0.01).run_until_idle()

    row = runtime.ledger.get_work("run-2")
    assert row["status"] == "succeeded"
    assert loads(row["result"]) == 8
