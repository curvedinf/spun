import os

import pytest

from spun import CallPromise, Worker, call, config
from spun.errors import UnknownCallError
from spun.runtime import runtime
from spun.serialization import dumps


def test_call_decorator_registers_project_function(tmp_path):
    (tmp_path / "calls.py").write_text(
        "from spun import call\n"
        "@call\n"
        "def answer():\n"
        "    return 42\n"
    )
    config(project_dir=str(tmp_path), discover=True, reset=True)

    run = call.answer()
    assert isinstance(run, CallPromise)
    assert run.status == "queued"
    Worker(project_dir=str(tmp_path), poll_interval=0.01).run_until_idle()

    assert run.result() == 42
    assert run.status == "succeeded"
    assert call.names() == ["answer"]


def test_unknown_call_raises_clear_error():
    call.clear()

    with pytest.raises(UnknownCallError, match="not defined"):
        call.resolve("missing")


def test_project_discovery_loads_env_and_calls(tmp_path):
    (tmp_path / ".env").write_text("SPUN_TEST_VALUE=from_file\n")
    (tmp_path / "calls.py").write_text(
        "import os\n"
        "from spun import call\n"
        "@call\n"
        "def value():\n"
        "    return os.environ['SPUN_TEST_VALUE']\n"
    )

    os.environ["SPUN_TEST_VALUE"] = "from_env"
    config(project_dir=str(tmp_path), discover=True, reset=True)

    assert os.environ["SPUN_TEST_VALUE"] == "from_file"
    run = call.value()
    Worker(project_dir=str(tmp_path), poll_interval=0.01).run_until_idle()
    assert run.result() == "from_file"


def test_call_scope_recovers_unacked_orphan(tmp_path):
    (tmp_path / "calls.py").write_text(
        "from spun import call\n"
        "@call\n"
        "def double(value):\n"
        "    return value * 2\n"
    )
    config(project_dir=str(tmp_path), discover=True, reset=True)

    with call.scope("request-1"):
        run = call.double(21)

    Worker(project_dir=str(tmp_path), poll_interval=0.01).run_until_idle()

    recovered = call.recover(scope="request-1", name="double", args=(21,))
    assert [item.id for item in recovered] == [run.id]
    assert recovered[0].status == "succeeded"
    assert recovered[0].result() == 42
    assert call.recover(scope="request-1", name="double", args=(21,)) == []


def test_call_scope_and_key_deduplicate_unacked_invocation(tmp_path):
    (tmp_path / "calls.py").write_text(
        "from spun import call\n"
        "@call\n"
        "def value():\n"
        "    return 'ok'\n"
    )
    config(project_dir=str(tmp_path), discover=True, reset=True)

    with call.scope("request-2"):
        first = call.value(_spun_key="same")
        second = call.value(_spun_key="same")

    assert first.id == second.id
    assert len(runtime.ledger.list_orphans(scope="request-2", name="value", key="same")) == 1


def test_orphan_queue_expires_short_lived_entries(tmp_path):
    config(project_dir=str(tmp_path), discover=False, reset=True)
    runtime.ledger.submit_call(
        call_name="short",
        payload=dumps({"type": "spun_call", "name": "short", "args": (), "kwargs": {}}),
        return_scope="request-3",
        args_hash="hash",
        orphan_ttl_seconds=0,
    )

    assert runtime.ledger.list_orphans(scope="request-3") == []
