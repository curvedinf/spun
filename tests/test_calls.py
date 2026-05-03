import os

import pytest

from spun import call, config
from spun.errors import UnknownCallError


def test_call_decorator_registers_project_function():
    call.clear()

    @call
    def answer():
        return 42

    assert call.answer() == 42
    assert call.names() == ["answer"]


def test_unknown_call_raises_clear_error():
    call.clear()

    with pytest.raises(UnknownCallError, match="not defined"):
        call.missing()


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
    assert call.value() == "from_file"
