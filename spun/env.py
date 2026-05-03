from pathlib import Path
from typing import Dict
import os


def parse_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def load_env(project_dir: Path) -> Dict[str, str]:
    """
    Load .env values into process environment. The file wins over existing env.
    """

    values = parse_env_file(project_dir / ".env")
    for key, value in values.items():
        os.environ[key] = value
    return values
