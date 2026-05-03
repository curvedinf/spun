import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Iterable, List


DEFAULT_CODE_FILES = ("spun.py", "calls.py", "schedule.py")


def load_module(path: Path) -> ModuleType:
    module_name = f"_spun_project_{path.stem}_{abs(hash(str(path)))}"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load Spun project file: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def discover(project_dir: Path, files: Iterable[str] = DEFAULT_CODE_FILES) -> List[Path]:
    loaded: List[Path] = []
    project_path = str(project_dir)
    if project_path not in sys.path:
        sys.path.insert(0, project_path)

    for filename in files:
        path = project_dir / filename
        if not path.exists():
            continue
        load_module(path)
        loaded.append(path)
    return loaded
