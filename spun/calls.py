from typing import Any, Callable, Dict, List

from .errors import UnknownCallError


class CallNamespace:
    """
    Decorator and namespace for worker-space project calls.
    """

    def __init__(self) -> None:
        self._definitions: Dict[str, Callable[..., Any]] = {}

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        self._definitions[func.__name__] = func
        return func

    def __getattr__(self, name: str) -> Callable[..., Any]:
        if name.startswith("_"):
            raise AttributeError(name)

        def invoke(*args: Any, **kwargs: Any) -> Any:
            try:
                func = self._definitions[name]
            except KeyError as exc:
                raise UnknownCallError(
                    f"Spun call '{name}' is not defined in loaded calls.py."
                ) from exc
            return func(*args, **kwargs)

        invoke.__name__ = name
        invoke.__qualname__ = f"call.{name}"
        return invoke

    def clear(self) -> None:
        self._definitions.clear()

    def names(self) -> List[str]:
        return sorted(self._definitions)

    def defined(self, name: str) -> bool:
        return name in self._definitions


call = CallNamespace()
