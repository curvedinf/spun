from contextlib import contextmanager
from contextvars import ContextVar
from hashlib import sha256
from typing import Any, Callable, Dict, Iterator, List, Optional

from .errors import UnknownCallError
from .serialization import dumps


_return_scope: ContextVar[Optional[str]] = ContextVar("spun_return_scope", default=None)


def _args_hash(args: tuple, kwargs: Dict[str, Any]) -> str:
    return sha256(dumps((args, sorted(kwargs.items())))).hexdigest()


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
            from .promise import CallPromise

            scope = kwargs.pop("_spun_scope", None) or _return_scope.get()
            key = kwargs.pop("_spun_key", None)
            return CallPromise.submit(
                name=name,
                args=args,
                kwargs=kwargs,
                args_hash=_args_hash(args, kwargs),
                scope=scope,
                key=key,
            )

        invoke.__name__ = name
        invoke.__qualname__ = f"call.{name}"
        return invoke

    def resolve(self, name: str) -> Callable[..., Any]:
        try:
            return self._definitions[name]
        except KeyError as exc:
            raise UnknownCallError(
                f"Spun call '{name}' is not defined in loaded calls.py."
            ) from exc

    def get(self, call_id: str) -> Any:
        from .promise import CallPromise

        return CallPromise(call_id)

    def recover(
        self,
        *,
        scope: Optional[str] = None,
        name: Optional[str] = None,
        key: Optional[str] = None,
        args: Optional[tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        limit: int = 50,
    ) -> List[Any]:
        from .promise import CallPromise
        from .runtime import runtime

        runtime.ledger.ensure_schema()
        args_hash = None
        if args is not None or kwargs is not None:
            args_hash = _args_hash(args or (), kwargs or {})
        rows = runtime.ledger.list_orphans(
            scope=scope,
            name=name,
            key=key,
            args_hash=args_hash,
            limit=limit,
        )
        return [CallPromise(row["work_id"]).claim() for row in rows]

    orphans = recover

    @contextmanager
    def scope(self, value: str) -> Iterator[None]:
        token = _return_scope.set(value)
        try:
            yield
        finally:
            _return_scope.reset(token)

    def clear(self) -> None:
        self._definitions.clear()

    def names(self) -> List[str]:
        return sorted(self._definitions)

    def defined(self, name: str) -> bool:
        return name in self._definitions


call = CallNamespace()
