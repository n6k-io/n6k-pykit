"""``@action``: a JSON-RPC method on a page's socket, also served as an MCP tool.

An action belongs to no ``Database``. It is served by the pages whose data module
binds it (defined or imported), and runs inside ``bind_catalogs`` so the members it
imports resolve against the databases that page serves. Injected params, hidden from
callers: ``Annotated[Session, db]`` and ``Annotated[<native Connection>, db]``; a bare
``Session`` / ``Connection`` means the page's only database.
"""

import asyncio
import functools
import inspect
import typing
from contextlib import ExitStack
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Any, Awaitable, Callable, overload

from n6k_app.drivers.base import driver_for_annotation
from n6k_app.members import bind_catalogs
from n6k_app.session_state import Session

if TYPE_CHECKING:
    from n6k_app.database import Database


@dataclass(frozen=True)
class Action:
    name: str
    fn: Callable[..., Any]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)


@overload
def action(name: Callable[..., Any], /) -> Action: ...


@overload
def action(name: str | None = None, /) -> Callable[[Callable[..., Any]], Action]: ...


def action(name: Any = None, /) -> Any:
    """Register ``fn`` as an action named ``name`` (default ``fn.__name__``).

    Its docstring and type hints are the MCP tool's description and input schema. A
    sync action runs off the event loop."""
    if callable(name):
        return Action(name.__name__, name)

    def decorator(fn: Callable[..., Any]) -> Action:
        return Action(name if name is not None else fn.__name__, fn)

    return decorator


def page_actions(module: ModuleType) -> list[Action]:
    """The ``Action``s a data module binds at top level (defined or imported)."""
    seen: set[int] = set()
    out: list[Action] = []
    for obj in vars(module).values():
        if isinstance(obj, Action) and id(obj) not in seen:
            seen.add(id(obj))
            out.append(obj)
    return out


@dataclass(frozen=True)
class _Injected:
    param: str
    alias: str
    native_db_type: str | None  # None: the alias's Session


def _target(act: Action, param: str, meta: tuple[Any, ...], databases: dict[str, "Database"]) -> str:
    """The alias an injected param binds to: the Database named in its ``Annotated``
    metadata, else the page's only database."""
    from n6k_app.database import Database

    named = [m for m in meta if isinstance(m, Database)]
    if len(named) > 1:
        raise TypeError(f"action '{act.name}' param '{param}' names more than one Database.")
    if named:
        for alias, db in databases.items():
            if db is named[0]:
                return alias
        known = ", ".join(databases) or "none"
        raise TypeError(
            f"action '{act.name}' param '{param}' uses a Database this page does not serve "
            f"(this page serves: {known}). A page serves the databases its own data.py names."
        )
    if len(databases) != 1:
        known = ", ".join(databases) or "none"
        raise TypeError(
            f"action '{act.name}' param '{param}' does not say which database it is for, and this "
            f"page serves {len(databases)} ({known}). Use Annotated[..., <db>]."
        )
    return next(iter(databases))


def _injections(act: Action, databases: dict[str, "Database"]) -> list[_Injected]:
    hints = typing.get_type_hints(act.fn, include_extras=True)
    out: list[_Injected] = []
    for param in inspect.signature(act.fn).parameters:
        hint = hints.get(param)
        meta: tuple[Any, ...] = ()
        if typing.get_origin(hint) is typing.Annotated:
            hint, *rest = typing.get_args(hint)
            meta = tuple(rest)
        if hint is Session:
            out.append(_Injected(param, _target(act, param, meta, databases), None))
        elif (driver := driver_for_annotation(hint)) is not None:
            out.append(_Injected(param, _target(act, param, meta, databases), driver.db_type))
    return out


def action_handler(
    act: Action, databases: dict[str, "Database"], sessions: dict[str, Session]
) -> Callable[..., Awaitable[Any]]:
    """``act`` as an async callable taking only its public params, bound to one
    connection's ``sessions``."""
    injected = _injections(act, databases)
    hidden = {i.param for i in injected}
    full = inspect.signature(act.fn)
    public = full.replace(parameters=[p for name, p in full.parameters.items() if name not in hidden])
    is_async = inspect.iscoroutinefunction(act.fn)

    def inject(kwargs: dict[str, Any], stack: ExitStack) -> dict[str, Any]:
        for i in injected:
            if i.native_db_type is None:
                kwargs[i.param] = sessions[i.alias]
            else:
                pool = databases[i.alias].connection_pool
                kwargs[i.param] = stack.enter_context(pool.native_checkout(i.native_db_type))
        return kwargs

    def call_sync(kwargs: dict[str, Any]) -> Any:
        with bind_catalogs(databases), ExitStack() as stack:
            return act.fn(**inject(kwargs, stack))

    @functools.wraps(act.fn)
    async def fn(*args: Any, **kwargs: Any) -> Any:
        bound = dict(public.bind(*args, **kwargs).arguments)
        if not is_async:
            return await asyncio.to_thread(call_sync, bound)
        with bind_catalogs(databases), ExitStack() as stack:
            return await act.fn(**inject(bound, stack))

    setattr(fn, "__signature__", public)
    return fn
