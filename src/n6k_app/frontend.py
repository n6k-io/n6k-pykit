"""Author-facing page API.

A `page.py` builds its UI imperatively against an injected builder `c`, and reads
data by importing the relations its `data.py` declares:

    from .data import by_month

    @page
    def revenue(c: Components):
        n = c.Slider(min=0, max=100, default=25, key="n")   # key= → live control
        c.BarChart(query=by_month.ref(year=2015), x="month", y="revenue")

`c` is the only thing a page() takes. Everything else comes in by import, so a
page's data surface is checkable by an editor — see `n6k_app.members`.

`page()` runs server-side (plain sync Python); each connection gets its own `c`,
so there is no shared render state.
"""

import inspect
import textwrap
from typing import Any, Callable, overload

import mistune

from n6k_app.database import Database
from n6k_app.members import Ref, bind_catalogs

__all__ = [
    "Components",
    "Page",
    "Ref",
    "page",
]


class _Factory:
    """``c.<Name>``: call it to append a node, or reach through it for a namespaced
    component (``c.Static.Metric`` emits ``"Static.Metric"``, resolved client-side)."""

    def __init__(self, owner: "Components", name: str):
        self._owner = owner
        self._name = name

    def __getattr__(self, member: str) -> "_Factory":
        # Dunder lookups (copy/pickle introspection) are never components.
        if member.startswith("__"):
            raise AttributeError(member)
        return _Factory(self._owner, f"{self._name}.{member}")

    def __call__(self, *children: Any, key: str | None = None, default: Any = None, **props: Any) -> Any:
        owner = self._owner
        if key is not None:
            value = owner._state.get(key, default)
            owner._children.append(
                {"component": self._name, "props": {**props, "value": value, "bind": key}, "children": []}
            )
            return value
        props = {k: (v.sql if isinstance(v, Ref) else v) for k, v in props.items()}
        node: dict[str, Any] = {"component": self._name, "props": props, "children": list(children)}
        owner._children.append(node)
        return Components(owner._state, node["children"])


# mistune v3 inline/leaf token type -> HTML tag; block containers handled below.
_MD_INLINE = {"strong": "strong", "emphasis": "em"}
_MD_BLOCK = {"paragraph": "p", "block_quote": "blockquote", "list_item": "li"}
_md_parser: Any = mistune.create_markdown(renderer=None)


def _md_children(token: dict[str, Any]) -> list[Any]:
    """Flatten a token's children into node dicts / bare strings (text runs)."""
    out: list[Any] = []
    for child in token.get("children", []) or []:
        node = _md_convert(child)
        if node is None:
            continue
        out.extend(node) if isinstance(node, list) else out.append(node)
    return out


def _md_convert(token: dict[str, Any]) -> Any:
    """One mistune AST token -> a node dict, a string, a list of nodes, or None."""
    kind = token["type"]
    attrs = token.get("attrs", {})
    if kind == "text":
        return token.get("raw", "")
    if kind == "softbreak":
        return " "
    if kind == "blank_line":
        return None
    if kind == "linebreak":
        return {"component": "br", "props": {}, "children": []}
    if kind == "codespan":
        return {"component": "code", "props": {}, "children": [token.get("raw", "")]}
    if kind == "link":
        return {"component": "a", "props": {"href": attrs.get("url", "")}, "children": _md_children(token)}
    if kind == "block_text":  # a tight list item's inline run — no <p> wrapper
        return _md_children(token)
    if kind == "heading":
        return {"component": f"h{attrs.get('level', 1)}", "props": {}, "children": _md_children(token)}
    if kind == "list":
        tag = "ol" if attrs.get("ordered") else "ul"
        return {"component": tag, "props": {}, "children": _md_children(token)}
    if kind == "block_code":
        code = {"component": "code", "props": {}, "children": [token.get("raw", "")]}
        return {"component": "pre", "props": {}, "children": [code]}
    if kind == "thematic_break":
        return {"component": "hr", "props": {}, "children": []}
    if kind in _MD_INLINE:
        return {"component": _MD_INLINE[kind], "props": {}, "children": _md_children(token)}
    if kind in _MD_BLOCK:
        return {"component": _MD_BLOCK[kind], "props": {}, "children": _md_children(token)}
    return token["raw"] if "raw" in token else _md_children(token)


def _md_to_nodes(text: str) -> list[Any]:
    """Parse a markdown string into top-level n6k nodes (server-side, once)."""
    out: list[Any] = []
    for token in _md_parser(textwrap.dedent(text).strip()):
        node = _md_convert(token)
        if node is None:
            continue
        out.extend(node) if isinstance(node, list) else out.append(node)
    return out


class Components:
    """The page builder, injected as `def page(c: Components): …`.

    Fully generic: every attribute is a callable that appends a `{component: <name>}`
    node to this builder's list (names resolve client-side against the MDX
    vocabulary), so `c.anything(...)` just works with no code generation.

    - A call with `key=` is an interactive control: emits a `bind` and returns the
      current value from state, so it feeds imperative Python (`if n > 50: …`).
    - Any other call is a display node returning a child `Components`, so it nests:

          with c.Grid(columns=2) as g:
              g.Metric(label="Rows", value=n)

    A generated `components.pyi` is purely additive typing — delete it and
    everything still runs, just untyped on the dynamic parts.
    """

    def __init__(self, state: dict[Any, Any], children: list[Any]):
        self._state = state
        self._children = children

    def __enter__(self) -> "Components":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def markdown(self, text: str) -> "Components":
        """Append markdown as native nodes (the tags MDX emits), parsed here so the
        browser needs no markdown parser. Other calls take text literally.
        """
        self._children.extend(_md_to_nodes(text))
        return self

    def __getattr__(self, name: str) -> _Factory:
        return _Factory(self, name)


class Page:
    """A `@page`-decorated function plus its metadata; `render` runs the body
    against a state dict and returns the component tree."""

    def __init__(self, func: Any, meta: dict[str, Any]):
        self.func = func
        self.meta = meta

    def render(self, state: dict[str, Any], catalogs: dict[str, "Database"] | None = None) -> dict[str, Any]:
        # `Components` is the only injectable parameter; members imported from
        # data.py resolve their catalog alias out of `catalogs`.
        root = Components(state or {}, [])
        args: dict[str, Any] = {}
        for p in inspect.signature(self.func).parameters.values():
            ann = p.annotation
            if inspect.isclass(ann) and issubclass(ann, Components):
                args[p.name] = root
            else:
                raise TypeError(
                    f"page {self.func.__name__}() declares a parameter {p.name!r} that n6k has "
                    f"nothing to pass. A page takes only its Components builder "
                    f"(`def {self.func.__name__}(c: Components)`); to read data, import the "
                    f"relation from data.py — `from .data import {p.name}` — and use "
                    f"{p.name}.ref()."
                )
        with bind_catalogs(catalogs or {}):
            self.func(**args)
        return {"component": "fragment", "props": {}, "children": root._children}


@overload
def page(func: Callable[..., object]) -> Page: ...


@overload
def page(func: None = ..., **meta: Any) -> Callable[[Callable[..., object]], Page]: ...


def page(func: Callable[..., object] | None = None, **meta: Any) -> Page | Callable[[Callable[..., object]], Page]:
    """Mark a function as a page. Usable bare (`@page`) or called (`@page(...)`).
    Keyword arguments are accepted and ignored."""
    if func is not None:
        return Page(func, meta)

    def deco(f: Callable[..., object]) -> Page:
        return Page(f, meta)

    return deco
