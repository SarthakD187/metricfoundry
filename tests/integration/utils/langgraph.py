"""Helpers for working with the LangGraph dependency during tests."""
from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import List


__all__ = ["ensure_langgraph_stub"]


def ensure_langgraph_stub() -> None:
    """Provide a very small stub of ``langgraph`` when the dependency is absent."""

    try:
        importlib.import_module("langgraph.graph")
        return
    except Exception:  # pragma: no cover - stub fallback path
        pass

    pkg = ModuleType("langgraph")
    mod = ModuleType("langgraph.graph")

    END = object()

    class _CompiledGraph:
        def __init__(self, nodes, order):
            self._nodes = nodes
            self._order = order

        def invoke(self, initial_state):
            state = dict(initial_state)
            for name in self._order:
                update = self._nodes[name](state)
                if update:
                    state.update(update)
            return state

    class StateGraph:
        def __init__(self, _state_type):
            self._nodes = {}
            self._edges = {}
            self._entry = None

        def add_node(self, name, func):
            self._nodes[name] = func

        def set_entry_point(self, name):
            self._entry = name

        def add_edge(self, source, dest):
            self._edges.setdefault(source, []).append(dest)

        def compile(self):
            order: List[str] = []
            current = self._entry
            visited: set[str] = set()
            while current is not None and current != END:
                if current in visited:
                    raise RuntimeError("cycle detected in stub graph")
                visited.add(current)
                order.append(current)
                next_nodes = self._edges.get(current, [])
                current = next_nodes[0] if next_nodes else None
            return _CompiledGraph(self._nodes, order)

    mod.END = END
    mod.StateGraph = StateGraph
    pkg.graph = mod
    sys.modules["langgraph"] = pkg
    sys.modules["langgraph.graph"] = mod
