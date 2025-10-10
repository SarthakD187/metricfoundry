from __future__ import annotations
from typing import Any, Dict, Mapping, MutableMapping
from .constants import PHASE_ORDER

def _with_phase(state: MutableMapping[str, Any], phase: str, payload: Dict[str, Any], **extra: Any) -> Dict[str, Any]:
    phases = dict(state.get("phase_outputs", {}))
    phases[phase] = payload
    update: Dict[str, Any] = {"phase_outputs": phases}
    update.update(extra)
    return update

def _emit_callback(state: Mapping[str, Any], phase: str, payload: Mapping[str, Any]) -> None:
    callback = state.get("_callback")
    if not callable(callback):
        return
    index = PHASE_ORDER.index(phase)
    callback(phase, payload, index, len(PHASE_ORDER))
