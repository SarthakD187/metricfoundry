from __future__ import annotations
from typing import Any, Dict, MutableMapping, Optional

from ..core.state import _with_phase, _emit_callback
from ..io.ingest import ingest_dataset
from ..core.types import BinaryInput

def ingest_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    source = state.get("source", {}) or {}
    key = source.get("key", "dataset.csv")

    body_input = state.get("raw_input")
    if body_input is None:
        body_input = state.get("body")

    dataset_input: BinaryInput
    if body_input is None:
        dataset_input = b""
    else:
        if isinstance(body_input, bytearray):
            dataset_input = bytes(body_input)
        else:
            if hasattr(body_input, "seek"):
                try:
                    body_input.seek(0)
                except Exception:
                    pass
            dataset_input = body_input

    dataset = ingest_dataset(key, dataset_input)

    closer = getattr(body_input, "close", None)
    if callable(closer):
        try:
            closer()
        except Exception:
            pass

    payload = {
        "rows": dataset.row_count,
        "columns": dataset.column_names,
        "bytesRead": dataset.bytes_read,
        "sourceFormat": dataset.source_format,
        "preview": dataset.preview_rows,
    }

    update = _with_phase(state, "ingest", payload, dataset=dataset, raw_input=None, body=None)
    _emit_callback(state, "ingest", payload)
    return update
