from __future__ import annotations
from typing import Any, Dict, MutableMapping, Optional, Union, IO
from ..core.state import _with_phase, _emit_callback
from ..io.ingest import ingest_dataset

def ingest_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    source = state.get("source", {}) or {}
    key = source.get("key", "dataset.csv")

    body_input = state.get("raw_input")
    if body_input is None:
        body_input = state.get("body")

    if body_input is None:
        data_bytes = b""
    elif isinstance(body_input, (bytes, bytearray)):
        data_bytes = bytes(body_input)
    else:
        try:
            body_input.seek(0)
        except Exception:
            pass
        data_bytes = body_input.read()

    dataset = ingest_dataset(key, data_bytes)

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
