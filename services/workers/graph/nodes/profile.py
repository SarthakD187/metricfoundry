from __future__ import annotations
from typing import Any, Dict, MutableMapping, List
from ..core.types import DatasetSummary
from ..core.state import _with_phase, _emit_callback

def profile_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    profiles = [dataset.column(name).to_profile(dataset.row_count) for name in dataset.column_names]
    completeness = 0.0
    if profiles and dataset.row_count:
        completeness = sum(1.0 - p["nullRatio"] for p in profiles) / len(profiles)

    payload = {"columnProfiles": profiles, "datasetCompleteness": completeness}
    update = _with_phase(state, "profile", payload, dataset=dataset)
    _emit_callback(state, "profile", payload)
    return update
