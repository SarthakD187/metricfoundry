from __future__ import annotations
import io, json, zipfile, mimetypes
from typing import Any, Dict, List, Mapping, MutableMapping, Optional
from ..core.state import _with_phase, _emit_callback
from ..core.constants import PHASE_ORDER
import csv
import io
import json
import base64
from typing import Any, Mapping


def finalize_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    phases: Dict[str, Dict[str, Any]] = state.get("phase_outputs", {})
    dq = phases.get("dq_validate", {})
    stats_phase = phases.get("descriptive_stats", {})

    metrics = {
        "rows": dataset.row_count,
        "columns": len(dataset.column_names),
        "bytesRead": dataset.bytes_read,
        "datasetCompleteness": phases.get("profile", {}).get("datasetCompleteness"),
        "dqScore": dq.get("score"),
    }

    dq_severity_summary = dq.get("severitySummary")
    dq_metrics_payload = dq.get("metrics")
    if dq_severity_summary:
        metrics["dqSeveritySummary"] = dq_severity_summary
    if dq_metrics_payload:
        metrics["dqMetrics"] = dq_metrics_payload

    correlations = stats_phase.get("correlations", [])
    outliers = stats_phase.get("outliers", [])

    ml_phase = phases.get("ml_inference")
    if isinstance(ml_phase, Mapping):
        ml_inference_payload = dict(ml_phase)
    else:
        ml_inference_payload = {
            "status": "skipped",
            "message": "Automated modeling did not execute for this dataset.",
        }

    artifact_prefix: str = state.get("artifact_prefix", "artifacts")
    existing_artifacts: Dict[str, Dict[str, Any]] = dict(state.get("artifact_contents", {}))
    bundle_artifacts = _build_curated_bundles(phases, existing_artifacts)
    artifact_contents = dict(existing_artifacts)
    artifact_contents.update(bundle_artifacts)

    manifest_entries: List[Dict[str, Any]] = []
    for phase in PHASE_ORDER:
        if phase not in phases:
            continue
        manifest_entries.append(
            {
                "name": f"{phase}_json",
                "description": f"Serialized output for the {phase} phase.",
                "contentType": "application/json",
                "key": f"{artifact_prefix}/phases/{phase}.json",
            }
        )

    manifest_entries.extend(_manifest_entries_for_artifacts(artifact_contents, artifact_prefix))

    manifest_entries.append(
        {
            "name": "results_json",
            "description": "Consolidated analytics results payload.",
            "contentType": "application/json",
            "key": f"{artifact_prefix}/results/results.json",
        }
    )
    manifest_entries.append(
        {
            "name": "results_manifest",
            "description": "Manifest describing generated analytics artifacts.",
            "contentType": "application/json",
            "key": f"{artifact_prefix}/results/manifest.json",
        }
    )

    manifest = {
        "jobId": state.get("job_id"),
        "basePath": artifact_prefix + "/",
        "artifacts": manifest_entries,
    }

    data_quality_manifest: Dict[str, Any] = {}
    if dq_severity_summary:
        data_quality_manifest["severitySummary"] = dq_severity_summary
    if dq_metrics_payload:
        data_quality_manifest["metrics"] = dq_metrics_payload
    if data_quality_manifest:
        manifest["dataQuality"] = data_quality_manifest

    payload = {
        "metrics": metrics,
        "manifest": manifest,
        "mlInference": ml_inference_payload,
        "correlations": correlations,
        "outliers": outliers,
    }


    dataset = state["dataset"]
    update = _with_phase(
        state,
        "finalize",
        payload,
        manifest=manifest,
        artifact_contents=artifact_contents,
        final_summary=payload,   # <-- IMPORTANT: now a dict, not a set
        dataset=dataset,
    )
    _emit_callback(state, "finalize", payload)
    return update

def _artifact_bytes_for_bundle(spec: dict) -> bytes:
    """
    Build bytes for a bundle specification.

    Supported:
      - type='csv': spec['rows'] (list[dict]), optional spec['headers']
      - type in {'png','image','image/png','binary'} with spec['bytes'] or spec['body'] (bytes)
      - type='text' with spec['text'] (str)
      - type='json' with spec['obj'] (any json-serializable)
    Fallback: JSON-encode the spec; any bytes inside are base64-wrapped.
    """
    t = (spec.get("type") or "").lower()

    # --- CSV ---
    if t == "csv":
        rows = spec.get("rows") or []
        headers = spec.get("headers")
        if not headers:
            keyset = set()
            for r in rows:
                if isinstance(r, Mapping):
                    keyset.update(r.keys())
            headers = sorted(keyset)

        buf = io.StringIO(newline="")
        writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()

        def _stringify_cell(v: Any) -> str:
            try:
                import math
                if v is None:
                    return ""
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    return ""
            except Exception:
                pass
            return str(v)

        for r in rows:
            safe = {h: _stringify_cell(r.get(h)) for h in headers}
            writer.writerow(safe)
        return buf.getvalue().encode("utf-8")

    # --- Raw binary/image bytes ---
    if t in {"png", "image", "image/png", "binary"}:
        body = spec.get("bytes", None)
        if body is None:
            body = spec.get("body", None)
        if isinstance(body, (bytes, bytearray, memoryview)):
            return bytes(body)

    # --- Plain text ---
    if t == "text":
        return (spec.get("text") or "").encode("utf-8")

    # --- JSON explicit object ---
    if t == "json":
        return json.dumps(spec.get("obj"), ensure_ascii=False, default=_json_default).encode("utf-8")

    # --- Fallback: JSON-encode the spec robustly (bytes -> base64 wrapper) ---
    return json.dumps(spec, ensure_ascii=False, default=_json_default).encode("utf-8")


def _json_default(o: Any):
    """JSON fallback: encode bytes as base64 so dumps never crashes."""
    if isinstance(o, (bytes, bytearray, memoryview)):
        return {"__b64__": True, "data": base64.b64encode(bytes(o)).decode("ascii")}
    # Add numpy/pandas special cases here if needed.
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")



def _zip_bytes_for(files: Mapping[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path, data in files.items():
            archive.writestr(path, data)
    buffer.seek(0)
    return buffer.getvalue()


def _is_visualization_artifact(relative_key: str, spec: Mapping[str, Any]) -> bool:
    if relative_key.startswith("results/graphs/"):
        return True

    bundle_hint = spec.get("bundle")
    if isinstance(bundle_hint, str) and bundle_hint.lower() == "visualization":
        return True

    content_type = spec.get("contentType")
    if isinstance(content_type, str) and content_type.lower().startswith("image/"):
        return True

    kind = spec.get("kind")
    if isinstance(kind, str) and kind.lower() == "image":
        return True

    return False


def _guess_content_type_for_artifact(relative_key: str, spec: Mapping[str, Any]) -> str:
    content_type = spec.get("contentType")
    if isinstance(content_type, str) and content_type:
        return content_type

    kind = spec.get("kind")
    if kind == "html":
        return "text/html"
    if kind == "text":
        return "text/plain"
    if kind == "json":
        return "application/json"
    if kind == "csv":
        return "text/csv"
    if kind == "image":
        return "image/png"

    guessed, _ = mimetypes.guess_type(relative_key)
    if guessed:
        return guessed

    return "application/octet-stream"


def _default_description_for_artifact(relative_key: str, spec: Mapping[str, Any]) -> str:
    if _is_visualization_artifact(relative_key, spec):
        return "Visualization asset generated during dataset analysis."
    if relative_key.endswith(".html"):
        return "HTML artifact generated during dataset analysis."
    if relative_key.endswith(".txt"):
        return "Text artifact generated during dataset analysis."
    if relative_key.endswith(".csv"):
        return "Tabular artifact generated during dataset analysis."
    if relative_key.endswith(".json"):
        return "JSON artifact generated during dataset analysis."
    return "Generated artifact from the analytics pipeline."


def _manifest_entries_for_artifacts(
    artifact_contents: Mapping[str, Mapping[str, Any]],
    artifact_prefix: str,
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for relative_key in sorted(artifact_contents.keys()):
        spec = artifact_contents[relative_key]
        description = spec.get("description")
        if not isinstance(description, str) or not description.strip():
            description = _default_description_for_artifact(relative_key, spec)
        content_type = _guess_content_type_for_artifact(relative_key, spec)
        entries.append(
            {
                "name": relative_key.replace("/", "_"),
                "description": description,
                "contentType": content_type,
                "key": f"{artifact_prefix}/{relative_key}",
            }
        )
    return entries


def _build_curated_bundles(
    phases: Mapping[str, Mapping[str, Any]],
    artifact_contents: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    bundles: Dict[str, Dict[str, Any]] = {}

    summary_files: Dict[str, bytes] = {}
    visualization_files: Dict[str, bytes] = {}

    for relative_key, spec in artifact_contents.items():
        if relative_key.startswith("results/bundles/"):
            continue
        body = _artifact_bytes_for_bundle(spec)
        if body is None:
            continue
        if relative_key.startswith("results/"):
            trimmed = relative_key[len("results/"):]
            if _is_visualization_artifact(relative_key, spec):
                visualization_files[trimmed] = body
            else:
                summary_files[trimmed] = body

    if summary_files:
        bundles["results/bundles/analytics_bundle.zip"] = {
            "kind": "binary",
            "data": _zip_bytes_for(summary_files),
            "description": (
                "Curated archive containing descriptive statistics, correlations, "
                "outliers, reports, and other tabular result artifacts."
            ),
            "contentType": "application/zip",
        }

    if visualization_files:
        bundles["results/bundles/visualizations.zip"] = {
            "kind": "binary",
            "data": _zip_bytes_for(visualization_files),
            "description": (
                "PNG visualisations generated during analysis, grouped for quick download."
            ),
            "contentType": "application/zip",
        }

    phase_payloads: Dict[str, bytes] = {}
    for phase_name, payload in phases.items():
        phase_payloads[f"{phase_name}.json"] = json.dumps(payload, indent=2, default=str).encode("utf-8")

    if phase_payloads:
        bundles["phases/phase_payloads.zip"] = {
            "kind": "binary",
            "data": _zip_bytes_for(phase_payloads),
            "description": "Combined JSON payloads emitted by each pipeline phase.",
            "contentType": "application/zip",
        }

    return bundles