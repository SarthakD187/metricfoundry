from __future__ import annotations
from typing import Any, Dict, List, Mapping, MutableMapping
import html
import logging
from pathlib import Path

from ..core.types import DatasetSummary
from ..core.state import _with_phase, _emit_callback
from ..core.constants import _NL_REPORT_TEMPLATE_NAME
from ..core.utils import _JINJA_ENV

logger = logging.getLogger(__name__)


def nl_report_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    """
    Build a natural-language and HTML narrative report from prior phases and
    attach both in-memory artifacts and (if artifact_prefix is provided) write
    the report files to disk.
    """
    dataset: DatasetSummary = state["dataset"]
    phases = state.get("phase_outputs", {})
    dq = phases.get("dq_validate", {}) or {}
    stats_phase = phases.get("descriptive_stats", {}) or {}

    # Basic dataset summary
    row_text = (
        f"{dataset.row_count:,} rows" if getattr(dataset, "row_count", None) else "an unknown number of rows"
    )
    column_text = f"{len(dataset.column_names)} columns"
    dq_score = dq.get("score")
    dq_text = f"Data quality score {dq_score:.2f}" if dq_score is not None else "Data quality checks executed"

    # Highlights from correlations / outliers
    highlights: List[str] = []
    correlations = stats_phase.get("correlations", []) or []
    if correlations:
        top = correlations[0]
        try:
            highlights.append(
                f"Strongest correlation observed between {top.get('left')} and {top.get('right')} (r={float(top.get('correlation', 0.0)):.2f})."
            )
        except Exception:
            pass

    outliers = stats_phase.get("outliers", []) or []
    if outliers:
        sample_outlier = outliers[0]
        vals = (sample_outlier.get("values") or [])[:2]
        try:
            formatted = ", ".join(f"{float(v.get('value', 0.0)):.2f}" for v in vals)
            col = sample_outlier.get("column")
            highlights.append(f"Potential outliers detected in {col}: {formatted}.")
        except Exception:
            pass

    summary_lines = [
        f"The dataset contains {row_text} across {column_text}.",
        dq_text + ".",
    ]
    summary_lines.extend(highlights)
    summary_lines.append("Further modeling pipelines can build on these profiling insights.")
    summary_text = " ".join(summary_lines)

    # DQ issues (cap to 20 for brevity)
    dq_issues = []
    for issue in list(dq.get("issues", []))[:20]:
        dq_issues.append(
            {
                "rule": issue.get("rule"),
                "column": issue.get("column"),
                "message": issue.get("message"),
                "severity": issue.get("severity"),
            }
        )

    dq_context = {
        "score": dq_score,
        # The template expects dq.severity_summary and iterates .items()
        "severity_summary": dict(dq.get("severitySummary", {})) or None,
        "issues": dq_issues,
    }

    # Correlation preview table (cap to 10)
    correlation_preview = [
        {
            "left": item.get("left"),
            "right": item.get("right"),
            "correlation": item.get("correlation", 0.0),
        }
        for item in correlations[:10]
    ]

    # Outlier preview (cap entries & values)
    outlier_preview: List[Dict[str, Any]] = []
    for entry in outliers[:5]:
        values = [
            {
                "value": value.get("value"),
                "zscore": value.get("zscore"),
            }
            for value in (entry.get("values") or [])[:5]
        ]
        outlier_preview.append(
            {
                "column": entry.get("column"),
                "values": values,
            }
        )

    # HTML rendering context for Jinja
    html_context = {
        "summary_text": summary_text,
        "highlights": highlights,
        "dataset": {"row_text": row_text, "column_text": column_text},
        "dq": dq_context,
        "correlations": correlation_preview,
        "outliers": outlier_preview,
    }

    # Render HTML (with safe fallback)
    escaped_summary = html.escape(summary_text)
    html_report = f"<html><body><p>{escaped_summary}</p></body></html>"
    if _JINJA_ENV is not None:
        try:
            template = _JINJA_ENV.get_template(_NL_REPORT_TEMPLATE_NAME)
            html_report = template.render(html_context)
        except Exception as e:
            logger.exception("failed to render HTML narrative report: %s", e)
            # leave the minimal fallback in html_report

    # In-memory artifacts for downstream consumption / finalize
    payload = {
        "summary": summary_text,
        "highlights": highlights,
        "reportArtifacts": {
            "text": "results/report.txt",
            "html": "results/report.html",
        },
    }

    artifact_contents = dict(state.get("artifact_contents", {}))
    artifact_contents["results/report.txt"] = {
        "kind": "text",
        "text": summary_text,
        "description": "Natural language narrative summarizing the dataset.",
        "contentType": "text/plain",
    }
    artifact_contents["results/report.html"] = {
        "kind": "html",
        "html": html_report,
        "description": "Formatted HTML narrative summarizing the dataset.",
        "contentType": "text/html",
    }

    # Optional: also write the artifacts to disk if artifact_prefix is supplied
    artifact_prefix = state.get("artifact_prefix")
    if artifact_prefix:
        try:
            prefix_path = Path(str(artifact_prefix))
            prefix_path.mkdir(parents=True, exist_ok=True)
            (prefix_path / "report.html").write_text(html_report)
            (prefix_path / "report.txt").write_text(summary_text)
        except Exception:
            logger.exception("failed to write narrative report files to artifact_prefix: %s", artifact_prefix)

    # Return updated state with this phase output
    update = _with_phase(
        state,
        "nl_report",
        payload,
        artifact_contents=artifact_contents,
        dataset=dataset,
    )
    _emit_callback(state, "nl_report", payload)
    return update
