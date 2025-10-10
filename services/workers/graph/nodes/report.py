from __future__ import annotations
from typing import Any, Dict, List, Mapping, MutableMapping
import html, logging
from ..core.types import DatasetSummary
from ..core.state import _with_phase, _emit_callback
from ..core.constants import _NL_REPORT_TEMPLATE_NAME
from ..core.utils import _JINJA_ENV

logger = logging.getLogger(__name__)

def nl_report_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    phases = state.get("phase_outputs", {})
    dq = phases.get("dq_validate", {})
    stats_phase = phases.get("descriptive_stats", {})

    row_text = f"{dataset.row_count:,} rows" if dataset.row_count else "an unknown number of rows"
    column_text = f"{len(dataset.column_names)} columns"
    dq_score = dq.get("score")
    dq_text = f"Data quality score {dq_score:.2f}" if dq_score is not None else "Data quality checks executed"

    highlights: List[str] = []
    correlations = stats_phase.get("correlations", [])
    if correlations:
        top = correlations[0]
        highlights.append(
            f"Strongest correlation observed between {top['left']} and {top['right']} (r={top['correlation']:.2f})."
        )

    outliers = stats_phase.get("outliers", [])
    if outliers:
        sample_outlier = outliers[0]
        values = sample_outlier["values"][:2]
        formatted = ", ".join(f"{v['value']:.2f}" for v in values)
        highlights.append(
            f"Potential outliers detected in {sample_outlier['column']}: {formatted}."
        )

    summary_lines = [
        f"The dataset contains {row_text} across {column_text}.",
        dq_text + ".",
    ]
    summary_lines.extend(highlights)
    summary_lines.append("Further modeling pipelines can build on these profiling insights.")
    summary_text = " ".join(summary_lines)

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
        "severity_summary": dict(dq.get("severitySummary", {})) or None,
        "issues": dq_issues,
    }

    correlation_preview = [
        {
            "left": item.get("left"),
            "right": item.get("right"),
            "correlation": item.get("correlation", 0.0),
        }
        for item in correlations[:10]
    ]

    outlier_preview: List[Dict[str, Any]] = []
    for entry in outliers[:5]:
        values = [
            {
                "value": value.get("value"),
                "zscore": value.get("zscore"),
            }
            for value in entry.get("values", [])[:5]
        ]
        outlier_preview.append(
            {
                "column": entry.get("column"),
                "values": values,
            }
        )

    html_context = {
        "summary_text": summary_text,
        "highlights": highlights,
        "dataset": {"row_text": row_text, "column_text": column_text},
        "dq": dq_context,
        "correlations": correlation_preview,
        "outliers": outlier_preview,
    }

    escaped_summary = html.escape(summary_text)
    html_report = f"<html><body><p>{escaped_summary}</p></body></html>"
    if _JINJA_ENV is not None:
        try:
            template = _JINJA_ENV.get_template(_NL_REPORT_TEMPLATE_NAME)
            html_report = template.render(html_context)
        except Exception:
            logger.exception("failed to render HTML narrative report")

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

    dataset = state["dataset"]
    update = _with_phase(
        state,
        "nl_report",
        payload,
        artifact_contents=artifact_contents,
        dataset=dataset,
    )
    _emit_callback(state, "nl_report", payload)
    return update