from __future__ import annotations
from typing import Any, Dict, List, MutableMapping, Optional
from ..core.types import DatasetSummary
from ..core.state import _with_phase, _emit_callback
from ..core.constants import (
    _DQ_SEVERITY_WEIGHTS, _NUMERIC_RANGE_ERROR_RATIO, _NUMERIC_RANGE_WARNING_RATIO,
    _CARDINALITY_ERROR_THRESHOLD, _CARDINALITY_WARNING_THRESHOLD, _DUPLICATE_ERROR_RATIO, _MAX_DISTINCT_TRACK
)

def dq_validate_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary | None = state.get("dataset")
    if dataset is None:
        raise ValueError("dq_validate requires 'dataset' in state. Upstream parse/profile must set state['dataset'].")

def dq_validate_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset = state.get("dataset")
    if dataset is None:
        raise ValueError(
            "dq_validate requires 'dataset' in state. "
            "Upstream parse/profile must set state['dataset']."
        )

    issues: List[Dict[str, Any]] = []
    rules_evaluated = 0

    severity_counts: Dict[str, int] = {"ERROR": 0, "WARNING": 0, "INFO": 0}
    total_penalty = 0.0

    def _record_issue(
        *,
        rule: str,
        severity: str,
        message: str,
        column: Optional[str] = None,
        value: Optional[Any] = None,
        **extra: Any,
    ) -> None:
        nonlocal total_penalty
        issue: Dict[str, Any] = {"rule": rule, "severity": severity, "message": message}
        if column is not None:
            issue["column"] = column
        if value is not None:
            issue["value"] = value
        if extra:
            issue.update(extra)
        issues.append(issue)
        if severity in severity_counts:
            severity_counts[severity] += 1
        total_penalty += _DQ_SEVERITY_WEIGHTS.get(severity, 0.0)

    dq_metrics: Dict[str, Any] = {
        "duplicateRows": {
            "duplicates": dataset.duplicate_row_count,
            "ratio": (dataset.duplicate_row_count / dataset.row_count) if dataset.row_count else 0.0,
            "severity": "OK",
        },
        "numericRanges": [],
        "categoricalCardinality": [],
    }

    for name in dataset.column_names:
        column = dataset.column(name)
        total = dataset.row_count or 1
        null_ratio = column.null_count / total
        rules_evaluated += 1
        if null_ratio > 0.2:
            severity = "WARNING" if null_ratio < 0.4 else "ERROR"
            _record_issue(
                rule="null_ratio_threshold",
                severity=severity,
                column=name,
                value=null_ratio,
                message=f"{int(null_ratio * 100)}% nulls detected",
            )

        inferred = column.inferred_type
        rules_evaluated += 1
        if inferred == "text" and column.type_counts.get("numeric", 0) > 0:
            _record_issue(
                rule="mixed_type",
                severity="WARNING",
                column=name,
                value=column.type_counts,
                message="Mixed types detected; values parsed as both text and numeric.",
            )

        if inferred == "numeric" and column.numeric_stats:
            stats = column.numeric_stats
            sample_count = len(stats.samples)
            if sample_count:
                rules_evaluated += 1
                stddev = stats.stddev
                lower_bound = stats.mean - 3 * stddev if stddev else stats.min_value
                upper_bound = stats.mean + 3 * stddev if stddev else stats.max_value
                lower_bound = lower_bound if lower_bound is not None else stats.min_value
                upper_bound = upper_bound if upper_bound is not None else stats.max_value
                violations = [
                    value for value in stats.samples if (lower_bound is not None and value < lower_bound) or (upper_bound is not None and value > upper_bound)
                ]
                violation_ratio = (len(violations) / sample_count) if sample_count else 0.0
                severity = "OK"
                if violation_ratio >= _NUMERIC_RANGE_ERROR_RATIO:
                    severity = "ERROR"
                elif violation_ratio >= _NUMERIC_RANGE_WARNING_RATIO:
                    severity = "WARNING"
                dq_metrics["numericRanges"].append(
                    {
                        "column": name,
                        "lowerBound": lower_bound,
                        "upperBound": upper_bound,
                        "minObserved": stats.min_value,
                        "maxObserved": stats.max_value,
                        "sampleCount": sample_count,
                        "violationCount": len(violations),
                        "violationRatio": violation_ratio,
                        "severity": severity,
                    }
                )
                if severity in {"WARNING", "ERROR"}:
                    message = (
                        f"{len(violations)} of {sample_count} sampled values fall outside expected range"
                    )
                    _record_issue(
                        rule="numeric_range",
                        severity=severity,
                        column=name,
                        value=violation_ratio,
                        message=message,
                        bounds={"lower": lower_bound, "upper": upper_bound},
                    )

        if inferred in {"text", "boolean"}:
            rules_evaluated += 1
            observed_distinct = len(column.distinct_values)
            sample_limit_hit = column.distinct_overflow or observed_distinct == _MAX_DISTINCT_TRACK
            severity = "OK"
            if observed_distinct >= _CARDINALITY_ERROR_THRESHOLD or (sample_limit_hit and observed_distinct >= _CARDINALITY_WARNING_THRESHOLD):
                severity = "ERROR"
            elif observed_distinct >= _CARDINALITY_WARNING_THRESHOLD:
                severity = "WARNING"
            cardinality_ratio = (
                observed_distinct / column.non_null_count if column.non_null_count else 0.0
            )
            dq_metrics["categoricalCardinality"].append(
                {
                    "column": name,
                    "distinctSample": observed_distinct,
                    "nonNullCount": column.non_null_count,
                    "ratio": cardinality_ratio,
                    "sampleLimitHit": sample_limit_hit,
                    "severity": severity,
                }
            )
            if severity in {"WARNING", "ERROR"}:
                message = (
                    f"Observed {observed_distinct} distinct values across {column.non_null_count} non-null rows"
                )
                if sample_limit_hit:
                    message += "; sample limit reached, true cardinality may be higher"
                _record_issue(
                    rule="categorical_cardinality",
                    severity=severity,
                    column=name,
                    value=observed_distinct,
                    message=message,
                )

    if dataset.row_count:
        rules_evaluated += 1
        duplicate_ratio = dataset.duplicate_row_count / dataset.row_count
        duplicate_severity = "OK"
        if dataset.duplicate_row_count:
            duplicate_severity = (
                "ERROR" if duplicate_ratio >= _DUPLICATE_ERROR_RATIO else "WARNING"
            )
            message = (
                f"Detected {dataset.duplicate_row_count} duplicate rows ({duplicate_ratio:.2%} of dataset)"
            )
            _record_issue(
                rule="duplicate_rows",
                severity=duplicate_severity,
                message=message,
                value={"count": dataset.duplicate_row_count, "ratio": duplicate_ratio},
            )
        dq_metrics["duplicateRows"]["severity"] = duplicate_severity
        dq_metrics["duplicateRows"]["duplicates"] = dataset.duplicate_row_count
        dq_metrics["duplicateRows"]["ratio"] = duplicate_ratio

    score = max(0.0, 1.0 - min(1.0, total_penalty))

    severity_order = ["ERROR", "WARNING", "INFO"]
    max_severity = next((level for level in severity_order if severity_counts.get(level)), None)

    payload = {
        "rulesEvaluated": rules_evaluated,
        "issues": issues,
        "score": score,
        "severitySummary": {
            "bySeverity": severity_counts,
            "maxSeverity": max_severity,
            "penalty": total_penalty,
            "issueCount": len(issues),
        },
        "metrics": dq_metrics,
    }

    update = _with_phase(state, "dq_validate", payload, dataset=dataset)
    _emit_callback(state, "dq_validate", payload)
    return update