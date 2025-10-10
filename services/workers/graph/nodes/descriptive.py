from __future__ import annotations
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple
from ..core.types import DatasetSummary, ColumnAccumulator
from ..core.state import _with_phase, _emit_callback
from ..core.utils import _get_pyplot, _sanitize_filename
from ..core.constants import _MAX_HISTOGRAMS, _MAX_SCATTERS, _MAX_BOX_PLOTS, _MAX_HEATMAP_COLUMNS
from typing import Sequence
import math
import io
import base64
import matplotlib
matplotlib.use("Agg")  # safe headless backend
from matplotlib import pyplot as plt

def descriptive_stats_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    stats_rows: List[Dict[str, Any]] = []
    outliers: List[Dict[str, Any]] = []

    for name in dataset.column_names:
        column = dataset.column(name)
        stats_obj = column.numeric_stats
        if not stats_obj or stats_obj.count == 0:
            continue
        quantiles = _compute_quantiles(stats_obj.samples, [0.05, 0.25, 0.5, 0.75, 0.95])
        row = {
            "column": name,
            "count": stats_obj.count,
            "mean": stats_obj.mean,
            "stddev": stats_obj.stddev,
            "min": stats_obj.min_value,
            "max": stats_obj.max_value,
        }
        row.update(quantiles)
        stats_rows.append(row)
        maybe_outlier = _detect_outliers(column)
        if maybe_outlier:
            outliers.append(maybe_outlier)

    correlations = _compute_correlations(dataset.numeric_row_samples)

    payload = {
        "numericColumns": len(stats_rows),
        "descriptiveTable": stats_rows,
        "correlations": correlations,
        "outliers": outliers,
    }

    artifact_contents = dict(state.get("artifact_contents", {}))
    if stats_rows:
        artifact_contents["results/descriptive_stats.csv"] = {
            "kind": "csv",
            "headers": [
                "column",
                "count",
                "mean",
                "stddev",
                "min",
                "max",
                "q5",
                "q25",
                "q50",
                "q75",
                "q95",
            ],
            "rows": stats_rows,
            "description": "Descriptive statistics for numeric columns.",
            "contentType": "text/csv",
        }
    if correlations:
        artifact_contents["results/correlations.csv"] = {
            "kind": "csv",
            "headers": ["left", "right", "correlation", "sampleSize"],
            "rows": correlations,
            "description": "Pairwise Pearson correlations for numeric columns.",
            "contentType": "text/csv",
        }
    if outliers:
        artifact_contents["results/outliers.json"] = {
            "kind": "json",
            "data": outliers,
            "description": "Detected outliers per column using a z-score heuristic.",
            "contentType": "application/json",
        }

    visualization_artifacts = _generate_visualization_artifacts(dataset, correlations)
    artifact_contents.update(visualization_artifacts)

    # keep dataset in state for downstream nodes
    dataset = state["dataset"]
    update = _with_phase(
        state,
        "descriptive_stats",
        payload,
        artifact_contents=artifact_contents,
        dataset=dataset,
    )
    _emit_callback(state, "descriptive_stats", payload)
    return update


def _compute_quantiles(samples: Sequence[float], probs: Sequence[float]) -> dict[float, float | None]:
    """
    Linear interpolation between order statistics (p in [0,1]).
    Returns {prob: value or None if no valid samples}.
    """
    # Filter out Nones/NaNs and sort
    xs = [x for x in samples if x is not None and not (isinstance(x, float) and math.isnan(x))]
    if not xs:
        return {float(p): None for p in probs}

    xs.sort()
    n = len(xs)

    out: dict[float, float | None] = {}
    for p in probs:
        p = max(0.0, min(1.0, float(p)))
        pos = p * (n - 1)              # 0-indexed position
        lo = math.floor(pos)
        hi = math.ceil(pos)
        if lo == hi:
            out[p] = xs[lo]
        else:
            frac = pos - lo
            out[p] = xs[lo] * (1.0 - frac) + xs[hi] * frac
    return out


def _detect_outliers(column: ColumnAccumulator) -> Optional[Dict[str, Any]]:
    stats = column.numeric_stats
    if not stats or stats.count < 5:
        return None
    stddev = stats.stddev
    if stddev == 0:
        return None
    mu = stats.mean
    outliers = []
    for value in stats.samples:
        score = abs(value - mu) / stddev
        if score >= 3.0:
            outliers.append({"value": value, "zscore": score})
    if not outliers:
        return None
    outliers.sort(key=lambda item: item["zscore"], reverse=True)
    return {
        "column": column.name,
        "method": "zscore",
        "threshold": 3.0,
        "values": outliers[:10],
    }


def _compute_correlations(samples: Sequence[Mapping[str, float]]) -> List[Dict[str, Any]]:
    stats: Dict[Tuple[str, str], Dict[str, float]] = {}
    for row in samples:
        columns = sorted(row.keys())
        for i, left in enumerate(columns):
            x = row[left]
            for right in columns[i + 1 :]:
                y = row[right]
                key = (left, right)
                agg = stats.setdefault(
                    key,
                    {
                        "count": 0.0,
                        "sum_x": 0.0,
                        "sum_y": 0.0,
                        "sum_x2": 0.0,
                        "sum_y2": 0.0,
                        "sum_xy": 0.0,
                    },
                )
                agg["count"] += 1
                agg["sum_x"] += x
                agg["sum_y"] += y
                agg["sum_x2"] += x * x
                agg["sum_y2"] += y * y
                agg["sum_xy"] += x * y

    correlations: List[Dict[str, Any]] = []
    for (left, right), agg in stats.items():
        count = agg["count"]
        if count < 2:
            continue
        numerator = count * agg["sum_xy"] - agg["sum_x"] * agg["sum_y"]
        denom_left = count * agg["sum_x2"] - agg["sum_x"] ** 2
        denom_right = count * agg["sum_y2"] - agg["sum_y"] ** 2
        denominator = math.sqrt(denom_left * denom_right)
        if denominator == 0:
            continue
        corr = numerator / denominator
        correlations.append(
            {
                "left": left,
                "right": right,
                "correlation": corr,
                "sampleSize": int(count),
            }
        )

    correlations.sort(key=lambda item: abs(item["correlation"]), reverse=True)
    return correlations[:20]


def _render_histogram(column: ColumnAccumulator) -> Optional[bytes]:
    stats = column.numeric_stats
    if not stats or len(stats.samples) < 2:
        return None

    plt = _get_pyplot()
    if plt is None:
        return None

    fig, ax = plt.subplots(figsize=(6, 4))
    bins = _histogram_bin_count(stats.samples)
    ax.hist(
        stats.samples,
        bins=bins,
        color="#2563eb",
        edgecolor="white",
        alpha=0.85,
    )
    ax.set_title(f"Distribution of {column.name}")
    ax.set_xlabel(column.name)
    ax.set_ylabel("Frequency")
    ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def _render_scatter(
    samples: Sequence[Mapping[str, float]], left: str, right: str
) -> Optional[bytes]:
    plt = _get_pyplot()
    if plt is None:
        return None

    xs: List[float] = []
    ys: List[float] = []
    for row in samples:
        if left in row and right in row:
            xs.append(row[left])
            ys.append(row[right])
    if len(xs) < 2:
        return None

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.scatter(xs, ys, s=24, alpha=0.75, c="#7c3aed", edgecolors="none")
    ax.set_title(f"{left} vs {right}")
    ax.set_xlabel(left)
    ax.set_ylabel(right)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def _compute_correlation_matrix(
    samples: Sequence[Mapping[str, float]], column_names: Sequence[str]
) -> Optional[List[List[float]]]:
    column_list = list(column_names)
    count_columns = len(column_list)
    if count_columns < 2:
        return None

    stats: Dict[Tuple[int, int], Dict[str, float]] = {}
    for row in samples:
        if not row:
            continue
        values: List[Tuple[int, float]] = []
        for idx, name in enumerate(column_list):
            value = row.get(name)
            if value is None:
                continue
            values.append((idx, float(value)))
        for left_idx in range(len(values)):
            i, x_val = values[left_idx]
            for right_idx in range(left_idx + 1, len(values)):
                j, y_val = values[right_idx]
                key = (min(i, j), max(i, j))
                agg = stats.setdefault(
                    key,
                    {
                        "count": 0.0,
                        "sum_x": 0.0,
                        "sum_y": 0.0,
                        "sum_x2": 0.0,
                        "sum_y2": 0.0,
                        "sum_xy": 0.0,
                    },
                )
                agg["count"] += 1.0
                agg["sum_x"] += x_val
                agg["sum_y"] += y_val
                agg["sum_x2"] += x_val * x_val
                agg["sum_y2"] += y_val * y_val
                agg["sum_xy"] += x_val * y_val

    matrix: List[List[float]] = [
        [float("nan") for _ in range(count_columns)]
        for _ in range(count_columns)
    ]
    for idx in range(count_columns):
        matrix[idx][idx] = 1.0

    has_value = False
    for (i, j), agg in stats.items():
        count = agg["count"]
        if count < 2:
            continue
        numerator = count * agg["sum_xy"] - agg["sum_x"] * agg["sum_y"]
        denom_left = count * agg["sum_x2"] - agg["sum_x"] ** 2
        denom_right = count * agg["sum_y2"] - agg["sum_y"] ** 2
        denominator = math.sqrt(denom_left * denom_right)
        if denominator == 0:
            continue
        correlation = numerator / denominator
        matrix[i][j] = correlation
        matrix[j][i] = correlation
        has_value = True

    if not has_value:
        return None

    return matrix

def _is_nan(x) -> bool:
    return x is None or (isinstance(x, float) and math.isnan(x))

def _clean_numeric_samples(samples):
    xs = [x for x in samples if not _is_nan(x)]
    return xs

def _histogram_bin_count(samples) -> int:
    """
    Choose bin count via Freedman–Diaconis rule; fallback to sqrt(n).
    Returns >= 1.
    """
    xs = _clean_numeric_samples(samples)
    n = len(xs)
    if n < 2:
        return 1
    xs.sort()
    # IQR = Q3 - Q1 using same quantile routine to stay consistent
    qs = _compute_quantiles(xs, [0.25, 0.75])
    q1, q3 = qs[0.25], qs[0.75]
    iqr = (q3 - q1) if (q1 is not None and q3 is not None) else 0.0
    data_range = xs[-1] - xs[0]
    if iqr and data_range:
        h = 2.0 * iqr * (n ** (-1.0 / 3.0))  # bin width
        if h > 0:
            k = max(1, int(round(data_range / h)))
            return min(k, max(1, int(round(math.sqrt(n)) * 10)))  # cap to avoid absurd bins
    # Fallbacks: Sturges or sqrt
    return max(1, int(round(math.sqrt(n))))


def _render_correlation_heatmap(
    samples: Sequence[Mapping[str, float]], column_names: Sequence[str]
) -> Optional[bytes]:
    plt = _get_pyplot()
    if plt is None:
        return None

    matrix = _compute_correlation_matrix(samples, column_names)
    if matrix is None:
        return None

    labels = list(column_names)
    fig_size = max(6, min(12, 0.75 * len(labels)))
    fig, ax = plt.subplots(figsize=(fig_size, fig_size))
    heatmap = ax.imshow(matrix, cmap="coolwarm", vmin=-1, vmax=1)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    ax.set_title("Correlation Heatmap")
    fig.colorbar(heatmap, ax=ax, fraction=0.046, pad=0.04, label="Correlation")
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def _render_box_plots(columns: Sequence[ColumnAccumulator]) -> Optional[bytes]:
    plt = _get_pyplot()
    if plt is None:
        return None

    data: List[Sequence[float]] = []
    labels: List[str] = []
    for column in columns:
        stats = column.numeric_stats
        if not stats or len(stats.samples) < 2:
            continue
        data.append(stats.samples)
        labels.append(column.name)

    if not data:
        return None

    width = max(6, min(12, 1.2 * len(data)))
    fig, ax = plt.subplots(figsize=(width, 4.5))
    box = ax.boxplot(
        data,
        labels=labels,
        patch_artist=True,
        vert=True,
    )
    for patch in box["boxes"]:
        patch.set(facecolor="#22c55e", alpha=0.6)
    for median in box["medians"]:
        median.set(color="#0f172a", linewidth=1.5)
    ax.set_title("Box Plot Distribution by Column")
    ax.set_ylabel("Value")
    ax.grid(True, axis="y", linestyle="--", linewidth=0.5, alpha=0.5)
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150)
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def _generate_visualization_artifacts(
    dataset: DatasetSummary, correlations: Sequence[Mapping[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    artifacts: Dict[str, Dict[str, Any]] = {}
    if _get_pyplot() is None:
        return artifacts

    numeric_columns: List[ColumnAccumulator] = []
    for name in dataset.column_names:
        column = dataset.column(name)
        if column.numeric_stats and column.numeric_stats.samples:
            numeric_columns.append(column)

    numeric_columns.sort(
        key=lambda col: col.numeric_stats.count if col.numeric_stats else 0,
        reverse=True,
    )

    for column in numeric_columns[:_MAX_HISTOGRAMS]:
        rendered = _render_histogram(column)
        if not rendered:
            continue
        slug = _sanitize_filename(column.name)
        artifacts[f"results/graphs/{slug}_histogram.png"] = {
            "kind": "image",
            "data": rendered,
            "description": f"Histogram showing the distribution of {column.name} values.",
            "contentType": "image/png",
        }

    for corr in list(correlations)[:_MAX_SCATTERS]:
        left = corr.get("left")
        right = corr.get("right")
        if not isinstance(left, str) or not isinstance(right, str):
            continue
        rendered = _render_scatter(dataset.numeric_row_samples, left, right)
        if not rendered:
            continue
        slug = _sanitize_filename(f"{left}_vs_{right}")
        artifacts[f"results/graphs/{slug}_scatter.png"] = {
            "kind": "image",
            "data": rendered,
            "description": (
                f"Scatter plot illustrating the relationship between {left} and {right}."
            ),
            "contentType": "image/png",
        }

    box_plot_rendered = _render_box_plots(numeric_columns[:_MAX_BOX_PLOTS])
    if box_plot_rendered:
        artifacts["results/graphs/box_plots.png"] = {
            "kind": "image",
            "data": box_plot_rendered,
            "description": "Box plots summarizing the distribution and spread of numeric columns.",
            "contentType": "image/png",
        }

    heatmap_columns = [col.name for col in numeric_columns[:_MAX_HEATMAP_COLUMNS]]
    heatmap_rendered = _render_correlation_heatmap(
        dataset.numeric_row_samples, heatmap_columns
    )
    if heatmap_rendered:
        artifacts["results/graphs/correlation_heatmap.png"] = {
            "kind": "image",
            "data": heatmap_rendered,
            "description": "Heatmap visualizing pairwise correlations between numeric columns.",
            "contentType": "image/png",
        }

    return artifacts