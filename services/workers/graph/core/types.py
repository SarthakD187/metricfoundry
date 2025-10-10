from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple, Union, IO
import math
from .constants import (
    _MAX_SAMPLE_VALUES,
    _MAX_DISTINCT_TRACK,
    _MAX_NUMERIC_ROW_SAMPLES,
)

# type alias used across the code
BinaryInput = Union[bytes, bytearray, IO[bytes]]

@dataclass
class RunningStats:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    samples: List[float] = field(default_factory=list)

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2
        self.min_value = value if self.min_value is None else min(self.min_value, value)
        self.max_value = value if self.max_value is None else max(self.max_value, value)
        if len(self.samples) < _MAX_NUMERIC_ROW_SAMPLES:
            self.samples.append(value)

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        return self.m2 / (self.count - 1)

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "count": self.count,
            "mean": self.mean,
            "stddev": self.stddev,
            "min": self.min_value,
            "max": self.max_value,
        }

@dataclass
class ColumnAccumulator:
    name: str
    null_count: int = 0
    type_counts: Dict[str, int] = field(default_factory=lambda: {"numeric": 0, "boolean": 0, "text": 0})
    sample_values: List[Any] = field(default_factory=list)
    distinct_values: set[str] = field(default_factory=set)
    distinct_overflow: bool = False
    numeric_stats: Optional[RunningStats] = None

    def register_value(self, kind: str, display_value: Any, numeric_value: Optional[float]) -> None:
        if kind == "null":
            self.null_count += 1
            return

        self.type_counts[kind] = self.type_counts.get(kind, 0) + 1

        if display_value is not None and len(self.sample_values) < _MAX_SAMPLE_VALUES:
            self.sample_values.append(display_value)

        if display_value is not None:
            text_value = str(display_value)
            if text_value not in self.distinct_values:
                if len(self.distinct_values) < _MAX_DISTINCT_TRACK:
                    self.distinct_values.add(text_value)
                else:
                    self.distinct_overflow = True

        if kind == "numeric" and numeric_value is not None:
            if self.numeric_stats is None:
                self.numeric_stats = RunningStats()
            self.numeric_stats.update(float(numeric_value))

    @property
    def non_null_count(self) -> int:
        return sum(self.type_counts.values())

    @property
    def inferred_type(self) -> str:
        if not self.type_counts:
            return "unknown"
        kind, count = max(self.type_counts.items(), key=lambda item: item[1])
        if count == 0:
            return "unknown"
        return kind

    def distinct_sample(self) -> List[str]:
        return sorted(self.distinct_values)

    def to_profile(self, total_rows: int) -> Dict[str, Any]:
        non_null = self.non_null_count
        null_ratio = (self.null_count / total_rows) if total_rows else 0.0
        return {
            "name": self.name,
            "inferredType": self.inferred_type,
            "nullCount": self.null_count,
            "nullRatio": null_ratio,
            "nonNullCount": non_null,
            "sampleValues": list(self.sample_values),
            "distinctSample": self.distinct_sample(),
        }

@dataclass
class DatasetSummary:
    row_count: int
    columns: Dict[str, ColumnAccumulator]
    preview_rows: List[Dict[str, Any]]
    numeric_row_samples: List[Dict[str, float]]
    bytes_read: int
    source_format: str
    training_rows: List[Dict[str, Any]]
    duplicate_row_count: int

    @property
    def column_names(self) -> List[str]:
        return sorted(self.columns.keys())

    def column(self, name: str) -> ColumnAccumulator:
        return self.columns[name]

@dataclass
class PipelineResult:
    phases: Dict[str, Dict[str, Any]]
    metrics: Dict[str, Any]
    manifest: Dict[str, Any]
    artifact_contents: Dict[str, Dict[str, Any]]
    correlations: List[Dict[str, Any]]
    outliers: List[Dict[str, Any]]
    ml_inference: Dict[str, Any]
