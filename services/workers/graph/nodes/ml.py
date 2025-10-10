from __future__ import annotations
from typing import Any, Dict, List, MutableMapping, Optional
from dataclasses import dataclass, field
from collections import Counter
from ..core.types import DatasetSummary
from ..core.state import _with_phase, _emit_callback

@dataclass
class _AutoMLArtifacts:
    payload: Dict[str, Any]
    prediction_headers: Optional[List[str]] = None
    prediction_rows: Optional[List[Dict[str, Any]]] = None
    model_bytes: Optional[bytes] = None
    prediction_artifact_path: Optional[str] = None
    prediction_artifact_description: Optional[str] = None
    extra_artifacts: Dict[str, Dict[str, Any]] = field(default_factory=dict)

def _run_automl_training(dataset: DatasetSummary) -> _AutoMLArtifacts:
    if not dataset.training_rows:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "Automated modeling requires at least a handful of fully parsed rows.",
            }
        )

    try:
        import pandas as pd  # type: ignore
        from pandas.api import types as pdt  # type: ignore
    except ImportError as exc:  # pragma: no cover - surfaced at runtime
        return _AutoMLArtifacts(
            payload={
                "status": "failed",
                "message": "Automated modeling requires the pandas dependency.",
                "details": str(exc),
            }
        )

    try:
        from sklearn.cluster import KMeans  # type: ignore
        from sklearn.compose import ColumnTransformer  # type: ignore
        from sklearn.ensemble import (  # type: ignore
            GradientBoostingClassifier,
            GradientBoostingRegressor,
            RandomForestClassifier,
            RandomForestRegressor,
        )
        from sklearn.impute import SimpleImputer  # type: ignore
        from sklearn.linear_model import LinearRegression, LogisticRegression  # type: ignore
        from sklearn.metrics import (  # type: ignore
            accuracy_score,
            f1_score,
            mean_absolute_error,
            mean_squared_error,
            r2_score,
            silhouette_score,
        )
        from sklearn.model_selection import train_test_split  # type: ignore
        from sklearn.pipeline import Pipeline as SKPipeline  # type: ignore
        from sklearn.preprocessing import LabelEncoder, OneHotEncoder, StandardScaler  # type: ignore
        import joblib  # type: ignore
    except ImportError as exc:  # pragma: no cover - surfaced at runtime
        return _AutoMLArtifacts(
            payload={
                "status": "failed",
                "message": "Automated modeling requires scikit-learn to be installed.",
                "details": str(exc),
            }
        )

    frame = pd.DataFrame(dataset.training_rows)
    if frame.empty:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "No sampled rows were available for automated modeling.",
            }
        )

    # Ensure a stable column order aligned with the dataset summary.
    for name in dataset.column_names:
        if name not in frame.columns:
            frame[name] = None
    ordered_columns = [name for name in dataset.column_names if name in frame.columns]
    frame = frame[ordered_columns]

    # Drop columns that are entirely missing or contain nested structures we cannot encode.
    drop_candidates: List[str] = []
    for col in list(frame.columns):
        series = frame[col]
        if series.dropna().empty:
            drop_candidates.append(col)
            continue
        sample_values = list(series.dropna().head(10))
        if any(isinstance(value, (list, dict, set, tuple)) for value in sample_values):
            drop_candidates.append(col)
    if drop_candidates:
        frame = frame.drop(columns=drop_candidates)
    if frame.empty:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "All columns were excluded from automated modeling after sanitisation.",
            }
        )

    # Promote numeric-like strings to numeric dtype and normalise booleans.
    for col in frame.columns:
        series = frame[col]
        if pdt.is_bool_dtype(series):
            frame[col] = series.astype(int)
            continue
        if pdt.is_object_dtype(series):
            numeric_candidate = pd.to_numeric(series, errors="coerce")
            if numeric_candidate.notnull().sum() >= max(3, int(0.6 * series.notnull().sum())):
                frame[col] = numeric_candidate

    lower_to_actual = {name.lower(): name for name in frame.columns}
    target_hints = ["target", "label", "y", "class", "outcome", "response", "default", "churn"]
    target_column: Optional[str] = None
    for hint in target_hints:
        if hint in lower_to_actual:
            target_column = lower_to_actual[hint]
            break

    if target_column is None:
        balanced_candidates: List[Tuple[float, str]] = []
        fallback: Optional[str] = None
        row_count = len(frame)
        for name in frame.columns:
            column = dataset.columns.get(name)
            series = frame[name]
            non_null = series.dropna()
            unique = non_null.nunique()
            if unique < 2:
                continue
            ratio = unique / max(1, len(non_null))
            if fallback is None:
                fallback = name
            type_penalty = 0.0
            if column is None or column.inferred_type == "text":
                type_penalty = 0.1
            if ratio > 0.98 and row_count > 50:
                type_penalty += 0.2
            if ratio < 0.02:
                type_penalty += 0.2
            balanced_candidates.append((type_penalty + abs(0.5 - min(ratio, 1.0)), name))
        if balanced_candidates:
            balanced_candidates.sort(key=lambda item: item[0])
            target_column = balanced_candidates[0][1]
        elif fallback is not None:
            target_column = fallback

    warnings_out: List[str] = []

    if target_column is None:
        feature_columns = [col for col in frame.columns if frame[col].notnull().sum() > 0]
        if not feature_columns:
            return _AutoMLArtifacts(
                payload={
                    "status": "skipped",
                    "message": "Clustering requires at least one informative feature column.",
                }
            )

        X = frame[feature_columns].copy()
        feature_mask = X.notnull().sum(axis=1) > 0
        X = X.loc[feature_mask]
        if len(X) < 10:
            return _AutoMLArtifacts(
                payload={
                    "status": "skipped",
                    "message": "At least 10 rows with usable features are required for clustering previews.",
                }
            )

        for col in X.columns:
            series = X[col]
            if pdt.is_bool_dtype(series):
                X[col] = series.astype(int)
                continue
            if pdt.is_object_dtype(series):
                numeric_candidate = pd.to_numeric(series, errors="coerce")
                if numeric_candidate.notnull().sum() >= max(3, int(0.6 * series.notnull().sum())):
                    X[col] = numeric_candidate
                else:
                    X[col] = series.astype(str)

        constant_columns = [col for col in X.columns if X[col].nunique(dropna=True) <= 1]
        if constant_columns:
            X = X.drop(columns=constant_columns)
            feature_columns = [col for col in feature_columns if col not in constant_columns]
        if X.empty or not feature_columns:
            return _AutoMLArtifacts(
                payload={
                    "status": "skipped",
                    "message": "Clustering requires at least one informative feature column.",
                }
            )

        numeric_features = [col for col in X.columns if pdt.is_numeric_dtype(X[col])]
        categorical_features = [col for col in X.columns if col not in numeric_features]
        if not numeric_features and not categorical_features:
            return _AutoMLArtifacts(
                payload={
                    "status": "skipped",
                    "message": "Clustering requires at least one informative feature column.",
                }
            )

        def _make_preprocessor() -> ColumnTransformer:
            transformers = []
            if numeric_features:
                transformers.append(
                    (
                        "numeric",
                        SKPipeline(
                            steps=[
                                ("imputer", SimpleImputer(strategy="median")),
                                ("scaler", StandardScaler()),
                            ]
                        ),
                        numeric_features,
                    )
                )
            if categorical_features:
                try:
                    encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
                except TypeError:  # pragma: no cover - older sklearn fallback
                    encoder = OneHotEncoder(handle_unknown="ignore", sparse=False)
                transformers.append(
                    (
                        "categorical",
                        SKPipeline(
                            steps=[
                                ("imputer", SimpleImputer(strategy="most_frequent")),
                                ("encoder", encoder),
                            ]
                        ),
                        categorical_features,
                    )
                )
            return ColumnTransformer(transformers=transformers, remainder="drop")

        try:
            preprocessor = _make_preprocessor()
            processed = preprocessor.fit_transform(X)
        except Exception as exc:
            return _AutoMLArtifacts(
                payload={
                    "status": "failed",
                    "message": "Failed to prepare features for clustering.",
                    "details": str(exc),
                }
            )

        if hasattr(processed, "toarray"):
            processed_array = processed.toarray()
        else:
            processed_array = processed

        max_clusters = min(6, len(X) - 1)
        if max_clusters < 2:
            return _AutoMLArtifacts(
                payload={
                    "status": "skipped",
                    "message": "Clustering requires at least two clusters and sufficient rows to evaluate them.",
                }
            )

        candidate_summaries: List[Dict[str, Any]] = []
        silhouette_scores_map: Dict[int, float] = {}
        best_labels: Optional[Any] = None
        best_k: Optional[int] = None
        best_score: float = float("-inf")

        for n_clusters in range(2, max_clusters + 1):
            model_name = f"kmeans_{n_clusters}"
            try:
                model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                labels = model.fit_predict(processed_array)
                if len(set(int(label) for label in labels)) < 2:
                    candidate_summaries.append(
                        {
                            "name": model_name,
                            "status": "degenerate",
                            "clusterCount": n_clusters,
                        }
                    )
                    warnings_out.append(f"{model_name}: produced a single cluster")
                    continue
                score = float(silhouette_score(processed_array, labels))
                silhouette_scores_map[n_clusters] = score
                candidate_summaries.append(
                    {
                        "name": model_name,
                        "status": "evaluated",
                        "clusterCount": n_clusters,
                        "silhouetteScore": score,
                    }
                )
                if score > best_score:
                    best_score = score
                    best_labels = labels
                    best_k = n_clusters
            except Exception as exc:
                warnings_out.append(f"{model_name}: {exc}")
                candidate_summaries.append(
                    {
                        "name": model_name,
                        "status": "error",
                        "clusterCount": n_clusters,
                        "message": str(exc),
                    }
                )

        if best_labels is None or best_k is None:
            return _AutoMLArtifacts(
                payload={
                    "status": "failed",
                    "message": "All clustering attempts failed.",
                    "warnings": warnings_out,
                    "candidateModels": candidate_summaries,
                }
            )

        cluster_sizes = Counter(int(label) for label in best_labels)
        cluster_summary = [
            {"cluster": int(cluster), "size": int(size)}
            for cluster, size in sorted(cluster_sizes.items(), key=lambda item: item[0])
        ]

        assignments = [
            {"rowId": int(idx), "cluster": int(label)}
            for idx, label in enumerate(best_labels)
        ]

        feature_stats: List[Dict[str, Any]] = []
        for col in X.columns:
            non_null = int(X[col].notnull().sum())
            coverage = float(non_null / len(X)) if len(X) else 0.0
            unique = int(X[col].nunique(dropna=True))
            feature_stats.append(
                {
                    "name": col,
                    "nonNullCount": non_null,
                    "coverage": coverage,
                    "uniqueValues": unique,
                }
            )

        payload = {
            "status": "succeeded",
            "message": "Automated clustering preview completed successfully.",
            "mode": "clustering",
            "rowsUsed": len(X),
            "featureColumns": list(X.columns),
            "bestModel": {
                "name": f"kmeans_{best_k}",
                "metric": "silhouette",
                "score": best_score,
                "clusterCount": best_k,
            },
            "candidateModels": candidate_summaries,
            "clusterSummary": cluster_summary,
            "silhouetteScores": {
                int(k): float(v) for k, v in silhouette_scores_map.items()
            },
            "featureStats": feature_stats,
            "predictionPreview": assignments[:10],
            "artifacts": {
                "clusters": "results/clusters.csv",
                "clusterMetrics": "results/clustering_metrics.json",
            },
        }
        if warnings_out:
            payload["warnings"] = warnings_out

        extra_artifacts = {
            "results/clustering_metrics.json": {
                "kind": "json",
                "data": {
                    "best": {
                        "clusterCount": best_k,
                        "silhouetteScore": best_score,
                    },
                    "candidates": candidate_summaries,
                },
                "description": "Silhouette scores for candidate clustering solutions.",
                "contentType": "application/json",
            }
        }

        return _AutoMLArtifacts(
            payload=payload,
            prediction_headers=["rowId", "cluster"],
            prediction_rows=assignments,
            prediction_artifact_path="results/clusters.csv",
            prediction_artifact_description="Cluster assignments produced by the automated clustering preview.",
            extra_artifacts=extra_artifacts,
        )

    feature_columns = [col for col in frame.columns if col != target_column]
    feature_columns = [col for col in feature_columns if frame[col].notnull().sum() > 0]
    if not feature_columns:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "Automated modeling needs at least one informative feature column.",
            }
        )

    X = frame[feature_columns].copy()
    target_series = frame[target_column]

    # Remove rows without a target value or with fully missing feature rows.
    mask = target_series.notnull()
    if not mask.any():
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "No rows with target values were available for automated modeling.",
            }
        )
    X = X.loc[mask]
    target_series = target_series.loc[mask]

    feature_mask = X.notnull().sum(axis=1) > 0
    X = X.loc[feature_mask]
    target_series = target_series.loc[feature_mask]
    if len(X) < 20:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "At least 20 rows with usable features and targets are required for automated modeling.",
            }
        )

    column_meta = dataset.columns.get(target_column)
    target_non_null = target_series.dropna()
    unique_targets = target_non_null.nunique()
    if column_meta and column_meta.inferred_type == "numeric" and unique_targets > 10:
        problem_type = "regression"
    elif unique_targets <= 10:
        problem_type = "classification"
    elif pdt.is_numeric_dtype(target_series):
        problem_type = "regression"
    else:
        problem_type = "classification"

    if problem_type == "regression":
        numeric_target = pd.to_numeric(target_series, errors="coerce")
        mask = numeric_target.notnull()
        X = X.loc[mask]
        target_series = numeric_target.loc[mask]
    else:
        target_series = target_series.astype(str)

    if len(X) < 20:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "After cleaning there were fewer than 20 rows available for modeling.",
            }
        )

    # Re-apply numeric promotion after row filtering to keep dtypes stable.
    for col in X.columns:
        series = X[col]
        if pdt.is_bool_dtype(series):
            X[col] = series.astype(int)
            continue
        if pdt.is_object_dtype(series):
            numeric_candidate = pd.to_numeric(series, errors="coerce")
            if numeric_candidate.notnull().sum() >= max(3, int(0.6 * series.notnull().sum())):
                X[col] = numeric_candidate

    numeric_features = [col for col in X.columns if pdt.is_numeric_dtype(X[col])]
    categorical_features = [col for col in X.columns if col not in numeric_features]

    if not numeric_features and not categorical_features:
        return _AutoMLArtifacts(
            payload={
                "status": "skipped",
                "message": "Feature engineering removed all usable columns for modeling.",
            }
        )

    if problem_type == "classification":
        label_encoder = LabelEncoder()
        encoded_target = label_encoder.fit_transform(target_series)
        metric_name = "accuracy"
    else:
        label_encoder = None
        encoded_target = target_series.astype(float)
        metric_name = "r2"

    stratify = encoded_target if problem_type == "classification" and len(set(encoded_target)) > 1 else None
    try:
        X_train, X_valid, y_train, y_valid = train_test_split(
            X,
            encoded_target,
            test_size=0.2,
            random_state=42,
            stratify=stratify,
        )
    except ValueError:
        X_train, X_valid, y_train, y_valid = train_test_split(
            X,
            encoded_target,
            test_size=0.2,
            random_state=42,
        )

    def _make_preprocessor() -> ColumnTransformer:
        transformers = []
        if numeric_features:
            transformers.append(
                (
                    "numeric",
                    SKPipeline(
                        steps=[
                            ("imputer", SimpleImputer(strategy="median")),
                            ("scaler", StandardScaler()),
                        ]
                    ),
                    numeric_features,
                )
            )
        if categorical_features:
            try:
                encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
            except TypeError:  # pragma: no cover - older sklearn fallback
                encoder = OneHotEncoder(handle_unknown="ignore", sparse=False)
            transformers.append(
                (
                    "categorical",
                    SKPipeline(
                        steps=[
                            ("imputer", SimpleImputer(strategy="most_frequent")),
                            ("encoder", encoder),
                        ]
                    ),
                    categorical_features,
                )
            )
        return ColumnTransformer(transformers=transformers, remainder="drop")

    candidate_models: List[Tuple[str, Any]]
    if problem_type == "classification":
        candidate_models = [
            ("random_forest_classifier", RandomForestClassifier(n_estimators=200, random_state=42)),
            ("gradient_boosting_classifier", GradientBoostingClassifier(random_state=42)),
            ("logistic_regression", LogisticRegression(max_iter=500, multi_class="auto")),
        ]
    else:
        candidate_models = [
            ("random_forest_regressor", RandomForestRegressor(n_estimators=200, random_state=42)),
            ("gradient_boosting_regressor", GradientBoostingRegressor(random_state=42)),
            ("linear_regression", LinearRegression()),
        ]

    candidate_summaries: List[Dict[str, Any]] = []
    warnings_out = []
    best_pipeline: Optional[SKPipeline] = None
    best_model_name: Optional[str] = None
    best_metrics: Dict[str, Any] = {}
    best_score: float = float("-inf")

    import warnings

    for model_name, estimator in candidate_models:
        pipeline = SKPipeline(
            steps=[
                ("preprocessor", _make_preprocessor()),
                ("model", estimator),
            ]
        )
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pipeline.fit(X_train, y_train)
            preds = pipeline.predict(X_valid)
            if problem_type == "classification":
                accuracy = float(accuracy_score(y_valid, preds))
                try:
                    f1 = float(f1_score(y_valid, preds, average="macro"))
                except ValueError:
                    f1 = accuracy
                metrics_map = {"accuracy": accuracy, "f1Macro": f1}
                score = accuracy
            else:
                r2 = float(r2_score(y_valid, preds))
                rmse = float(math.sqrt(mean_squared_error(y_valid, preds)))
                mae = float(mean_absolute_error(y_valid, preds))
                metrics_map = {"r2": r2, "rmse": rmse, "mae": mae}
                score = r2
            candidate_summaries.append(
                {
                    "name": model_name,
                    "status": "evaluated",
                    "metrics": metrics_map,
                }
            )
            if score > best_score:
                best_score = score
                best_pipeline = pipeline
                best_model_name = model_name
                best_metrics = metrics_map
        except Exception as exc:  # pragma: no cover - defensive
            warnings_out.append(f"{model_name}: {exc}")
            candidate_summaries.append(
                {
                    "name": model_name,
                    "status": "error",
                    "message": str(exc),
                }
            )

    if best_pipeline is None or best_model_name is None:
        return _AutoMLArtifacts(
            payload={
                "status": "failed",
                "message": "All automated modeling attempts failed.",
                "warnings": warnings_out,
                "candidateModels": candidate_summaries,
            }
        )

    # Re-fit on the full dataset for artifact generation.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        best_pipeline.fit(X, encoded_target)

    predictions_encoded = best_pipeline.predict(X)

    if problem_type == "classification" and label_encoder is not None:
        predictions = label_encoder.inverse_transform(predictions_encoded.astype(int))
        actual_values = target_series.astype(str)
    else:
        predictions = predictions_encoded
        actual_values = target_series.astype(float if problem_type == "regression" else str)

    prediction_table = pd.DataFrame(
        {
            "rowId": list(range(len(X))),
            "prediction": predictions,
            "actual": actual_values.values,
        }
    )

    estimator = best_pipeline.named_steps.get("model")
    if problem_type == "classification" and estimator is not None and hasattr(estimator, "predict_proba"):
        try:
            probabilities = best_pipeline.predict_proba(X)
            classes = getattr(estimator, "classes_", [])
            if label_encoder is not None and len(classes):
                try:
                    classes = label_encoder.inverse_transform(classes.astype(int))
                except Exception:
                    classes = [str(cls) for cls in classes]
            for index, cls in enumerate(classes):
                column_name = f"prob_{cls}"
                prediction_table[column_name] = probabilities[:, index]
        except Exception as exc:  # pragma: no cover - probabilistic output may fail
            warnings_out.append(f"predict_proba unavailable: {exc}")

    prediction_table = prediction_table.where(pd.notnull(prediction_table), None)
    prediction_headers = [str(col) for col in prediction_table.columns]
    prediction_rows = prediction_table.to_dict(orient="records")

    feature_stats: List[Dict[str, Any]] = []
    for col in X.columns:
        non_null = int(X[col].notnull().sum())
        coverage = float(non_null / len(X)) if len(X) else 0.0
        unique = int(X[col].nunique(dropna=True))
        feature_stats.append(
            {
                "name": col,
                "nonNullCount": non_null,
                "coverage": coverage,
                "uniqueValues": unique,
            }
        )

    if problem_type == "classification" and label_encoder is not None:
        distribution = target_series.value_counts().to_dict()
    else:
        distribution = {
            "min": float(target_series.min()),
            "max": float(target_series.max()),
            "mean": float(target_series.mean()),
            "median": float(target_series.median()),
        }

    payload: Dict[str, Any] = {
        "status": "succeeded",
        "message": "Automated model selection completed successfully.",
        "target": target_column,
        "problemType": problem_type,
        "rowsUsed": len(X),
        "featureColumns": list(X.columns),
        "bestModel": {
            "name": best_model_name,
            "metric": metric_name,
            "score": best_score,
            "metrics": best_metrics,
        },
        "candidateModels": candidate_summaries,
        "trainTestSplit": {
            "trainRows": len(X_train),
            "validationRows": len(X_valid),
        },
        "featureStats": feature_stats,
        "targetDistribution": distribution,
        "predictionPreview": prediction_rows[:10],
    }
    if warnings_out:
        payload["warnings"] = warnings_out

    model_buffer = io.BytesIO()
    joblib.dump(best_pipeline, model_buffer)
    model_bytes = model_buffer.getvalue()
    payload["artifacts"] = {
        "predictions": "results/predictions.csv",
        "model": "results/model.joblib",
    }
    payload["modelSizeBytes"] = len(model_bytes)

    return _AutoMLArtifacts(
        payload=payload,
        prediction_headers=prediction_headers,
        prediction_rows=prediction_rows,
        model_bytes=model_bytes,
        prediction_artifact_path="results/predictions.csv",
        prediction_artifact_description="Predictions generated by the automated model selection pipeline.",
    )

def ml_inference_node(state: MutableMapping[str, Any]) -> Dict[str, Any]:
    dataset: DatasetSummary = state["dataset"]
    artifact_contents = dict(state.get("artifact_contents", {}))

    try:
        automl_result = _run_automl_training(dataset)
    except Exception as exc:  # pragma: no cover - defensive guardrail
        payload = {
            "status": "failed",
            "message": "Automated modeling encountered an unexpected error.",
            "details": str(exc),
        }
        dataset = state["dataset"]
        update = _with_phase(
        state,
        "ml_inference",
        payload,
        artifact_contents=artifact_contents,
        dataset=dataset,
    )
        
        _emit_callback(state, "ml_inference", payload)
        return update

    payload = dict(automl_result.payload)

    if automl_result.extra_artifacts:
        artifact_contents.update(automl_result.extra_artifacts)

    if automl_result.prediction_headers and automl_result.prediction_rows is not None:
        artifact_path = (
            automl_result.prediction_artifact_path or "results/predictions.csv"
        )
        artifact_description = (
            automl_result.prediction_artifact_description
            or "Predictions generated by the automated model selection pipeline."
        )
        artifact_contents[artifact_path] = {
            "kind": "csv",
            "headers": automl_result.prediction_headers,
            "rows": automl_result.prediction_rows,
            "description": artifact_description,
            "contentType": "text/csv",
        }

    if automl_result.model_bytes:
        artifact_contents["results/model.joblib"] = {
            "kind": "binary",
            "data": automl_result.model_bytes,
            "description": "Serialized scikit-learn pipeline selected by automated modeling.",
            "contentType": "application/octet-stream",
        }

    dataset = state["dataset"]
    update = _with_phase(
        state,
        "ml_inference",
        payload,
        artifact_contents=artifact_contents,
        dataset=dataset,
    )
    _emit_callback(state, "ml_inference", payload)
    return update
