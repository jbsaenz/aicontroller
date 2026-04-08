"""Model training and selection for predictive maintenance."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from config import (
    PHASE4_BEST_MODEL_SUMMARY_PATH,
    PHASE4_FEATURE_IMPORTANCE_PATH,
    PHASE4_MODEL_ARTIFACT_PATH,
    PHASE4_MODEL_COMPARISON_PATH,
)
from evaluation import build_model_evaluation
from inference import build_risk_outputs, save_risk_outputs


def _derive_cooling_power_ratio(features_df: pd.DataFrame, default: float = 0.24) -> float:
    if "cooling_power_w" not in features_df.columns or "asic_power_w" not in features_df.columns:
        return default

    asic_power = pd.to_numeric(features_df["asic_power_w"], errors="coerce")
    cooling_power = pd.to_numeric(features_df["cooling_power_w"], errors="coerce")
    ratio = cooling_power / asic_power.replace(0, np.nan)
    ratio = ratio.replace([np.inf, -np.inf], np.nan).dropna()
    if ratio.empty:
        return default

    median_ratio = float(ratio.median())
    if np.isnan(median_ratio) or np.isinf(median_ratio):
        return default
    return max(median_ratio, 0.0)


def _time_split(
    df: pd.DataFrame, split_quantile: float = 0.80
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    ordered = df.sort_values("timestamp").reset_index(drop=True)
    cutoff = ordered["timestamp"].quantile(split_quantile)
    train_df = ordered[ordered["timestamp"] <= cutoff].copy()
    valid_df = ordered[ordered["timestamp"] > cutoff].copy()
    if len(valid_df) == 0:
        # Fallback for edge cases with repeated timestamp values.
        split_idx = int(len(ordered) * split_quantile)
        train_df = ordered.iloc[:split_idx].copy()
        valid_df = ordered.iloc[split_idx:].copy()
    return train_df, valid_df


def _build_preprocessor(
    numeric_cols: List[str], categorical_cols: List[str]
) -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_cols,
            ),
            (
                "cat",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_cols,
            ),
        ],
        remainder="drop",
    )


def _extract_feature_importance(
    fitted_pipeline: Pipeline,
    model_name: str,
    top_n: int = 20,
) -> pd.DataFrame:
    preprocessor = fitted_pipeline.named_steps["preprocessor"]
    estimator = fitted_pipeline.named_steps["model"]

    feature_names = preprocessor.get_feature_names_out()

    if model_name == "random_forest":
        importance = estimator.feature_importances_
    elif model_name == "logistic_regression":
        coeff = estimator.coef_[0]
        importance = np.abs(coeff)
    else:
        importance = np.zeros(len(feature_names))

    imp_df = pd.DataFrame(
        {"feature": feature_names, "importance": importance}
    ).sort_values("importance", ascending=False)
    return imp_df.head(top_n).reset_index(drop=True)


def run_training_pipeline(
    features_df: pd.DataFrame,
    feature_cols: List[str],
    target_col: str = "failure_within_horizon",
    model_artifact_path: Path | str | None = None,
) -> Dict[str, object]:
    """Train baseline models, compare results, and save best model outputs."""

    working = features_df.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")
    working = working.dropna(subset=["timestamp"]).copy()

    train_df, valid_df = _time_split(working)
    if len(train_df) == 0 or len(valid_df) == 0:
        raise ValueError("Time split failed: train/validation set is empty.")

    X_train = train_df[feature_cols].copy()
    y_train = train_df[target_col].astype(int).to_numpy()
    X_valid = valid_df[feature_cols].copy()
    y_valid = valid_df[target_col].astype(int).to_numpy()

    categorical_cols = [c for c in feature_cols if c == "operating_mode"]
    numeric_cols = [c for c in feature_cols if c not in categorical_cols]

    models = {
        "logistic_regression": LogisticRegression(
            max_iter=1200,
            class_weight="balanced",
            random_state=42,
        ),
        "random_forest": RandomForestClassifier(
            n_estimators=300,
            max_depth=12,
            min_samples_leaf=5,
            class_weight="balanced_subsample",
            random_state=42,
            n_jobs=-1,
        ),
    }

    comparison: Dict[str, Dict[str, object]] = {}
    fitted_models: Dict[str, Pipeline] = {}
    y_scores: Dict[str, np.ndarray] = {}

    for name, estimator in models.items():
        pipeline = Pipeline(
            steps=[
                ("preprocessor", _build_preprocessor(numeric_cols, categorical_cols)),
                ("model", estimator),
            ]
        )
        pipeline.fit(X_train, y_train)
        probs = pipeline.predict_proba(X_valid)[:, 1]
        metrics = build_model_evaluation(y_valid, probs)

        comparison[name] = metrics
        fitted_models[name] = pipeline
        y_scores[name] = probs

    # Select best model by F1 at optimal threshold, tie-breaker on PR-AUC.
    ranking = []
    for name, metrics in comparison.items():
        f1_opt = metrics["optimal_threshold_metrics"]["f1"]
        pr_auc = metrics["optimal_threshold_metrics"]["pr_auc"]
        ranking.append((name, float(f1_opt), float(pr_auc)))
    ranking.sort(key=lambda x: (x[1], x[2]), reverse=True)
    best_model_name = ranking[0][0]
    best_model = fitted_models[best_model_name]
    best_scores = y_scores[best_model_name]
    best_eval = comparison[best_model_name]
    best_threshold = float(best_eval["optimal_threshold"]["threshold"])
    cooling_power_ratio = _derive_cooling_power_ratio(features_df)
    artifact_path = (
        Path(model_artifact_path)
        if model_artifact_path is not None
        else PHASE4_MODEL_ARTIFACT_PATH
    )

    # Save model artifact.
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model_name": best_model_name,
            "pipeline": best_model,
            "feature_columns": feature_cols,
            "target_column": target_col,
            "threshold": best_threshold,
            "serving_defaults": {
                "cooling_power_ratio": cooling_power_ratio,
            },
            "trained_at": pd.Timestamp.utcnow().isoformat(),
        },
        artifact_path,
    )

    # Export model comparison and best model summary.
    PHASE4_MODEL_COMPARISON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with Path(PHASE4_MODEL_COMPARISON_PATH).open("w", encoding="utf-8") as f:
        json.dump(comparison, f, indent=2)

    best_summary = {
        "best_model_name": best_model_name,
        "threshold_for_alerting": best_threshold,
        "train_rows": int(len(train_df)),
        "validation_rows": int(len(valid_df)),
        "train_positive_rate": float(np.mean(y_train)),
        "validation_positive_rate": float(np.mean(y_valid)),
        "metrics": best_eval,
    }
    with Path(PHASE4_BEST_MODEL_SUMMARY_PATH).open("w", encoding="utf-8") as f:
        json.dump(best_summary, f, indent=2)

    # Feature importance export.
    importance_df = _extract_feature_importance(best_model, best_model_name, top_n=25)
    importance_df.to_csv(PHASE4_FEATURE_IMPORTANCE_PATH, index=False)

    # Validation risk outputs.
    validation_context_cols = [
        "timestamp",
        "miner_id",
        "operating_mode",
        "failure_within_horizon",
        "asic_temperature_c",
        "power_instability_index",
        "hashrate_degradation_pct_12h",
        "te_drift_pct_4h",
    ]
    validation_context = valid_df[validation_context_cols].copy()
    risk_df, alerts_df = build_risk_outputs(
        validation_df=validation_context, y_score=best_scores, threshold=best_threshold
    )
    inference_written = save_risk_outputs(risk_df, alerts_df)

    return {
        "best_model_name": best_model_name,
        "best_threshold": best_threshold,
        "model_comparison_path": str(PHASE4_MODEL_COMPARISON_PATH),
        "best_model_summary_path": str(PHASE4_BEST_MODEL_SUMMARY_PATH),
        "feature_importance_path": str(PHASE4_FEATURE_IMPORTANCE_PATH),
        "model_artifact_path": str(artifact_path),
        "validation_risk_outputs": inference_written,
        "train_rows": int(len(train_df)),
        "validation_rows": int(len(valid_df)),
    }
