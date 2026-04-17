"""Evaluation helpers for predictive-maintenance classifiers."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    auc,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)


def _safe_roc_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    classes = np.unique(y_true)
    if len(classes) < 2:
        return float("nan")
    return float(roc_auc_score(y_true, y_score))


def _pr_auc(y_true: np.ndarray, y_score: np.ndarray) -> float:
    precision, recall, _ = precision_recall_curve(y_true, y_score)
    return float(auc(recall, precision))


def select_optimal_threshold(y_true: np.ndarray, y_score: np.ndarray) -> dict[str, float]:
    """Select probability threshold maximizing F1 score."""

    precision, recall, thresholds = precision_recall_curve(y_true, y_score)
    if len(thresholds) == 0:
        return {"threshold": 0.5, "precision": 0.0, "recall": 0.0, "f1": 0.0}

    precision_t = precision[1:]
    recall_t = recall[1:]
    f1 = (2 * precision_t * recall_t) / (precision_t + recall_t + 1e-12)
    best_idx = int(np.nanargmax(f1))

    return {
        "threshold": float(thresholds[best_idx]),
        "precision": float(precision_t[best_idx]),
        "recall": float(recall_t[best_idx]),
        "f1": float(f1[best_idx]),
    }


def evaluate_at_threshold(
    y_true: np.ndarray, y_score: np.ndarray, threshold: float
) -> dict[str, float]:
    """Compute classification metrics at a fixed probability threshold."""

    y_pred = (y_score >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()

    return {
        "threshold": float(threshold),
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": _safe_roc_auc(y_true, y_score),
        "pr_auc": _pr_auc(y_true, y_score),
        "tp": int(tp),
        "fp": int(fp),
        "tn": int(tn),
        "fn": int(fn),
    }


def build_model_evaluation(y_true: np.ndarray, y_score: np.ndarray) -> dict[str, object]:
    """Build default + optimized-threshold evaluation bundle."""

    default_metrics = evaluate_at_threshold(y_true, y_score, threshold=0.5)
    optimal = select_optimal_threshold(y_true, y_score)
    optimal_metrics = evaluate_at_threshold(
        y_true, y_score, threshold=float(optimal["threshold"])
    )

    return {
        "default_threshold_metrics": default_metrics,
        "optimal_threshold": optimal,
        "optimal_threshold_metrics": optimal_metrics,
    }


def build_threshold_analysis_table(
    y_true: np.ndarray,
    y_score: np.ndarray,
    thresholds: np.ndarray | None = None,
) -> pd.DataFrame:
    """Return metric table across multiple thresholds."""

    if thresholds is None:
        thresholds = np.round(np.arange(0.20, 0.91, 0.05), 3)

    rows = []
    for threshold in thresholds:
        metrics = evaluate_at_threshold(y_true, y_score, float(threshold))
        alert_rate = float(np.mean(y_score >= threshold))
        metrics["alert_rate"] = alert_rate
        rows.append(metrics)

    return pd.DataFrame(rows).sort_values("threshold").reset_index(drop=True)


def get_confusion_matrix_array(
    y_true: np.ndarray, y_score: np.ndarray, threshold: float
) -> np.ndarray:
    """Return confusion matrix array [[tn, fp], [fn, tp]]."""

    y_pred = (y_score >= threshold).astype(int)
    return confusion_matrix(y_true, y_pred, labels=[0, 1])


def get_classification_report_dict(
    y_true: np.ndarray, y_score: np.ndarray, threshold: float
) -> dict[str, object]:
    """Return sklearn classification report as dict at given threshold."""

    y_pred = (y_score >= threshold).astype(int)
    return classification_report(y_true, y_pred, output_dict=True, zero_division=0)
