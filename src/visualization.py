"""Visualization utilities for telemetry EDA and KPI analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import auc, precision_recall_curve, roc_curve

from config import OUTPUTS_FIGURES_DIR

sns.set_theme(style="whitegrid", context="notebook")


def _ensure_output_dir() -> Path:
    OUTPUTS_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUTS_FIGURES_DIR


def plot_correlation_heatmap(corr_df: pd.DataFrame) -> str:
    """Render and save a correlation heatmap."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase3_correlation_heatmap.png"

    fig, ax = plt.subplots(figsize=(10, 7))
    sns.heatmap(
        corr_df,
        cmap="coolwarm",
        center=0.0,
        annot=False,
        square=False,
        cbar_kws={"label": "Pearson r"},
        ax=ax,
    )
    ax.set_title("Telemetry Correlation Matrix (Phase 3)")
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)


def plot_power_hashrate_tradeoff(df: pd.DataFrame) -> str:
    """Render and save power vs hashrate trade-off scatter plot."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase3_power_vs_hashrate.png"

    sample = df.sample(n=min(8000, len(df)), random_state=42)
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.scatterplot(
        data=sample,
        x="asic_power_w",
        y="asic_hashrate_ths",
        hue="operating_mode",
        alpha=0.35,
        s=20,
        ax=ax,
    )
    ax.set_title("Power vs Hashrate by Operating Mode")
    ax.set_xlabel("ASIC Power (W)")
    ax.set_ylabel("ASIC Hashrate (TH/s)")
    ax.legend(title="Mode", loc="best")
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)


def plot_te_distribution_by_mode(df: pd.DataFrame) -> str:
    """Render and save TE distribution by mode."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase3_te_by_mode.png"

    fig, ax = plt.subplots(figsize=(9, 6))
    sns.boxplot(
        data=df,
        x="operating_mode",
        y="true_efficiency_te",
        order=["eco", "normal", "turbo"],
        ax=ax,
    )
    ax.set_title("True Efficiency (TE) by Operating Mode")
    ax.set_xlabel("Operating Mode")
    ax.set_ylabel("True Efficiency (TH/W adjusted)")
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)


def build_phase3_figures(corr_df: pd.DataFrame, df_with_kpi: pd.DataFrame) -> Dict[str, str]:
    """Generate all Phase 3 required figures."""

    return {
        "correlation_heatmap": plot_correlation_heatmap(corr_df),
        "power_hashrate_tradeoff": plot_power_hashrate_tradeoff(df_with_kpi),
        "te_by_mode": plot_te_distribution_by_mode(df_with_kpi),
    }


def plot_phase5_confusion_matrix(cm: np.ndarray) -> str:
    """Render and save confusion matrix heatmap."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase5_confusion_matrix.png"

    fig, ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        cbar=False,
        xticklabels=["Pred 0", "Pred 1"],
        yticklabels=["True 0", "True 1"],
        ax=ax,
    )
    ax.set_title("Phase 5 Confusion Matrix")
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)


def plot_phase5_pr_curve(y_true: np.ndarray, y_score: np.ndarray) -> str:
    """Render and save precision-recall curve."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase5_precision_recall_curve.png"

    precision, recall, _ = precision_recall_curve(y_true, y_score)
    pr_auc = auc(recall, precision)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(recall, precision, linewidth=2, label=f"PR AUC={pr_auc:.3f}")
    ax.set_title("Phase 5 Precision-Recall Curve")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.02)
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)


def plot_phase5_roc_curve(y_true: np.ndarray, y_score: np.ndarray) -> str:
    """Render and save ROC curve."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase5_roc_curve.png"

    fpr, tpr, _ = roc_curve(y_true, y_score)
    roc_auc = auc(fpr, tpr)

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(fpr, tpr, linewidth=2, label=f"ROC AUC={roc_auc:.3f}")
    ax.plot([0, 1], [0, 1], linestyle="--", linewidth=1)
    ax.set_title("Phase 5 ROC Curve")
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.02)
    ax.legend(loc="lower right")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)


def plot_phase5_risk_distribution(risk_df: pd.DataFrame) -> str:
    """Render and save risk score distribution."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase5_risk_distribution.png"

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.histplot(
        data=risk_df,
        x="risk_score",
        hue="failure_within_horizon",
        bins=35,
        kde=True,
        stat="density",
        common_norm=False,
        alpha=0.35,
        ax=ax,
    )
    ax.set_title("Phase 5 Risk Score Distribution by True Label")
    ax.set_xlabel("Predicted Risk Score")
    ax.set_ylabel("Density")
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)
