"""Visualization utilities for telemetry EDA and KPI analysis."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import auc, precision_recall_curve, roc_curve

from src.config import OUTPUTS_FIGURES_DIR

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


def build_phase3_figures(corr_df: pd.DataFrame, df_with_kpi: pd.DataFrame) -> dict[str, str]:
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


def plot_feature_importance(importance_csv_path: str | None = None) -> str:
    """Render horizontal bar chart of top 20 model features."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase4_feature_importance.png"

    if importance_csv_path is None:
        from src.config import PHASE4_FEATURE_IMPORTANCE_PATH
        importance_csv_path = str(PHASE4_FEATURE_IMPORTANCE_PATH)

    imp_df = pd.read_csv(importance_csv_path)
    top_20 = imp_df.head(20).sort_values("importance", ascending=True)

    fig, ax = plt.subplots(figsize=(9, 7))
    colors = plt.cm.viridis(np.linspace(0.25, 0.85, len(top_20)))
    ax.barh(top_20["feature"], top_20["importance"], color=colors)
    ax.set_xlabel("Importance (absolute)")
    ax.set_title("Top 20 Predictive Features")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)


def plot_mode_performance_breakdown(
    risk_df: pd.DataFrame, threshold: float = 0.5
) -> str:
    """Render per-mode precision/recall/F1 grouped bar chart."""

    from sklearn.metrics import f1_score, precision_score, recall_score

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase5_mode_performance.png"

    working = risk_df.copy()
    working["y_pred"] = (working["risk_score"] >= threshold).astype(int)
    working["y_true"] = working["failure_within_horizon"].astype(int)

    modes = ["eco", "normal", "turbo"]
    metrics_data = {"mode": [], "Precision": [], "Recall": [], "F1": []}
    for mode in modes:
        subset = working[working["operating_mode"] == mode]
        if len(subset) == 0 or subset["y_true"].nunique() < 2:
            continue
        metrics_data["mode"].append(mode)
        metrics_data["Precision"].append(
            precision_score(subset["y_true"], subset["y_pred"], zero_division=0)
        )
        metrics_data["Recall"].append(
            recall_score(subset["y_true"], subset["y_pred"], zero_division=0)
        )
        metrics_data["F1"].append(
            f1_score(subset["y_true"], subset["y_pred"], zero_division=0)
        )

    if not metrics_data["mode"]:
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.text(0.5, 0.5, "Insufficient data by mode", ha="center", va="center")
        fig.savefig(out_path, dpi=180)
        plt.close(fig)
        return str(out_path)

    metrics_df = pd.DataFrame(metrics_data)
    melted = metrics_df.melt(id_vars="mode", var_name="Metric", value_name="Score")

    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(metrics_data["mode"]))
    width = 0.25
    for i, metric in enumerate(["Precision", "Recall", "F1"]):
        vals = metrics_df[metric].to_numpy()
        offset = (i - 1) * width
        bars = ax.bar(x + offset, vals, width, label=metric, alpha=0.85)
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                    f"{v:.2f}", ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(metrics_data["mode"])
    ax.set_ylabel("Score")
    ax.set_title("Prediction Performance by Operating Mode")
    ax.legend()
    ax.set_ylim(0, 1.0)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=180)
    plt.close(fig)
    return str(out_path)


def plot_te_timeseries(df: pd.DataFrame, n_miners: int = 4) -> str:
    """Render TE time-series for a few miners showing healthy vs degrading patterns."""

    out_dir = _ensure_output_dir()
    out_path = out_dir / "phase3_te_timeseries.png"

    working = df.copy()
    working["timestamp"] = pd.to_datetime(working["timestamp"], errors="coerce")

    # Pick miners with diverse TE behavior: highest and lowest mean TE
    miner_te = working.groupby("miner_id")["true_efficiency_te"].mean().sort_values()
    if len(miner_te) < n_miners:
        selected = miner_te.index.tolist()
    else:
        half = n_miners // 2
        selected = miner_te.head(half).index.tolist() + miner_te.tail(n_miners - half).index.tolist()

    fig, axes = plt.subplots(len(selected), 1, figsize=(12, 3 * len(selected)), sharex=True)
    if len(selected) == 1:
        axes = [axes]

    for ax, miner_id in zip(axes, selected):
        miner_data = working[working["miner_id"] == miner_id].sort_values("timestamp")
        ax.plot(miner_data["timestamp"], miner_data["true_efficiency_te"],
                linewidth=0.8, alpha=0.7, color="steelblue")
        # Add rolling mean overlay
        roll = miner_data["true_efficiency_te"].rolling(window=12, min_periods=1).mean()
        ax.plot(miner_data["timestamp"], roll, linewidth=1.5, color="orangered",
                label="2h rolling mean")
        mean_te = miner_data["true_efficiency_te"].mean()
        ax.set_ylabel("TE")
        ax.set_title(f"{miner_id} — mean TE: {mean_te:.4f}", fontsize=10)
        ax.legend(loc="upper right", fontsize=8)
        ax.grid(alpha=0.3)

    axes[-1].set_xlabel("Time")
    fig.suptitle("True Efficiency Over Time (Selected Miners)", fontsize=13, y=1.01)
    plt.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return str(out_path)
