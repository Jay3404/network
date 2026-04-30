#!/usr/bin/env python3
"""
Week 1 Azure Functions trace analysis and baseline warm policy simulation.

Scope:
- Azure Functions Trace 2019 per-minute invocation analysis
- Burst statistics for global, app, and function workload series
- App-level reactive/static/local_predictive warming baselines
"""

from __future__ import annotations

import argparse
import csv
import gc
import re
import warnings
from pathlib import Path
from typing import Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
AZURE_2019_EXTRACTED = ROOT_DIR / "data" / "azure_functions_2019" / "extracted"
AZURE_2021_EXTRACTED = ROOT_DIR / "data" / "azure_functions_2021" / "extracted"
OUTPUT_ROOT = ROOT_DIR / "outputs" / "week1"

META_COLUMNS = ["HashOwner", "HashApp", "HashFunction", "Trigger"]
INVOCATION_PATTERN = "invocations_per_function_md.anon.d*.csv"
DEFAULT_CHUNKSIZE = 5000
DEFAULT_PREDICTIVE_VARIANTS = ",".join(
    [
        "ma:15",
        "ma:60",
        "ma:180",
        "ewma:0.3",
        "mean_std:60:1",
        "p95:60",
        "max_ma:5:60",
        "max_ma_seasonal:60:1440",
    ]
)
DEFAULT_ML_FORECAST_MODELS = "local_mean_std,tcn,lstm,lightgbm"

pd = None
np = None
plt = None


def log(message: str) -> None:
    print(f"[week1] {message}", flush=True)


def ensure_base_dirs() -> None:
    for path in [
        ROOT_DIR / "code",
        OUTPUT_ROOT,
        OUTPUT_ROOT / "global_2019",
        OUTPUT_ROOT / "app_2019",
        OUTPUT_ROOT / "function_2019",
        OUTPUT_ROOT / "analysis_2019",
        OUTPUT_ROOT / "baseline_2019",
        OUTPUT_ROOT / "predictive_sweep_2019",
        OUTPUT_ROOT / "ml_forecast_2019",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def require_analysis_dependencies(include_plotting: bool = False) -> None:
    global np, pd, plt

    missing: list[str] = []
    if pd is None:
        try:
            import pandas as pandas_module

            pd = pandas_module
        except ModuleNotFoundError:
            missing.append("pandas")

    if np is None:
        try:
            import numpy as numpy_module

            np = numpy_module
        except ModuleNotFoundError:
            missing.append("numpy")

    if include_plotting and plt is None:
        try:
            import matplotlib

            matplotlib.use("Agg")
            import matplotlib.pyplot as pyplot_module

            plt = pyplot_module
        except ModuleNotFoundError:
            missing.append("matplotlib")

    if missing:
        package_list = " ".join(sorted(set(missing)))
        raise RuntimeError(
            "Missing required Python package(s): "
            f"{package_list}. Install them with: pip install pandas numpy matplotlib"
        )


def get_invocation_files() -> list[Path]:
    files = list(AZURE_2019_EXTRACTED.rglob(INVOCATION_PATTERN))

    def day_key(path: Path) -> tuple[int, str]:
        match = re.search(r"\.d(\d+)\.csv$", path.name)
        day = int(match.group(1)) if match else 10**9
        return day, str(path)

    return sorted(files, key=day_key)


def read_header(path: Path) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as file:
        return next(csv.reader(file))


def get_minute_columns(path: Path) -> list[str]:
    columns = read_header(path)
    minute_columns = [col for col in columns if col.isdigit() and 1 <= int(col) <= 1440]
    return sorted(minute_columns, key=lambda value: int(value))


def file_day_number(path: Path, fallback_index: int) -> int:
    match = re.search(r"\.d(\d+)\.csv$", path.name)
    return int(match.group(1)) if match else fallback_index + 1


def add_time_columns(df: pd.DataFrame, day_number: int, day_index: int) -> pd.DataFrame:
    df = df.copy()
    df["day"] = day_number
    df["minute_of_day"] = df["minute_of_day"].astype(int)
    df["time_index"] = day_index * 1440 + df["minute_of_day"] - 1
    return df


def sanitize_filename(value: object, max_len: int = 32) -> str:
    text = str(value)
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
    return text[:max_len] if len(text) > max_len else text


def chunk_reader(
    path: Path,
    usecols: list[str],
    chunksize: int = DEFAULT_CHUNKSIZE,
) -> Iterable[pd.DataFrame]:
    return pd.read_csv(path, usecols=usecols, chunksize=chunksize)


def inspect_data() -> None:
    ensure_base_dirs()
    files = get_invocation_files()
    log(f"Azure 2019 extracted path: {AZURE_2019_EXTRACTED}")
    log(f"Azure 2021 extracted path: {AZURE_2021_EXTRACTED}")
    log(f"2019 invocation CSV count: {len(files)}")

    if not files:
        log("No 2019 invocation files found.")
        return

    first_file = files[0]
    columns = read_header(first_file)
    minute_columns = get_minute_columns(first_file)
    log(f"First 2019 invocation file: {first_file}")
    log(f"Total columns in sample file: {len(columns)}")
    log(f"Metadata columns: {[col for col in META_COLUMNS if col in columns]}")
    log(f"Minute column count: {len(minute_columns)}")
    log(f"First 12 columns: {columns[:12]}")
    log(f"Last 12 columns: {columns[-12:]}")

    files_2021 = list(AZURE_2021_EXTRACTED.rglob("*"))
    files_2021 = [path for path in files_2021 if path.is_file()]
    log(f"2021 extracted file count: {len(files_2021)}")
    for path in files_2021[:5]:
        log(f"2021 sample file: {path}")


def build_global_workload(files: list[Path]) -> pd.DataFrame:
    records: list[pd.DataFrame] = []
    for day_index, path in enumerate(files):
        day_number = file_day_number(path, day_index)
        minute_columns = get_minute_columns(path)
        log(f"Building global series from {path.name}")

        totals = np.zeros(len(minute_columns), dtype=np.float64)
        for chunk in chunk_reader(path, minute_columns):
            totals += chunk[minute_columns].sum(axis=0).to_numpy(dtype=np.float64)

        day_df = pd.DataFrame(
            {
                "entity_type": "global",
                "entity_id": "global",
                "minute_of_day": np.arange(1, len(minute_columns) + 1),
                "invocations": totals,
            }
        )
        records.append(add_time_columns(day_df, day_number, day_index))

    result = pd.concat(records, ignore_index=True)
    return result[["entity_type", "entity_id", "day", "minute_of_day", "time_index", "invocations"]]


def compute_top_entities(
    files: list[Path],
    entity_col: str,
    top_k: int,
    coverage_threshold: float | None = None,
) -> pd.DataFrame:
    totals: dict[str, float] = {}
    for path in files:
        minute_columns = get_minute_columns(path)
        usecols = [entity_col] + minute_columns
        log(f"Computing total invocations for {entity_col} from {path.name}")
        for chunk in chunk_reader(path, usecols):
            chunk_total = chunk[minute_columns].sum(axis=1)
            grouped = chunk_total.groupby(chunk[entity_col]).sum()
            for entity_id, value in grouped.items():
                key = str(entity_id)
                totals[key] = totals.get(key, 0.0) + float(value)

    top_df = pd.DataFrame(
        [{"entity_id": entity_id, "total_invocations": total} for entity_id, total in totals.items()]
    )
    if top_df.empty:
        return top_df

    top_df = top_df.sort_values("total_invocations", ascending=False).reset_index(drop=True)
    total_invocations = float(top_df["total_invocations"].sum())
    top_df["rank"] = np.arange(1, len(top_df) + 1)
    top_df["cumulative_invocations"] = top_df["total_invocations"].cumsum()
    top_df["coverage"] = (
        top_df["cumulative_invocations"] / total_invocations if total_invocations else 0.0
    )

    if coverage_threshold is not None:
        selected_count = int((top_df["coverage"] < coverage_threshold).sum()) + 1
        selected_count = min(selected_count, len(top_df))
        selected = top_df.head(selected_count)
        achieved = float(selected["coverage"].iloc[-1]) if not selected.empty else 0.0
        log(
            f"Selected {selected_count} {entity_col} entities for "
            f"{coverage_threshold:.3%} coverage target; achieved {achieved:.4%}"
        )
        return selected.reset_index(drop=True)

    return top_df.head(top_k).reset_index(drop=True)


def build_top_entity_workload(
    files: list[Path],
    entity_col: str,
    entity_type: str,
    top_k: int,
    coverage_threshold: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    top_entities = compute_top_entities(files, entity_col, top_k, coverage_threshold)
    if top_entities.empty:
        return pd.DataFrame(), top_entities

    top_ids = [str(value) for value in top_entities["entity_id"].tolist()]
    top_set = set(top_ids)
    selection_label = (
        f"{coverage_threshold:.3%} coverage"
        if coverage_threshold is not None
        else f"top-{top_k}"
    )
    records: list[pd.DataFrame] = []

    for day_index, path in enumerate(files):
        day_number = file_day_number(path, day_index)
        minute_columns = get_minute_columns(path)
        usecols = [entity_col] + minute_columns
        log(f"Building {selection_label} {entity_type} minute series from {path.name}")

        day_grouped: pd.DataFrame | None = None
        for chunk in chunk_reader(path, usecols):
            chunk[entity_col] = chunk[entity_col].astype(str)
            chunk = chunk[chunk[entity_col].isin(top_set)]
            if chunk.empty:
                continue

            grouped = chunk.groupby(entity_col, sort=False)[minute_columns].sum()
            day_grouped = grouped if day_grouped is None else day_grouped.add(grouped, fill_value=0)

        if day_grouped is None:
            day_grouped = pd.DataFrame(0.0, index=top_ids, columns=minute_columns)
        else:
            day_grouped = day_grouped.reindex(index=top_ids, columns=minute_columns, fill_value=0)

        day_grouped = day_grouped.copy()
        day_grouped.index.name = entity_col
        day_wide = day_grouped.reset_index().rename(columns={entity_col: "entity_id"})
        day_long = day_wide.melt(
            id_vars="entity_id",
            value_vars=minute_columns,
            var_name="minute_of_day",
            value_name="invocations",
        )
        day_long["entity_type"] = entity_type
        records.append(add_time_columns(day_long, day_number, day_index))

    result = pd.concat(records, ignore_index=True)
    result["invocations"] = pd.to_numeric(result["invocations"], errors="coerce").fillna(0.0)
    return (
        result[["entity_type", "entity_id", "day", "minute_of_day", "time_index", "invocations"]],
        top_entities,
    )


def add_burst_features(df: pd.DataFrame, window: int, z_value: float) -> pd.DataFrame:
    if df.empty:
        return df

    log(f"Computing burst statistics with window={window}, z={z_value}")
    output_frames: list[pd.DataFrame] = []
    safe_window = max(1, int(window))

    for (_, entity_id), group in df.groupby(["entity_type", "entity_id"], sort=False):
        group = group.sort_values("time_index").copy()
        invocations = group["invocations"].astype(float)
        rolling = invocations.rolling(window=safe_window, min_periods=1)

        group["rolling_mean"] = rolling.mean()
        group["rolling_std"] = rolling.std(ddof=0).fillna(0.0)
        group["z_threshold"] = group["rolling_mean"] + z_value * group["rolling_std"]
        group["is_burst_z"] = invocations > group["z_threshold"]
        group["rolling_q95"] = rolling.quantile(0.95)
        group["rolling_q99"] = rolling.quantile(0.99)
        group["is_burst_q95"] = invocations > group["rolling_q95"]
        group["is_burst_q99"] = invocations > group["rolling_q99"]

        mean_denominator = group["rolling_mean"].replace(0, np.nan)
        group["burst_intensity"] = invocations / mean_denominator
        group["cv_window"] = group["rolling_std"] / mean_denominator
        output_frames.append(group)

    return pd.concat(output_frames, ignore_index=True)


def summarize_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for (entity_type, entity_id), group in df.groupby(["entity_type", "entity_id"], sort=False):
        invocations = group["invocations"].astype(float)
        total_points = int(len(group))
        total_invocations = float(invocations.sum())
        mean_invocations = float(invocations.mean()) if total_points else 0.0
        std_invocations = float(invocations.std(ddof=0)) if total_points else 0.0
        cv = std_invocations / mean_invocations if mean_invocations > 0 else np.nan

        burst_intensity = group["burst_intensity"].replace([np.inf, -np.inf], np.nan)
        cv_window = group["cv_window"].replace([np.inf, -np.inf], np.nan)
        z_points = int(group["is_burst_z"].sum())
        q95_points = int(group["is_burst_q95"].sum())
        q99_points = int(group["is_burst_q99"].sum())

        rows.append(
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "total_points": total_points,
                "total_invocations": total_invocations,
                "mean_invocations": mean_invocations,
                "std_invocations": std_invocations,
                "cv": cv,
                "p50_invocations": float(invocations.quantile(0.50)),
                "p95_invocations": float(invocations.quantile(0.95)),
                "p99_invocations": float(invocations.quantile(0.99)),
                "max_invocations": float(invocations.max()),
                "z_burst_points": z_points,
                "z_burst_ratio": z_points / total_points if total_points else 0.0,
                "q95_burst_points": q95_points,
                "q95_burst_ratio": q95_points / total_points if total_points else 0.0,
                "q99_burst_points": q99_points,
                "q99_burst_ratio": q99_points / total_points if total_points else 0.0,
                "mean_burst_intensity": float(burst_intensity.mean(skipna=True)),
                "p95_burst_intensity": float(burst_intensity.quantile(0.95)),
                "max_burst_intensity": float(burst_intensity.max(skipna=True)),
                "mean_cv_window": float(cv_window.mean(skipna=True)),
            }
        )

    return pd.DataFrame(rows)


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log(f"Saved CSV: {path}")


def plot_single_burst(df: pd.DataFrame, output_path: Path, title: str) -> None:
    plot_df = df.sort_values("time_index")
    plt.figure(figsize=(14, 5))
    plt.plot(plot_df["time_index"], plot_df["invocations"], label="invocations", linewidth=1.0)
    plt.plot(plot_df["time_index"], plot_df["rolling_mean"], label="rolling_mean", linewidth=1.0)
    plt.plot(plot_df["time_index"], plot_df["z_threshold"], label="z_threshold", linewidth=0.9)

    burst_df = plot_df[plot_df["is_burst_z"]]
    if not burst_df.empty:
        plt.scatter(
            burst_df["time_index"],
            burst_df["invocations"],
            label="z burst",
            s=12,
            color="tab:red",
            alpha=0.75,
        )

    plt.title(title)
    plt.xlabel("Minute index")
    plt.ylabel("Invocations")
    plt.legend()
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150)
    plt.close()
    log(f"Saved plot: {output_path}")


def plot_entity_bursts(
    df: pd.DataFrame,
    output_dir: Path,
    prefix: str,
    max_plots: int,
) -> None:
    if df.empty:
        return

    entity_totals = (
        df.groupby("entity_id", sort=False)["invocations"]
        .sum()
        .sort_values(ascending=False)
        .head(max(1, max_plots))
    )
    top_ids = entity_totals.index.tolist()

    combined = df[df["entity_id"].isin(top_ids)].copy()
    plt.figure(figsize=(14, 6))
    for entity_id in top_ids:
        entity_df = combined[combined["entity_id"] == entity_id].sort_values("time_index")
        plt.plot(entity_df["time_index"], entity_df["invocations"], linewidth=0.9, label=sanitize_filename(entity_id, 10))
    plt.title(f"{prefix} top entity workload")
    plt.xlabel("Minute index")
    plt.ylabel("Invocations")
    plt.legend(ncol=2, fontsize=8)
    plt.tight_layout()
    combined_path = output_dir / f"{prefix}_top_entities_burst.png"
    plt.savefig(combined_path, dpi=150)
    plt.close()
    log(f"Saved plot: {combined_path}")

    for rank, entity_id in enumerate(top_ids, start=1):
        entity_df = df[df["entity_id"] == entity_id]
        filename = f"{prefix}_rank{rank:02d}_{sanitize_filename(entity_id)}_burst.png"
        plot_single_burst(entity_df, output_dir / filename, f"{prefix} rank {rank} burst")


def plot_baseline_bars(summary_df: pd.DataFrame, output_dir: Path) -> None:
    metrics = [
        ("avg_latency_ms", "Average latency (ms)"),
        ("cold_served_ratio", "Cold-served ratio"),
        ("total_resource_cost", "Total resource cost"),
    ]
    for metric, title in metrics:
        plt.figure(figsize=(max(8, len(summary_df) * 1.1), 5))
        colors = plt.cm.tab10(np.arange(len(summary_df)) % 10)
        plt.bar(summary_df["policy"], summary_df[metric], color=colors)
        plt.title(title)
        plt.xlabel("Policy")
        plt.ylabel(metric)
        plt.xticks(rotation=25, ha="right")
        plt.tight_layout()
        output_path = output_dir / f"baseline_{metric}.png"
        plt.savefig(output_path, dpi=150)
        plt.close()
        log(f"Saved plot: {output_path}")


def run_burst_pipeline(
    level: str,
    files: list[Path],
    output_dir: Path,
    top_k: int,
    window: int,
    z_value: float,
    max_plots: int,
    coverage_threshold: float | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)

    if level == "global":
        workload = build_global_workload(files)
        top_entities = pd.DataFrame([{"entity_id": "global", "total_invocations": workload["invocations"].sum()}])
    elif level == "app":
        workload, top_entities = build_top_entity_workload(
            files, "HashApp", "app", top_k, coverage_threshold
        )
        save_csv(top_entities, output_dir / "top_apps.csv")
    elif level == "function":
        workload, top_entities = build_top_entity_workload(
            files, "HashFunction", "function", top_k, coverage_threshold
        )
        save_csv(top_entities, output_dir / "top_functions.csv")
    else:
        raise ValueError(f"Unsupported analysis level: {level}")

    burst_df = add_burst_features(workload, window, z_value)
    summary_df = summarize_metrics(burst_df)

    save_csv(workload, output_dir / f"{level}_workload.csv")
    save_csv(burst_df, output_dir / f"{level}_burst_timeseries.csv")
    save_csv(summary_df, output_dir / f"{level}_summary_metrics.csv")

    if level == "global":
        plot_single_burst(burst_df, output_dir / "global_burst.png", "Global Azure Functions 2019 burst")
    else:
        plot_entity_bursts(burst_df, output_dir, level, max_plots)

    return burst_df, summary_df


def compute_local_prediction(df: pd.DataFrame, variant: dict[str, object]) -> pd.Series:
    model = str(variant["model"])
    grouped = df.groupby("entity_id", sort=False)["invocations"]

    if model == "ma":
        window = int(variant["window"])
        return grouped.transform(
            lambda series: series.shift(1).rolling(window, min_periods=1).mean()
        ).fillna(0.0)

    if model == "ewma":
        alpha = float(variant["alpha"])
        return grouped.transform(
            lambda series: series.shift(1).ewm(alpha=alpha, adjust=False, min_periods=1).mean()
        ).fillna(0.0)

    if model == "mean_std":
        window = int(variant["window"])
        z_value = float(variant["z"])

        def mean_std(series: pd.Series) -> pd.Series:
            shifted = series.shift(1)
            rolling = shifted.rolling(window, min_periods=1)
            return rolling.mean() + z_value * rolling.std(ddof=0).fillna(0.0)

        return grouped.transform(mean_std).fillna(0.0)

    if model == "p95":
        window = int(variant["window"])
        quantile = float(variant["quantile"])
        return grouped.transform(
            lambda series: series.shift(1).rolling(window, min_periods=1).quantile(quantile)
        ).fillna(0.0)

    if model == "max_ma":
        short_window = int(variant["short_window"])
        long_window = int(variant["long_window"])

        def max_ma(series: pd.Series) -> pd.Series:
            shifted = series.shift(1)
            short_ma = shifted.rolling(short_window, min_periods=1).mean()
            long_ma = shifted.rolling(long_window, min_periods=1).mean()
            return pd.concat([short_ma, long_ma], axis=1).max(axis=1)

        return grouped.transform(max_ma).fillna(0.0)

    if model == "seasonal":
        lag = int(variant["lag"])
        return grouped.transform(lambda series: series.shift(lag)).fillna(0.0)

    if model == "max_ma_seasonal":
        window = int(variant["window"])
        lag = int(variant["lag"])

        def max_ma_seasonal(series: pd.Series) -> pd.Series:
            shifted = series.shift(1)
            moving_average = shifted.rolling(window, min_periods=1).mean()
            seasonal = series.shift(lag)
            return pd.concat([moving_average, seasonal], axis=1).max(axis=1)

        return grouped.transform(max_ma_seasonal).fillna(0.0)

    raise ValueError(f"Unsupported predictive variant model: {model}")


def apply_policy(
    workload: pd.DataFrame,
    policy: str,
    capacity_per_instance: int,
    cold_start_penalty_ms: float,
    execution_ms: float,
    static_warm_instances: int,
    prediction_window: int,
    predictive_variant: dict[str, object] | None = None,
    policy_label: str | None = None,
) -> pd.DataFrame:
    df = workload.sort_values(["entity_id", "time_index"]).copy()
    invocations = df["invocations"].astype(float)

    if policy == "reactive":
        df["warm_instances"] = 0
    elif policy == "static":
        df["warm_instances"] = max(0, int(static_warm_instances))
    elif policy == "local_predictive":
        safe_window = max(1, int(prediction_window))
        variant = predictive_variant or {"model": "ma", "window": safe_window}
        predicted = compute_local_prediction(df, variant).clip(lower=0.0)
        df["predicted_invocations"] = predicted
        warm_instances = np.ceil(predicted / capacity_per_instance).astype(int)
        df["warm_instances"] = np.maximum(warm_instances, 1)
    else:
        raise ValueError(f"Unsupported policy: {policy}")

    if policy != "local_predictive":
        df["predicted_invocations"] = np.nan

    df["warm_capacity"] = df["warm_instances"] * capacity_per_instance
    df["warm_served"] = np.minimum(invocations, df["warm_capacity"])
    df["cold_served"] = np.maximum(0.0, invocations - df["warm_capacity"])

    inv_array = invocations.to_numpy(dtype=np.float64)
    active = inv_array > 0
    df["cold_start_rate"] = np.divide(
        df["cold_served"].to_numpy(dtype=np.float64),
        inv_array,
        out=np.zeros_like(inv_array, dtype=np.float64),
        where=active,
    )
    numerator = (
        df["warm_served"] * execution_ms
        + df["cold_served"] * (execution_ms + cold_start_penalty_ms)
    )
    df["avg_latency_ms"] = np.divide(
        numerator.to_numpy(dtype=np.float64),
        inv_array,
        out=np.zeros_like(inv_array, dtype=np.float64),
        where=active,
    )
    df["resource_cost"] = df["warm_instances"]
    df["policy"] = policy_label or policy

    return df


def summarize_baselines(baseline_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for policy, group in baseline_df.groupby("policy", sort=False):
        invocations = group["invocations"].astype(float)
        active = group[invocations > 0]
        total_invocations = float(invocations.sum())
        total_cold_served = float(group["cold_served"].sum())
        weighted_avg_latency_ms = (
            float((group["avg_latency_ms"] * invocations).sum() / total_invocations)
            if total_invocations
            else 0.0
        )

        rows.append(
            {
                "policy": policy,
                "total_invocations": total_invocations,
                "total_cold_served": total_cold_served,
                "cold_served_ratio": total_cold_served / total_invocations if total_invocations else 0.0,
                "avg_cold_start_rate": float(active["cold_start_rate"].mean()) if not active.empty else 0.0,
                "avg_latency_ms": float(active["avg_latency_ms"].mean()) if not active.empty else 0.0,
                "weighted_avg_latency_ms": weighted_avg_latency_ms,
                "p95_latency_ms": float(active["avg_latency_ms"].quantile(0.95)) if not active.empty else 0.0,
                "p99_latency_ms": float(active["avg_latency_ms"].quantile(0.99)) if not active.empty else 0.0,
                "avg_warm_instances": float(group["warm_instances"].mean()),
                "total_resource_cost": float(group["resource_cost"].sum()),
            }
        )

    return pd.DataFrame(rows)


def run_baseline_2019(
    args: argparse.Namespace,
    files: list[Path],
    output_dir: Path,
    entity_col: str = "HashApp",
    entity_type: str = "app",
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.coverage_threshold is None:
        log(f"Building {entity_type}-level top-k workload for baseline policies")
    else:
        log(
            f"Building {entity_type}-level {args.coverage_threshold:.3%} coverage "
            "workload for baseline policies"
        )
    workload, top_entities = build_top_entity_workload(
        files, entity_col, entity_type, args.top_k, args.coverage_threshold
    )
    top_filename = "top_apps.csv" if entity_type == "app" else "top_functions.csv"
    save_csv(top_entities, output_dir / top_filename)
    if args.save_baseline_workload:
        save_csv(workload, output_dir / f"{entity_type}_workload.csv")

    policies = ["reactive", "static", "local_predictive"]
    summary_frames: list[pd.DataFrame] = []
    for policy in policies:
        log(f"Running baseline policy: {policy}")
        policy_df = apply_policy(
            workload=workload,
            policy=policy,
            capacity_per_instance=args.capacity,
            cold_start_penalty_ms=args.cold_start_penalty,
            execution_ms=args.execution_ms,
            static_warm_instances=args.static_warm,
            prediction_window=args.prediction_window,
        )
        summary_frames.append(summarize_baselines(policy_df))
        if args.save_baseline_timeseries:
            save_csv(policy_df, output_dir / f"baseline_policy_timeseries_{policy}.csv")

    summary_df = pd.concat(summary_frames, ignore_index=True)

    save_csv(summary_df, output_dir / "baseline_policy_summary.csv")
    plot_baseline_bars(summary_df, output_dir)

    log("Running static warm instance sensitivity analysis")
    static_summary_frames: list[pd.DataFrame] = []
    for static_warm in parse_int_list(args.static_warm_values):
        policy_df = apply_policy(
            workload=workload,
            policy="static",
            capacity_per_instance=args.capacity,
            cold_start_penalty_ms=args.cold_start_penalty,
            execution_ms=args.execution_ms,
            static_warm_instances=static_warm,
            prediction_window=args.prediction_window,
        )
        policy_df["policy"] = f"static_{static_warm}"
        policy_df["static_warm_instances"] = static_warm
        policy_summary = summarize_baselines(policy_df)
        policy_summary["static_warm_instances"] = static_warm
        static_summary_frames.append(policy_summary)

    static_summary_df = pd.concat(static_summary_frames, ignore_index=True)
    ordered_columns = ["policy", "static_warm_instances"] + [
        col for col in static_summary_df.columns if col not in {"policy", "static_warm_instances"}
    ]
    static_summary_df = static_summary_df[ordered_columns]
    save_csv(static_summary_df, output_dir / "static_warm_sensitivity.csv")


def run_predictive_sweep_2019(
    args: argparse.Namespace,
    files: list[Path],
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    coverage = args.coverage_threshold if args.coverage_threshold is not None else 0.99
    log(f"Building app-level {coverage:.3%} coverage workload for local predictive variants")
    workload, top_entities = build_top_entity_workload(files, "HashApp", "app", args.top_k, coverage)
    save_csv(top_entities, output_dir / "top_apps.csv")

    variants = parse_predictive_variants(args.predictive_variants)
    summary_frames: list[pd.DataFrame] = []
    for variant in variants:
        label = predictive_variant_label(variant)
        log(f"Running local predictive variant: {label}")
        policy_df = apply_policy(
            workload=workload,
            policy="local_predictive",
            capacity_per_instance=args.capacity,
            cold_start_penalty_ms=args.cold_start_penalty,
            execution_ms=args.execution_ms,
            static_warm_instances=args.static_warm,
            prediction_window=args.prediction_window,
            predictive_variant=variant,
            policy_label=label,
        )
        summary = summarize_baselines(policy_df)
        for key, value in variant.items():
            summary[f"variant_{key}"] = value
        summary_frames.append(summary)
        del policy_df
        gc.collect()

    summary_df = pd.concat(summary_frames, ignore_index=True)
    variant_columns = [col for col in summary_df.columns if col.startswith("variant_")]
    metric_columns = [col for col in summary_df.columns if col not in {"policy", *variant_columns}]
    summary_df = summary_df[["policy"] + variant_columns + metric_columns]
    save_csv(summary_df, output_dir / "predictive_variant_summary.csv")
    plot_baseline_bars(summary_df, output_dir)


def workload_to_matrix(
    workload: pd.DataFrame,
    top_entities: pd.DataFrame,
) -> tuple[np.ndarray, list[str]]:
    entity_ids = [str(value) for value in top_entities["entity_id"].tolist()]
    n_entities = len(entity_ids)
    n_times = int(workload["time_index"].max()) + 1
    matrix = np.zeros((n_entities, n_times), dtype=np.float32)

    entity_codes = pd.Categorical(
        workload["entity_id"].astype(str),
        categories=entity_ids,
        ordered=True,
    ).codes
    valid = entity_codes >= 0
    time_index = workload["time_index"].to_numpy(dtype=np.int64)
    invocations = workload["invocations"].to_numpy(dtype=np.float32)
    matrix[entity_codes[valid], time_index[valid]] = invocations[valid]
    return matrix, entity_ids


def summarize_forecast_arrays(
    policy: str,
    actual: np.ndarray,
    predicted: np.ndarray,
    capacity_per_instance: int,
    cold_start_penalty_ms: float,
    execution_ms: float,
) -> dict[str, object]:
    predicted = np.maximum(predicted, 0.0)
    warm_instances = np.maximum(
        np.ceil(predicted / capacity_per_instance).astype(np.float32),
        1.0,
    )
    warm_capacity = warm_instances * capacity_per_instance
    cold_served = np.maximum(0.0, actual - warm_capacity)
    warm_served = np.minimum(actual, warm_capacity)
    active = actual > 0
    total_invocations = float(actual.sum(dtype=np.float64))
    total_cold_served = float(cold_served.sum(dtype=np.float64))
    numerator = warm_served * execution_ms + cold_served * (execution_ms + cold_start_penalty_ms)

    avg_latency = np.divide(
        numerator,
        actual,
        out=np.zeros_like(actual, dtype=np.float32),
        where=active,
    )
    cold_start_rate = np.divide(
        cold_served,
        actual,
        out=np.zeros_like(actual, dtype=np.float32),
        where=active,
    )
    active_latency = avg_latency[active]
    active_cold_rate = cold_start_rate[active]

    return {
        "policy": policy,
        "total_invocations": total_invocations,
        "total_cold_served": total_cold_served,
        "cold_served_ratio": total_cold_served / total_invocations if total_invocations else 0.0,
        "avg_cold_start_rate": float(active_cold_rate.mean()) if active_cold_rate.size else 0.0,
        "avg_latency_ms": float(active_latency.mean()) if active_latency.size else 0.0,
        "weighted_avg_latency_ms": (
            float(numerator.sum(dtype=np.float64) / total_invocations) if total_invocations else 0.0
        ),
        "p95_latency_ms": float(np.quantile(active_latency, 0.95)) if active_latency.size else 0.0,
        "p99_latency_ms": float(np.quantile(active_latency, 0.99)) if active_latency.size else 0.0,
        "avg_warm_instances": float(warm_instances.mean(dtype=np.float64)),
        "total_resource_cost": float(warm_instances.sum(dtype=np.float64)),
    }


def local_mean_std_matrix_prediction(
    values: np.ndarray,
    eval_start: int,
    eval_end: int,
    window: int,
    z_value: float,
    chunk_size: int,
) -> np.ndarray:
    eval_times = np.arange(eval_start, eval_end)
    start_times = np.maximum(0, eval_times - window)
    counts = np.maximum(1, eval_times - start_times).astype(np.float32)
    prediction = np.empty((values.shape[0], eval_end - eval_start), dtype=np.float32)

    for start in range(0, values.shape[0], chunk_size):
        end = min(start + chunk_size, values.shape[0])
        chunk = values[start:end].astype(np.float32, copy=False)
        cumsum = np.cumsum(chunk, axis=1, dtype=np.float32)
        cumsum = np.concatenate([np.zeros((chunk.shape[0], 1), dtype=np.float32), cumsum], axis=1)
        cumsum_sq = np.cumsum(chunk * chunk, axis=1, dtype=np.float32)
        cumsum_sq = np.concatenate([np.zeros((chunk.shape[0], 1), dtype=np.float32), cumsum_sq], axis=1)

        sums = cumsum[:, eval_times] - cumsum[:, start_times]
        sums_sq = cumsum_sq[:, eval_times] - cumsum_sq[:, start_times]
        means = sums / counts
        variance = np.maximum(sums_sq / counts - means * means, 0.0)
        prediction[start:end] = means + z_value * np.sqrt(variance)

    return prediction


def build_training_windows(
    log_values: np.ndarray,
    train_end: int,
    seq_len: int,
    sample_count: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    app_indices = rng.integers(0, log_values.shape[0], size=sample_count)
    target_times = rng.integers(seq_len, train_end, size=sample_count)
    windows = np.lib.stride_tricks.sliding_window_view(log_values[:, :train_end], seq_len, axis=1)
    window_starts = target_times - seq_len
    x_train = windows[app_indices, window_starts].astype(np.float32, copy=True)
    y_train = log_values[app_indices, target_times].astype(np.float32, copy=True)
    return x_train, y_train, app_indices, target_times


def build_lightgbm_features(
    windows: np.ndarray,
    seasonal_values: np.ndarray,
    target_times: np.ndarray,
) -> np.ndarray:
    minute_of_day = (target_times % 1440).astype(np.float32)
    return np.column_stack(
        [
            windows[:, -1],
            windows[:, -5:].mean(axis=1),
            windows[:, -15:].mean(axis=1),
            windows.mean(axis=1),
            windows.std(axis=1),
            np.quantile(windows, 0.95, axis=1),
            windows.max(axis=1),
            seasonal_values,
            np.sin(2 * np.pi * minute_of_day / 1440.0),
            np.cos(2 * np.pi * minute_of_day / 1440.0),
        ]
    ).astype(np.float32)


def train_torch_forecaster(
    model_name: str,
    x_train: np.ndarray,
    y_train: np.ndarray,
    epochs: int,
    batch_size: int,
    learning_rate: float,
    residual_quantile: float,
    seed: int,
):
    import torch
    from torch import nn

    torch.manual_seed(seed)
    torch.set_num_threads(max(1, min(4, torch.get_num_threads())))

    class LSTMForecaster(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.lstm = nn.LSTM(input_size=1, hidden_size=32, num_layers=1, batch_first=True)
            self.head = nn.Linear(32, 1)

        def forward_sequence(self, x):
            output, _ = self.lstm(x)
            return self.head(output).squeeze(-1)

        def forward(self, x):
            return self.forward_sequence(x)[:, -1]

    class Chomp1d(nn.Module):
        def __init__(self, chomp_size: int) -> None:
            super().__init__()
            self.chomp_size = chomp_size

        def forward(self, x):
            return x[:, :, :-self.chomp_size] if self.chomp_size else x

    class TCNForecaster(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            layers = []
            in_channels = 1
            hidden_channels = 32
            for dilation in [1, 2, 4, 8, 16, 32]:
                padding = 2 * dilation
                layers.extend(
                    [
                        nn.Conv1d(
                            in_channels,
                            hidden_channels,
                            kernel_size=3,
                            padding=padding,
                            dilation=dilation,
                        ),
                        Chomp1d(padding),
                        nn.ReLU(),
                    ]
                )
                in_channels = hidden_channels
            self.network = nn.Sequential(*layers)
            self.head = nn.Conv1d(hidden_channels, 1, kernel_size=1)

        def forward_sequence(self, x):
            x = x.transpose(1, 2)
            return self.head(self.network(x)).squeeze(1)

        def forward(self, x):
            return self.forward_sequence(x)[:, -1]

    if model_name == "lstm":
        model = LSTMForecaster()
    elif model_name == "tcn":
        model = TCNForecaster()
    else:
        raise ValueError(f"Unsupported torch model: {model_name}")

    log_mean = float(x_train.mean())
    log_std = float(x_train.std() + 1e-6)
    model.log_mean = log_mean
    model.log_std = log_std

    x_tensor = torch.from_numpy((x_train - log_mean) / log_std).unsqueeze(-1)
    y_tensor = torch.from_numpy((y_train - log_mean) / log_std)
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
    loss_fn = nn.MSELoss()

    model.train()
    n_samples = x_tensor.shape[0]
    for epoch in range(epochs):
        permutation = torch.randperm(n_samples)
        total_loss = 0.0
        for start in range(0, n_samples, batch_size):
            batch_index = permutation[start : start + batch_size]
            optimizer.zero_grad()
            prediction = model(x_tensor[batch_index])
            loss = loss_fn(prediction, y_tensor[batch_index])
            loss.backward()
            optimizer.step()
            total_loss += float(loss.item()) * len(batch_index)
        log(f"{model_name.upper()} epoch {epoch + 1}/{epochs} mse={total_loss / n_samples:.6f}")

    model.eval()
    train_predictions: list[np.ndarray] = []
    with torch.no_grad():
        for start in range(0, n_samples, batch_size):
            prediction = model(x_tensor[start : start + batch_size]).cpu().numpy()
            train_predictions.append(prediction * log_std + log_mean)
    train_prediction = np.concatenate(train_predictions)
    residual = y_train - train_prediction
    model.residual_buffer = max(0.0, float(np.quantile(residual, residual_quantile)))
    log(
        f"{model_name.upper()} log-residual q{residual_quantile:.2f} "
        f"buffer={model.residual_buffer:.4f}"
    )

    return model


def predict_torch_sequence(
    model,
    log_values: np.ndarray,
    eval_start: int,
    eval_end: int,
    chunk_size: int,
    batch_size: int,
) -> np.ndarray:
    import torch

    prediction = np.empty((log_values.shape[0], eval_end - eval_start), dtype=np.float32)
    model.eval()
    with torch.no_grad():
        for start in range(0, log_values.shape[0], chunk_size):
            end = min(start + chunk_size, log_values.shape[0])
            chunk_predictions: list[np.ndarray] = []
            for batch_start in range(start, end, batch_size):
                batch_end = min(batch_start + batch_size, end)
                x_batch = torch.from_numpy(log_values[batch_start:batch_end]).unsqueeze(-1)
                normalized_batch = (x_batch - model.log_mean) / model.log_std
                predicted_log_sequence = (
                    model.forward_sequence(normalized_batch).cpu().numpy() * model.log_std
                    + model.log_mean
                    + model.residual_buffer
                )
                chunk_predictions.append(predicted_log_sequence[:, eval_start - 1 : eval_end - 1])
            prediction[start:end] = np.vstack(chunk_predictions)
            log(f"Predicted {end}/{log_values.shape[0]} apps with torch model")

    return np.expm1(prediction).clip(min=0.0).astype(np.float32)


def predict_lightgbm(
    model,
    log_values: np.ndarray,
    eval_start: int,
    eval_end: int,
    seq_len: int,
    chunk_size: int,
    residual_buffer: float,
) -> np.ndarray:
    prediction = np.empty((log_values.shape[0], eval_end - eval_start), dtype=np.float32)
    eval_times = np.arange(eval_start, eval_end)
    for start in range(0, log_values.shape[0], chunk_size):
        end = min(start + chunk_size, log_values.shape[0])
        chunk = log_values[start:end]
        windows = np.lib.stride_tricks.sliding_window_view(chunk, seq_len, axis=1)
        eval_windows = windows[:, eval_start - seq_len : eval_end - seq_len].reshape(-1, seq_len)
        seasonal = chunk[:, eval_times - 1440].reshape(-1)
        target_times = np.tile(eval_times, end - start)
        features = build_lightgbm_features(eval_windows, seasonal, target_times)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            predicted_log = model.predict(features) + residual_buffer
        prediction[start:end] = np.expm1(predicted_log).reshape(end - start, -1)
        log(f"Predicted {end}/{log_values.shape[0]} apps with LightGBM")

    return prediction.clip(min=0.0).astype(np.float32)


def resolve_ml_train_end(args: argparse.Namespace, total_minutes: int) -> int:
    if args.ml_train_ratio is not None:
        return int(total_minutes * args.ml_train_ratio)
    return int(args.ml_train_days) * 1440


def validate_ml_window(
    train_end: int,
    eval_start: int,
    eval_end: int,
    seq_len: int,
) -> None:
    if train_end <= seq_len:
        raise ValueError("ML training window must leave more training points than --ml-seq-len")
    if eval_start >= eval_end:
        raise ValueError("ML evaluation window is empty")
    if eval_start < 1440:
        raise ValueError("ML evaluation must start after at least 1 day for seasonal LightGBM features")


def ml_window_metadata(
    split_method: str,
    train_end: int,
    eval_start: int,
    eval_end: int,
    seq_len: int,
    args: argparse.Namespace,
) -> dict[str, object]:
    return {
        "split_method": split_method,
        "train_minutes": train_end,
        "train_days": train_end / 1440,
        "eval_start_minute": eval_start,
        "eval_end_minute": eval_end,
        "eval_minutes": eval_end - eval_start,
        "eval_days": (eval_end - eval_start) / 1440,
        "seq_len": seq_len,
        "train_samples": args.ml_train_samples,
        "ml_residual_quantile": args.ml_residual_quantile,
    }


def run_ml_forecast_window(
    args: argparse.Namespace,
    values: np.ndarray,
    log_values: np.ndarray,
    train_end: int,
    eval_start: int,
    eval_end: int,
    split_method: str,
    return_predictions: bool = False,
) -> tuple[list[dict[str, object]], dict[str, np.ndarray]]:
    seq_len = int(args.ml_seq_len)
    validate_ml_window(train_end, eval_start, eval_end, seq_len)

    actual = values[:, eval_start:eval_end]
    models = [item.strip() for item in args.ml_models.split(",") if item.strip()]

    rows: list[dict[str, object]] = []
    predictions: dict[str, np.ndarray] = {}
    metadata = ml_window_metadata(split_method, train_end, eval_start, eval_end, seq_len, args)
    if "local_mean_std" in models:
        log(f"Running local_mean_std_60_z1 on {split_method} evaluation window")
        prediction = local_mean_std_matrix_prediction(
            values,
            eval_start,
            eval_end,
            window=60,
            z_value=1.0,
            chunk_size=args.ml_app_chunk_size,
        )
        rows.append(
            summarize_forecast_arrays(
                "local_mean_std_60_z1",
                actual,
                prediction,
                args.capacity,
                args.cold_start_penalty,
                args.execution_ms,
            )
        )
        rows[-1].update(metadata)
        if return_predictions:
            predictions["local_mean_std_60_z1"] = prediction
        else:
            del prediction
            gc.collect()

    needs_torch = any(model in {"tcn", "lstm"} for model in models)
    x_train = y_train = app_indices = target_times = None
    if needs_torch or "lightgbm" in models:
        log(
            f"Sampling {args.ml_train_samples} training windows "
            f"(seq_len={seq_len}, train_days={train_end / 1440:.3f})"
        )
        x_train, y_train, app_indices, target_times = build_training_windows(
            log_values,
            train_end,
            seq_len,
            args.ml_train_samples,
            args.ml_random_seed,
        )

    for model_name in ["tcn", "lstm"]:
        if model_name not in models:
            continue
        log(f"Training {model_name.upper()} rolling forecast baseline")
        model = train_torch_forecaster(
            model_name,
            x_train,
            y_train,
            args.ml_epochs,
            args.ml_batch_size,
            args.ml_learning_rate,
            args.ml_residual_quantile,
            args.ml_random_seed,
        )
        prediction = predict_torch_sequence(
            model,
            log_values,
            eval_start,
            eval_end,
            chunk_size=args.ml_app_chunk_size,
            batch_size=args.ml_inference_batch_size,
        )
        rows.append(
            summarize_forecast_arrays(
                model_name,
                actual,
                prediction,
                args.capacity,
                args.cold_start_penalty,
                args.execution_ms,
            )
        )
        rows[-1].update(metadata)
        if return_predictions:
            predictions[model_name] = prediction
            del model
        else:
            del model, prediction
            gc.collect()

    if "lightgbm" in models:
        import lightgbm as lgb

        log("Training LightGBM rolling forecast baseline")
        seasonal_train = np.where(
            target_times >= 1440,
            log_values[app_indices, target_times - 1440],
            0.0,
        ).astype(np.float32)
        x_features = build_lightgbm_features(x_train, seasonal_train, target_times)
        model = lgb.LGBMRegressor(
            objective="regression",
            n_estimators=args.lightgbm_estimators,
            learning_rate=0.05,
            num_leaves=31,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=args.ml_random_seed,
            n_jobs=-1,
            verbosity=-1,
        )
        model.fit(x_features, y_train)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            train_prediction = model.predict(x_features)
        residual_buffer = max(
            0.0,
            float(np.quantile(y_train - train_prediction, args.ml_residual_quantile)),
        )
        log(
            f"LightGBM log-residual q{args.ml_residual_quantile:.2f} "
            f"buffer={residual_buffer:.4f}"
        )
        prediction = predict_lightgbm(
            model,
            log_values,
            eval_start,
            eval_end,
            seq_len,
            chunk_size=args.ml_app_chunk_size,
            residual_buffer=residual_buffer,
        )
        rows.append(
            summarize_forecast_arrays(
                "lightgbm",
                actual,
                prediction,
                args.capacity,
                args.cold_start_penalty,
                args.execution_ms,
            )
        )
        rows[-1].update(metadata)
        if return_predictions:
            predictions["lightgbm"] = prediction
            del model, x_features
        else:
            del model, prediction, x_features
            gc.collect()

    return rows, predictions


def iter_rolling_windows(
    total_minutes: int,
    initial_train_end: int,
    eval_days: float,
    step_days: float,
    max_folds: int,
) -> Iterable[tuple[int, int, int]]:
    eval_minutes = max(1, int(eval_days * 1440))
    step_minutes = max(1, int(step_days * 1440))
    train_end = initial_train_end
    fold = 1

    while train_end < total_minutes:
        eval_start = train_end
        eval_end = min(eval_start + eval_minutes, total_minutes)
        if eval_start >= eval_end:
            break
        yield fold, train_end, eval_end
        if max_folds > 0 and fold >= max_folds:
            break
        train_end += step_minutes
        fold += 1


def summarize_rolling_predictions(
    rows_by_policy: dict[str, list[np.ndarray]],
    actual_windows: list[np.ndarray],
    args: argparse.Namespace,
    metadata: dict[str, object],
) -> pd.DataFrame:
    actual = np.concatenate(actual_windows, axis=1)
    rows: list[dict[str, object]] = []

    for policy, prediction_windows in rows_by_policy.items():
        prediction = np.concatenate(prediction_windows, axis=1)
        row = summarize_forecast_arrays(
            policy,
            actual,
            prediction,
            args.capacity,
            args.cold_start_penalty,
            args.execution_ms,
        )
        row.update(metadata)
        rows.append(row)
        del prediction
        gc.collect()

    del actual
    gc.collect()
    return pd.DataFrame(rows)


def run_ml_forecast_2019(
    args: argparse.Namespace,
    files: list[Path],
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    coverage = args.coverage_threshold
    if coverage is None:
        log(f"Building app-level top-{args.top_k} workload for ML forecast baselines")
    else:
        log(f"Building app-level {coverage:.3%} coverage workload for ML forecast baselines")
    workload, top_entities = build_top_entity_workload(files, "HashApp", "app", args.top_k, coverage)
    save_csv(top_entities, output_dir / "top_apps.csv")

    values, _ = workload_to_matrix(workload, top_entities)
    del workload
    gc.collect()

    seq_len = int(args.ml_seq_len)
    log_values = np.log1p(values).astype(np.float32)
    train_end = resolve_ml_train_end(args, values.shape[1])
    eval_start = train_end
    eval_end = values.shape[1]

    if args.ml_eval_mode in {"holdout", "both"}:
        rows, _ = run_ml_forecast_window(
            args,
            values,
            log_values,
            train_end,
            eval_start,
            eval_end,
            "chronological_holdout",
        )
        summary_df = pd.DataFrame(rows)
        save_csv(summary_df, output_dir / "ml_forecast_summary.csv")
        plot_baseline_bars(summary_df, output_dir)

    if args.ml_eval_mode in {"rolling", "both"}:
        log("Running expanding-window rolling-origin ML evaluation")
        fold_rows: list[dict[str, object]] = []
        actual_windows: list[np.ndarray] = []
        prediction_windows_by_policy: dict[str, list[np.ndarray]] = {}
        total_eval_minutes = 0

        for fold, fold_train_end, fold_eval_end in iter_rolling_windows(
            values.shape[1],
            train_end,
            args.ml_rolling_eval_days,
            args.ml_rolling_step_days,
            args.ml_rolling_max_folds,
        ):
            fold_eval_start = fold_train_end
            log(
                f"Rolling fold {fold}: train_minutes=0..{fold_train_end - 1}, "
                f"eval_minutes={fold_eval_start}..{fold_eval_end - 1}"
            )
            rows, predictions = run_ml_forecast_window(
                args,
                values,
                log_values,
                fold_train_end,
                fold_eval_start,
                fold_eval_end,
                "rolling_origin",
                return_predictions=True,
            )
            actual_windows.append(values[:, fold_eval_start:fold_eval_end])
            total_eval_minutes += fold_eval_end - fold_eval_start
            for row in rows:
                row["fold"] = fold
                fold_rows.append(row)
            for policy, prediction in predictions.items():
                prediction_windows_by_policy.setdefault(policy, []).append(prediction)
            gc.collect()

        if not fold_rows:
            raise ValueError("Rolling-origin evaluation produced no folds")

        fold_df = pd.DataFrame(fold_rows)
        save_csv(fold_df, output_dir / "ml_forecast_rolling_folds.csv")

        rolling_metadata = {
            "split_method": "rolling_origin",
            "initial_train_minutes": train_end,
            "initial_train_days": train_end / 1440,
            "rolling_eval_days": args.ml_rolling_eval_days,
            "rolling_step_days": args.ml_rolling_step_days,
            "rolling_folds": int(fold_df["fold"].max()),
            "eval_minutes": total_eval_minutes,
            "eval_days": total_eval_minutes / 1440,
            "seq_len": seq_len,
            "train_samples": args.ml_train_samples,
            "ml_residual_quantile": args.ml_residual_quantile,
        }
        rolling_summary_df = summarize_rolling_predictions(
            prediction_windows_by_policy,
            actual_windows,
            args,
            rolling_metadata,
        )
        save_csv(rolling_summary_df, output_dir / "ml_forecast_rolling_summary.csv")
        rolling_plot_dir = output_dir / "rolling_plots"
        rolling_plot_dir.mkdir(parents=True, exist_ok=True)
        plot_baseline_bars(rolling_summary_df, rolling_plot_dir)


def require_files(files: list[Path]) -> None:
    if files:
        return
    raise FileNotFoundError(
        f"No files matching {INVOCATION_PATTERN} were found under {AZURE_2019_EXTRACTED}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Azure Functions Trace 2019 week-1 burst analysis and warming baseline simulation."
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=[
            "inspect",
            "global_2019",
            "app_2019",
            "function_2019",
            "analysis_2019",
            "baseline_2019",
            "predictive_sweep_2019",
            "ml_forecast_2019",
        ],
    )
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument(
        "--coverage-threshold",
        type=float,
        default=None,
        help="Select the minimum number of entities whose cumulative invocation coverage reaches this threshold.",
    )
    parser.add_argument("--window", type=int, default=60)
    parser.add_argument("--z", type=float, default=3.0)
    parser.add_argument("--capacity", type=int, default=20)
    parser.add_argument("--cold-start-penalty", type=float, default=800)
    parser.add_argument("--execution-ms", type=float, default=100)
    parser.add_argument("--static-warm", type=int, default=1)
    parser.add_argument(
        "--static-warm-values",
        default="0,1,5,10,20,50,100,500,1000",
        help="Comma-separated static warm instance counts for sensitivity analysis.",
    )
    parser.add_argument("--prediction-window", type=int, default=60)
    parser.add_argument(
        "--predictive-variants",
        default=DEFAULT_PREDICTIVE_VARIANTS,
        help=(
            "Comma-separated local predictor specs. Supported examples: "
            "ma:60, ewma:0.3, mean_std:60:1, p95:60, max_ma:5:60, "
            "max_ma_seasonal:60:1440."
        ),
    )
    parser.add_argument("--ml-models", default=DEFAULT_ML_FORECAST_MODELS)
    parser.add_argument("--ml-seq-len", type=int, default=60)
    parser.add_argument("--ml-train-days", type=int, default=7)
    parser.add_argument(
        "--ml-train-ratio",
        type=float,
        default=None,
        help="Chronological train ratio for ML holdout/rolling start. Overrides --ml-train-days when set.",
    )
    parser.add_argument(
        "--ml-eval-mode",
        choices=["holdout", "rolling", "both"],
        default="holdout",
        help="Run chronological holdout, expanding-window rolling-origin evaluation, or both.",
    )
    parser.add_argument("--ml-train-samples", type=int, default=200000)
    parser.add_argument("--ml-epochs", type=int, default=2)
    parser.add_argument("--ml-batch-size", type=int, default=1024)
    parser.add_argument("--ml-inference-batch-size", type=int, default=64)
    parser.add_argument("--ml-app-chunk-size", type=int, default=128)
    parser.add_argument("--ml-learning-rate", type=float, default=0.001)
    parser.add_argument("--ml-residual-quantile", type=float, default=0.9)
    parser.add_argument("--ml-random-seed", type=int, default=42)
    parser.add_argument("--ml-rolling-eval-days", type=float, default=1.0)
    parser.add_argument("--ml-rolling-step-days", type=float, default=1.0)
    parser.add_argument(
        "--ml-rolling-max-folds",
        type=int,
        default=0,
        help="Maximum rolling-origin folds. 0 means use all folds until the trace ends.",
    )
    parser.add_argument("--lightgbm-estimators", type=int, default=200)
    parser.add_argument(
        "--save-baseline-workload",
        action="store_true",
        help="Save the expanded baseline workload CSV. This can be several GB for coverage-based app subsets.",
    )
    parser.add_argument(
        "--save-baseline-timeseries",
        action="store_true",
        help="Save per-policy baseline time series CSVs. This can be several GB for coverage-based app subsets.",
    )
    parser.add_argument("--max-plots", type=int, default=10)
    return parser.parse_args()


def parse_int_list(value: str) -> list[int]:
    items: list[int] = []
    for raw_item in value.split(","):
        raw_item = raw_item.strip()
        if not raw_item:
            continue
        item = int(raw_item)
        if item < 0:
            raise ValueError("--static-warm-values must contain non-negative integers")
        items.append(item)
    if not items:
        raise ValueError("--static-warm-values must contain at least one value")
    return items


def format_number_label(value: float) -> str:
    formatted = f"{value:g}"
    return formatted.replace(".", "p")


def parse_predictive_variant(raw_spec: str) -> dict[str, object]:
    parts = [part.strip() for part in raw_spec.split(":")]
    if not parts or not parts[0]:
        raise ValueError("Empty predictive variant")

    model = parts[0]
    if model == "ma" and len(parts) == 2:
        window = int(parts[1])
        if window <= 0:
            raise ValueError("ma window must be positive")
        return {"model": model, "window": window}

    if model == "ewma" and len(parts) == 2:
        alpha = float(parts[1])
        if not (0 < alpha <= 1):
            raise ValueError("ewma alpha must be in the range (0, 1]")
        return {"model": model, "alpha": alpha}

    if model == "mean_std" and len(parts) == 3:
        window = int(parts[1])
        z_value = float(parts[2])
        if window <= 0:
            raise ValueError("mean_std window must be positive")
        if z_value < 0:
            raise ValueError("mean_std z must be non-negative")
        return {"model": model, "window": window, "z": z_value}

    if model == "p95" and len(parts) == 2:
        window = int(parts[1])
        if window <= 0:
            raise ValueError("p95 window must be positive")
        return {"model": model, "window": window, "quantile": 0.95}

    if model == "max_ma" and len(parts) == 3:
        short_window = int(parts[1])
        long_window = int(parts[2])
        if short_window <= 0 or long_window <= 0:
            raise ValueError("max_ma windows must be positive")
        return {"model": model, "short_window": short_window, "long_window": long_window}

    if model == "seasonal" and len(parts) == 2:
        lag = int(parts[1])
        if lag <= 0:
            raise ValueError("seasonal lag must be positive")
        return {"model": model, "lag": lag}

    if model == "max_ma_seasonal" and len(parts) == 3:
        window = int(parts[1])
        lag = int(parts[2])
        if window <= 0 or lag <= 0:
            raise ValueError("max_ma_seasonal window and lag must be positive")
        return {"model": model, "window": window, "lag": lag}

    raise ValueError(f"Unsupported predictive variant: {raw_spec}")


def parse_predictive_variants(value: str) -> list[dict[str, object]]:
    variants = [parse_predictive_variant(item) for item in value.split(",") if item.strip()]
    if not variants:
        raise ValueError("--predictive-variants must contain at least one variant")
    return variants


def predictive_variant_label(variant: dict[str, object]) -> str:
    model = str(variant["model"])
    if model == "ma":
        return f"local_ma_{variant['window']}"
    if model == "ewma":
        return f"local_ewma_a{format_number_label(float(variant['alpha']))}"
    if model == "mean_std":
        return f"local_mean_std_{variant['window']}_z{format_number_label(float(variant['z']))}"
    if model == "p95":
        return f"local_p95_{variant['window']}"
    if model == "max_ma":
        return f"local_max_ma_{variant['short_window']}_{variant['long_window']}"
    if model == "seasonal":
        return f"local_seasonal_{variant['lag']}"
    if model == "max_ma_seasonal":
        return f"local_max_ma_{variant['window']}_seasonal_{variant['lag']}"
    raise ValueError(f"Unsupported predictive variant model: {model}")


def main() -> None:
    args = parse_args()
    ensure_base_dirs()

    if args.capacity <= 0:
        raise ValueError("--capacity must be positive")
    if args.top_k <= 0:
        raise ValueError("--top-k must be positive")
    if args.coverage_threshold is not None and not (0 < args.coverage_threshold <= 1):
        raise ValueError("--coverage-threshold must be in the range (0, 1]")
    if args.ml_seq_len <= 0:
        raise ValueError("--ml-seq-len must be positive")
    if args.ml_train_days <= 0:
        raise ValueError("--ml-train-days must be positive")
    if args.ml_train_ratio is not None and not (0 < args.ml_train_ratio < 1):
        raise ValueError("--ml-train-ratio must be in the range (0, 1)")
    if args.ml_train_samples <= 0:
        raise ValueError("--ml-train-samples must be positive")
    if args.ml_epochs <= 0:
        raise ValueError("--ml-epochs must be positive")
    if not (0 < args.ml_residual_quantile < 1):
        raise ValueError("--ml-residual-quantile must be in the range (0, 1)")
    if args.ml_rolling_eval_days <= 0:
        raise ValueError("--ml-rolling-eval-days must be positive")
    if args.ml_rolling_step_days <= 0:
        raise ValueError("--ml-rolling-step-days must be positive")
    if args.ml_rolling_max_folds < 0:
        raise ValueError("--ml-rolling-max-folds must be non-negative")

    if args.mode == "inspect":
        inspect_data()
        return

    require_analysis_dependencies(include_plotting=True)

    files = get_invocation_files()
    require_files(files)
    log(f"Found {len(files)} Azure 2019 invocation files")

    if args.mode == "global_2019":
        run_burst_pipeline(
            "global",
            files,
            OUTPUT_ROOT / "global_2019",
            args.top_k,
            args.window,
            args.z,
            args.max_plots,
            args.coverage_threshold,
        )
    elif args.mode == "app_2019":
        run_burst_pipeline(
            "app",
            files,
            OUTPUT_ROOT / "app_2019",
            args.top_k,
            args.window,
            args.z,
            args.max_plots,
            args.coverage_threshold,
        )
    elif args.mode == "function_2019":
        run_burst_pipeline(
            "function",
            files,
            OUTPUT_ROOT / "function_2019",
            args.top_k,
            args.window,
            args.z,
            args.max_plots,
            args.coverage_threshold,
        )
    elif args.mode == "analysis_2019":
        analysis_dir = OUTPUT_ROOT / "analysis_2019"
        _, global_summary = run_burst_pipeline(
            "global",
            files,
            analysis_dir / "global",
            args.top_k,
            args.window,
            args.z,
            args.max_plots,
            args.coverage_threshold,
        )
        _, app_summary = run_burst_pipeline(
            "app",
            files,
            analysis_dir / "app",
            args.top_k,
            args.window,
            args.z,
            args.max_plots,
            args.coverage_threshold,
        )
        _, function_summary = run_burst_pipeline(
            "function",
            files,
            analysis_dir / "function",
            args.top_k,
            args.window,
            args.z,
            args.max_plots,
            args.coverage_threshold,
        )
        combined_summary = pd.concat([global_summary, app_summary, function_summary], ignore_index=True)
        save_csv(combined_summary, analysis_dir / "combined_summary_metrics.csv")
    elif args.mode == "baseline_2019":
        run_baseline_2019(args, files, OUTPUT_ROOT / "baseline_2019")
    elif args.mode == "predictive_sweep_2019":
        run_predictive_sweep_2019(args, files, OUTPUT_ROOT / "predictive_sweep_2019")
    elif args.mode == "ml_forecast_2019":
        run_ml_forecast_2019(args, files, OUTPUT_ROOT / "ml_forecast_2019")
    else:
        raise ValueError(f"Unsupported mode: {args.mode}")

    log("Done")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        log(f"ERROR: {exc}")
        raise SystemExit(1) from exc


# README
#
# Purpose:
#   Week 1 implementation for Azure Functions trace cleanup, burst statistics,
#   and baseline warm policy evaluation.
#
# Data assumptions:
#   Azure 2019 extracted path:
#     data/azure_functions_2019/extracted
#   Azure 2021 extracted path:
#     data/azure_functions_2021/extracted
#
# Output directories:
#   outputs/week1/global_2019/
#   outputs/week1/app_2019/
#   outputs/week1/function_2019/
#   outputs/week1/analysis_2019/
#   outputs/week1/baseline_2019/
#
# Examples:
#   python code/azure_trace_week1.py --mode inspect
#   python code/azure_trace_week1.py --mode analysis_2019 --top-k 20 --window 60 --z 3
#   python code/azure_trace_week1.py --mode baseline_2019 --coverage-threshold 0.99 --capacity 20 --cold-start-penalty 800 --execution-ms 100 --static-warm 1 --prediction-window 60
#   python code/azure_trace_week1.py --mode predictive_sweep_2019 --coverage-threshold 0.99 --capacity 20 --cold-start-penalty 800 --execution-ms 100
#   python code/azure_trace_week1.py --mode ml_forecast_2019 --coverage-threshold 0.99 --capacity 20 --cold-start-penalty 800 --execution-ms 100 --ml-train-ratio 0.7 --ml-eval-mode both
