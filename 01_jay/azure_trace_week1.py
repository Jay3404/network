#!/usr/bin/env python3
"""
Week 1 Azure Functions trace analysis and baseline warm policy simulation.

Scope:
- Azure Functions Trace 2019 per-minute invocation analysis
- Burst statistics for global, app, and function workload series
- Function-level reactive/static/local_predictive warming baselines
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
AZURE_2019_EXTRACTED = ROOT_DIR / "data" / "azure_functions_2019" / "extracted"
AZURE_2021_EXTRACTED = ROOT_DIR / "data" / "azure_functions_2021" / "extracted"
OUTPUT_ROOT = ROOT_DIR / "outputs" / "week1"

META_COLUMNS = ["HashOwner", "HashApp", "HashFunction", "Trigger"]
INVOCATION_PATTERN = "invocations_per_function_md.anon.d*.csv"
DEFAULT_CHUNKSIZE = 5000

pd = None
np = None
plt = None


def log(message: str) -> None:
    print(f"[week1] {message}", flush=True)


def ensure_base_dirs() -> None:
    for path in [
        ROOT_DIR / "01_jay",
        OUTPUT_ROOT,
        OUTPUT_ROOT / "global_2019",
        OUTPUT_ROOT / "app_2019",
        OUTPUT_ROOT / "function_2019",
        OUTPUT_ROOT / "analysis_2019",
        OUTPUT_ROOT / "baseline_2019",
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


def compute_top_entities(files: list[Path], entity_col: str, top_k: int) -> pd.DataFrame:
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
    return top_df.sort_values("total_invocations", ascending=False).head(top_k).reset_index(drop=True)


def build_top_entity_workload(
    files: list[Path],
    entity_col: str,
    entity_type: str,
    top_k: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    top_entities = compute_top_entities(files, entity_col, top_k)
    if top_entities.empty:
        return pd.DataFrame(), top_entities

    top_ids = [str(value) for value in top_entities["entity_id"].tolist()]
    top_set = set(top_ids)
    records: list[pd.DataFrame] = []

    for day_index, path in enumerate(files):
        day_number = file_day_number(path, day_index)
        minute_columns = get_minute_columns(path)
        usecols = [entity_col] + minute_columns
        log(f"Building top-{top_k} {entity_type} minute series from {path.name}")

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
        plt.figure(figsize=(8, 5))
        plt.bar(summary_df["policy"], summary_df[metric], color=["#4C78A8", "#F58518", "#54A24B"])
        plt.title(title)
        plt.xlabel("Policy")
        plt.ylabel(metric)
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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)

    if level == "global":
        workload = build_global_workload(files)
        top_entities = pd.DataFrame([{"entity_id": "global", "total_invocations": workload["invocations"].sum()}])
    elif level == "app":
        workload, top_entities = build_top_entity_workload(files, "HashApp", "app", top_k)
        save_csv(top_entities, output_dir / "top_apps.csv")
    elif level == "function":
        workload, top_entities = build_top_entity_workload(files, "HashFunction", "function", top_k)
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


def apply_policy(
    workload: pd.DataFrame,
    policy: str,
    capacity_per_instance: int,
    cold_start_penalty_ms: float,
    execution_ms: float,
    static_warm_instances: int,
    prediction_window: int,
) -> pd.DataFrame:
    df = workload.sort_values(["entity_id", "time_index"]).copy()
    invocations = df["invocations"].astype(float)

    if policy == "reactive":
        df["warm_instances"] = 0
    elif policy == "static":
        df["warm_instances"] = max(0, int(static_warm_instances))
    elif policy == "local_predictive":
        safe_window = max(1, int(prediction_window))
        predicted = (
            df.groupby("entity_id", sort=False)["invocations"]
            .transform(lambda series: series.shift(1).rolling(safe_window, min_periods=1).mean())
            .fillna(0.0)
        )
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
    df["policy"] = policy

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
    entity_col: str = "HashFunction",
    entity_type: str = "function",
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    log(f"Building {entity_type}-level top-k workload for baseline policies")
    workload, top_entities = build_top_entity_workload(files, entity_col, entity_type, args.top_k)
    top_filename = "top_apps.csv" if entity_type == "app" else "top_functions.csv"
    save_csv(top_entities, output_dir / top_filename)
    save_csv(workload, output_dir / f"{entity_type}_workload.csv")

    policies = ["reactive", "static", "local_predictive"]
    policy_frames: list[pd.DataFrame] = []
    for policy in policies:
        log(f"Running baseline policy: {policy}")
        policy_frames.append(
            apply_policy(
                workload=workload,
                policy=policy,
                capacity_per_instance=args.capacity,
                cold_start_penalty_ms=args.cold_start_penalty,
                execution_ms=args.execution_ms,
                static_warm_instances=args.static_warm,
                prediction_window=args.prediction_window,
            )
        )

    baseline_df = pd.concat(policy_frames, ignore_index=True)
    summary_df = summarize_baselines(baseline_df)

    save_csv(baseline_df, output_dir / "baseline_policy_timeseries.csv")
    save_csv(summary_df, output_dir / "baseline_policy_summary.csv")
    plot_baseline_bars(summary_df, output_dir)

    log("Running static warm instance sensitivity analysis")
    static_frames: list[pd.DataFrame] = []
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
        static_frames.append(policy_df)

    static_sweep_df = pd.concat(static_frames, ignore_index=True)
    static_summary_df = summarize_baselines(static_sweep_df)
    static_values = static_sweep_df[["policy", "static_warm_instances"]].drop_duplicates()
    static_summary_df = static_summary_df.merge(static_values, on="policy", how="left")
    ordered_columns = ["policy", "static_warm_instances"] + [
        col for col in static_summary_df.columns if col not in {"policy", "static_warm_instances"}
    ]
    static_summary_df = static_summary_df[ordered_columns]
    save_csv(static_summary_df, output_dir / "static_warm_sensitivity.csv")


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
        ],
    )
    parser.add_argument("--top-k", type=int, default=20)
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


def main() -> None:
    args = parse_args()
    ensure_base_dirs()

    if args.capacity <= 0:
        raise ValueError("--capacity must be positive")
    if args.top_k <= 0:
        raise ValueError("--top-k must be positive")

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
        )
        _, app_summary = run_burst_pipeline(
            "app",
            files,
            analysis_dir / "app",
            args.top_k,
            args.window,
            args.z,
            args.max_plots,
        )
        _, function_summary = run_burst_pipeline(
            "function",
            files,
            analysis_dir / "function",
            args.top_k,
            args.window,
            args.z,
            args.max_plots,
        )
        combined_summary = pd.concat([global_summary, app_summary, function_summary], ignore_index=True)
        save_csv(combined_summary, analysis_dir / "combined_summary_metrics.csv")
    elif args.mode == "baseline_2019":
        run_baseline_2019(args, files, OUTPUT_ROOT / "baseline_2019")
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
#   python 01_jay/azure_trace_week1.py --mode inspect
#   python 01_jay/azure_trace_week1.py --mode analysis_2019 --top-k 20 --window 60 --z 3
#   python 01_jay/azure_trace_week1.py --mode baseline_2019 --top-k 20 --capacity 20 --cold-start-penalty 800 --execution-ms 100 --static-warm 1 --prediction-window 60
