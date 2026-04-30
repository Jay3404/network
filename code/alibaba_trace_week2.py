"""
Week 2 Alibaba microservices trace preparation.

Scope:
- Inspect Alibaba Cluster Trace microservices v2021 local files.
- Summarize call graph and resource tables.
- Build a first graph-prior table for downstream prewarming experiments.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import math
from collections import defaultdict
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_ROOT = ROOT_DIR / "data" / "alibaba_clusterdata" / "raw" / "microservices_v2021"
OUTPUT_ROOT = ROOT_DIR / "outputs" / "week2" / "alibaba_microservices_2021"

TABLE_DIRS = {
    "node": DATA_ROOT / "Node",
    "resource": DATA_ROOT / "MSResource",
    "rtqps": DATA_ROOT / "MSRTQps",
    "callgraph": DATA_ROOT / "MSCallGraph",
}

SCHEMAS = {
    "node": ["timestamp", "nodeid", "cpu_utilization", "memory_utilization"],
    "resource": [
        "timestamp",
        "msname",
        "msinstanceid",
        "nodeid",
        "cpu_utilization",
        "memory_utilization",
    ],
    "rtqps": ["timestamp", "msname", "msinstanceid", "metrics", "value"],
    "callgraph": ["timestamp", "traceid", "rpcid", "um", "rpctype", "interface", "dm", "rt"],
}

MISSING_SERVICE_VALUES = {"", "nan", "none", "null", "(?)", "?"}

pd = None
np = None


def require_dependencies() -> None:
    global pd, np
    if pd is not None and np is not None:
        return
    try:
        import numpy as _np
        import pandas as _pd
    except ImportError as exc:
        raise RuntimeError("Install required packages first: pip install pandas numpy") from exc
    pd = _pd
    np = _np


def log(message: str) -> None:
    print(f"[week2] {message}", flush=True)


def ensure_output_dir() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


def save_csv(rows: Iterable[dict[str, object]], output_path: Path) -> None:
    rows = list(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        log(f"Saved empty CSV: {output_path}")
        return

    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    log(f"Saved CSV: {output_path}")


def data_files(table: str, include_archives: bool = False) -> list[Path]:
    directory = TABLE_DIRS[table]
    if not directory.exists():
        return []

    suffixes = {".csv", ".gz"}
    files = [
        path
        for path in directory.iterdir()
        if path.is_file()
        and (
            path.suffix in suffixes
            or path.name.endswith(".csv.gz")
            or (include_archives and path.name.endswith(".tar.gz"))
        )
    ]
    return sorted(files)


def csv_files(table: str, max_files: int | None = None) -> list[Path]:
    files = [
        path
        for path in data_files(table, include_archives=False)
        if path.name.endswith(".csv") or path.name.endswith(".csv.gz")
    ]
    if max_files is not None and max_files > 0:
        return files[:max_files]
    return files


def open_text(path: Path):
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def has_header(path: Path, schema: list[str]) -> bool:
    with open_text(path) as handle:
        first_line = handle.readline().strip()
    if not first_line:
        return False
    first_token = first_line.split(",", 1)[0].strip().lower()
    return first_token == schema[0].lower()


def read_chunks(table: str, files: list[Path], chunksize: int):
    require_dependencies()
    schema = SCHEMAS[table]
    for path in files:
        header = 0 if has_header(path, schema) else None
        names = None if header == 0 else schema
        log(f"Reading {table}: {path.name}")
        for chunk in pd.read_csv(path, header=header, names=names, chunksize=chunksize):
            chunk.columns = [str(col).strip().lower() for col in chunk.columns]
            yield path, chunk


def clean_service(series):
    return series.astype(str).str.strip().str.lower()


def valid_service_mask(series):
    cleaned = clean_service(series)
    return ~cleaned.isin(MISSING_SERVICE_VALUES)


def build_manifest() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for table, directory in TABLE_DIRS.items():
        files = data_files(table, include_archives=True)
        csv_count = sum(1 for path in files if path.name.endswith(".csv") or path.name.endswith(".csv.gz"))
        archive_count = sum(1 for path in files if path.name.endswith(".tar.gz"))
        rows.append(
            {
                "table": table,
                "directory": str(directory.relative_to(ROOT_DIR)),
                "exists": directory.exists(),
                "file_count": len(files),
                "csv_count": csv_count,
                "archive_count": archive_count,
                "total_bytes": sum(path.stat().st_size for path in files),
            }
        )
    return rows


def summarize_callgraph(files: list[Path], chunksize: int) -> None:
    edge_stats: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
        lambda: {"call_count": 0.0, "rt_abs_sum": 0.0, "rt_abs_count": 0.0}
    )
    service_stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"outgoing_calls": 0.0, "incoming_calls": 0.0}
    )

    if not files:
        log("No callgraph CSV files found; skipping callgraph summary")
        return

    for _, chunk in read_chunks("callgraph", files, chunksize):
        required = {"um", "dm", "rpctype", "rt"}
        if not required.issubset(chunk.columns):
            raise ValueError(f"Callgraph CSV missing columns: {sorted(required - set(chunk.columns))}")

        mask = valid_service_mask(chunk["um"]) & valid_service_mask(chunk["dm"])
        chunk = chunk.loc[mask, ["um", "dm", "rpctype", "rt"]].copy()
        if chunk.empty:
            continue

        chunk["um"] = chunk["um"].astype(str)
        chunk["dm"] = chunk["dm"].astype(str)
        chunk["rpctype"] = chunk["rpctype"].astype(str).str.lower().str.strip()
        chunk["rt_abs"] = pd.to_numeric(chunk["rt"], errors="coerce").abs()

        grouped = chunk.groupby(["um", "dm", "rpctype"], dropna=False).agg(
            call_count=("rt", "size"),
            rt_abs_sum=("rt_abs", "sum"),
            rt_abs_count=("rt_abs", "count"),
        )

        for (um, dm, rpctype), row in grouped.iterrows():
            key = (str(um), str(dm), str(rpctype))
            edge_stats[key]["call_count"] += float(row["call_count"])
            edge_stats[key]["rt_abs_sum"] += float(row["rt_abs_sum"])
            edge_stats[key]["rt_abs_count"] += float(row["rt_abs_count"])
            service_stats[str(um)]["outgoing_calls"] += float(row["call_count"])
            service_stats[str(dm)]["incoming_calls"] += float(row["call_count"])

    edge_rows: list[dict[str, object]] = []
    outgoing_totals: dict[str, float] = defaultdict(float)
    for (um, _, _), stats in edge_stats.items():
        outgoing_totals[um] += stats["call_count"]

    for (um, dm, rpctype), stats in edge_stats.items():
        rt_count = stats["rt_abs_count"]
        call_count = stats["call_count"]
        edge_rows.append(
            {
                "um": um,
                "dm": dm,
                "rpctype": rpctype,
                "call_count": int(call_count),
                "avg_abs_rt_ms": stats["rt_abs_sum"] / rt_count if rt_count else math.nan,
                "p_downstream_given_upstream": (
                    call_count / outgoing_totals[um] if outgoing_totals[um] else 0.0
                ),
            }
        )

    edge_rows.sort(key=lambda row: row["call_count"], reverse=True)
    save_csv(edge_rows, OUTPUT_ROOT / "callgraph_edge_summary.csv")

    service_rows: list[dict[str, object]] = []
    all_services = set(service_stats)
    for _, dm, _ in edge_stats:
        all_services.add(dm)
    for service in sorted(all_services):
        stats = service_stats[service]
        service_rows.append(
            {
                "msname": service,
                "incoming_calls": int(stats["incoming_calls"]),
                "outgoing_calls": int(stats["outgoing_calls"]),
                "total_graph_calls": int(stats["incoming_calls"] + stats["outgoing_calls"]),
            }
        )
    service_rows.sort(key=lambda row: row["total_graph_calls"], reverse=True)
    save_csv(service_rows, OUTPUT_ROOT / "callgraph_service_summary.csv")


def summarize_resource(files: list[Path], chunksize: int) -> None:
    stats: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "sample_count": 0,
            "cpu_sum": 0.0,
            "memory_sum": 0.0,
            "instances": set(),
            "nodes": set(),
        }
    )

    if not files:
        log("No resource CSV files found; skipping resource summary")
        return

    for _, chunk in read_chunks("resource", files, chunksize):
        required = {"msname", "msinstanceid", "nodeid", "cpu_utilization", "memory_utilization"}
        if not required.issubset(chunk.columns):
            raise ValueError(f"Resource CSV missing columns: {sorted(required - set(chunk.columns))}")

        mask = valid_service_mask(chunk["msname"])
        chunk = chunk.loc[mask, list(required)].copy()
        if chunk.empty:
            continue

        chunk["cpu_utilization"] = pd.to_numeric(chunk["cpu_utilization"], errors="coerce")
        chunk["memory_utilization"] = pd.to_numeric(chunk["memory_utilization"], errors="coerce")

        for msname, group in chunk.groupby("msname"):
            service = str(msname)
            service_stats = stats[service]
            service_stats["sample_count"] += int(len(group))
            service_stats["cpu_sum"] += float(group["cpu_utilization"].sum(skipna=True))
            service_stats["memory_sum"] += float(group["memory_utilization"].sum(skipna=True))
            service_stats["instances"].update(group["msinstanceid"].dropna().astype(str).unique())
            service_stats["nodes"].update(group["nodeid"].dropna().astype(str).unique())

    rows: list[dict[str, object]] = []
    for msname, item in stats.items():
        sample_count = int(item["sample_count"])
        rows.append(
            {
                "msname": msname,
                "sample_count": sample_count,
                "instance_count": len(item["instances"]),
                "node_count": len(item["nodes"]),
                "avg_cpu_utilization": item["cpu_sum"] / sample_count if sample_count else math.nan,
                "avg_memory_utilization": item["memory_sum"] / sample_count if sample_count else math.nan,
            }
        )

    rows.sort(key=lambda row: row["sample_count"], reverse=True)
    save_csv(rows, OUTPUT_ROOT / "service_resource_summary.csv")


def summarize_rtqps(files: list[Path], chunksize: int) -> None:
    metric_stats: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"sample_count": 0.0, "value_sum": 0.0}
    )

    if not files:
        log("No MSRTQps CSV files found; skipping RT/QPS summary")
        return

    for _, chunk in read_chunks("rtqps", files, chunksize):
        required = {"msname", "metrics", "value"}
        if not required.issubset(chunk.columns):
            raise ValueError(f"MSRTQps CSV missing columns: {sorted(required - set(chunk.columns))}")

        mask = valid_service_mask(chunk["msname"])
        chunk = chunk.loc[mask, ["msname", "metrics", "value"]].copy()
        if chunk.empty:
            continue

        chunk["metrics"] = chunk["metrics"].astype(str).str.strip()
        chunk["value"] = pd.to_numeric(chunk["value"], errors="coerce")
        grouped = chunk.groupby(["msname", "metrics"]).agg(
            sample_count=("value", "count"),
            value_sum=("value", "sum"),
        )
        for (msname, metric), row in grouped.iterrows():
            key = (str(msname), str(metric))
            metric_stats[key]["sample_count"] += float(row["sample_count"])
            metric_stats[key]["value_sum"] += float(row["value_sum"])

    rows: list[dict[str, object]] = []
    for (msname, metric), item in metric_stats.items():
        count = item["sample_count"]
        rows.append(
            {
                "msname": msname,
                "metric": metric,
                "sample_count": int(count),
                "avg_value": item["value_sum"] / count if count else math.nan,
            }
        )

    rows.sort(key=lambda row: (row["msname"], row["metric"]))
    save_csv(rows, OUTPUT_ROOT / "service_rtqps_summary.csv")


def load_optional_csv(path: Path):
    require_dependencies()
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def build_graph_prior(rt_scale_ms: float) -> None:
    require_dependencies()

    edge_path = OUTPUT_ROOT / "callgraph_edge_summary.csv"
    resource_path = OUTPUT_ROOT / "service_resource_summary.csv"
    service_path = OUTPUT_ROOT / "callgraph_service_summary.csv"

    edge_df = load_optional_csv(edge_path)
    if edge_df.empty:
        log("Callgraph edge summary is missing; graph prior not generated")
        return

    resource_df = load_optional_csv(resource_path)
    if not resource_df.empty:
        resource_df = resource_df.rename(
            columns={
                "msname": "dm",
                "avg_cpu_utilization": "dm_avg_cpu_utilization",
                "avg_memory_utilization": "dm_avg_memory_utilization",
                "instance_count": "dm_instance_count",
                "node_count": "dm_node_count",
            }
        )
        keep = [
            "dm",
            "dm_avg_cpu_utilization",
            "dm_avg_memory_utilization",
            "dm_instance_count",
            "dm_node_count",
        ]
        edge_df = edge_df.merge(resource_df[keep], on="dm", how="left")

    avg_rt = pd.to_numeric(edge_df["avg_abs_rt_ms"], errors="coerce").fillna(rt_scale_ms)
    transition = pd.to_numeric(edge_df["p_downstream_given_upstream"], errors="coerce").fillna(0.0)
    edge_df["latency_decay"] = 1.0 / (1.0 + avg_rt / rt_scale_ms)
    edge_df["graph_prior_weight"] = transition * edge_df["latency_decay"]

    edge_df = edge_df.sort_values("graph_prior_weight", ascending=False)
    edge_df.to_csv(OUTPUT_ROOT / "graph_prior_edges.csv", index=False)
    log(f"Saved CSV: {OUTPUT_ROOT / 'graph_prior_edges.csv'}")

    service_df = load_optional_csv(service_path)
    if service_df.empty:
        return

    incoming = pd.to_numeric(service_df["incoming_calls"], errors="coerce").fillna(0.0)
    outgoing = pd.to_numeric(service_df["outgoing_calls"], errors="coerce").fillna(0.0)
    total = incoming + outgoing
    service_df["incoming_share"] = incoming / incoming.sum() if incoming.sum() else 0.0
    service_df["outgoing_share"] = outgoing / outgoing.sum() if outgoing.sum() else 0.0
    service_df["graph_centrality_proxy"] = total / total.sum() if total.sum() else 0.0

    if not resource_df.empty:
        resource_service_df = resource_df.rename(columns={"dm": "msname"})
        keep = [
            "msname",
            "dm_avg_cpu_utilization",
            "dm_avg_memory_utilization",
            "dm_instance_count",
            "dm_node_count",
        ]
        service_df = service_df.merge(resource_service_df[keep], on="msname", how="left")

    service_df = service_df.sort_values("graph_centrality_proxy", ascending=False)
    service_df.to_csv(OUTPUT_ROOT / "graph_prior_services.csv", index=False)
    log(f"Saved CSV: {OUTPUT_ROOT / 'graph_prior_services.csv'}")


def run(args: argparse.Namespace) -> None:
    ensure_output_dir()
    manifest = build_manifest()
    save_csv(manifest, OUTPUT_ROOT / "dataset_manifest.csv")

    if args.mode == "inspect":
        for row in manifest:
            log(
                f"{row['table']}: csv={row['csv_count']}, archives={row['archive_count']}, "
                f"bytes={row['total_bytes']}"
            )
        return

    max_files = args.max_files if args.max_files > 0 else None

    if args.mode in {"summarize", "all"}:
        summarize_callgraph(csv_files("callgraph", max_files), args.chunksize)
        summarize_resource(csv_files("resource", max_files), args.chunksize)
        summarize_rtqps(csv_files("rtqps", max_files), args.chunksize)

    if args.mode in {"graph_prior", "all"}:
        build_graph_prior(args.rt_scale_ms)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Alibaba microservices v2021 Week 2 preprocessing and graph-prior builder."
    )
    parser.add_argument(
        "--mode",
        choices=["inspect", "summarize", "graph_prior", "all"],
        default="inspect",
    )
    parser.add_argument("--chunksize", type=int, default=500000)
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="Limit CSV files per table for smoke tests. 0 means process all available CSV files.",
    )
    parser.add_argument(
        "--rt-scale-ms",
        type=float,
        default=100.0,
        help="Latency scale used in graph_prior_weight = P(dm|um) / (1 + avg_rt / rt_scale_ms).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.chunksize <= 0:
        raise ValueError("--chunksize must be positive")
    if args.max_files < 0:
        raise ValueError("--max-files must be non-negative")
    if args.rt_scale_ms <= 0:
        raise ValueError("--rt-scale-ms must be positive")
    run(args)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as exc:
        log(f"ERROR: {exc}")
        raise SystemExit(1) from exc
