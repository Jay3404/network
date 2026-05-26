#!/usr/bin/env python3
"""
Week 3-4 Alibaba microservices trace preprocessing.

Scope:
- Inspect extracted Alibaba microservices v2021 files
- Build call-graph edge priors from MS_CallGraph_Table
- Build node/resource/placement priors from node and MS_Resource_Table
- Generate a deterministic synthetic edge topology from Alibaba node load
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Iterable


ROOT_DIR = Path(__file__).resolve().parents[1]
ALIBABA_ROOT = ROOT_DIR / "data" / "alibaba_microservices_2021"
DEFAULT_DATA_ROOT = ALIBABA_ROOT / "extracted"
OUTPUT_ROOT = ROOT_DIR / "outputs" / "week3_4" / "alibaba"

NODE_COLUMNS = ["timestamp", "nodeid", "cpu_utilization", "memory_utilization"]
MS_RESOURCE_COLUMNS = [
    "timestamp",
    "msname",
    "msinstanceid",
    "nodeid",
    "cpu_utilization",
    "memory_utilization",
]
MS_CALLGRAPH_COLUMNS = ["timestamp", "traceid", "rpcid", "um", "rpctype", "interface", "dm", "rt"]
MS_RTQPS_COLUMNS = ["timestamp", "msname", "msinstanceid", "metrics", "value"]

COLUMN_ALIASES = {
    "node_cpu_usage": "cpu_utilization",
    "node_memory_usage": "memory_utilization",
    "instance_cpu_usage": "cpu_utilization",
    "instance_memory_usage": "memory_utilization",
}

COMPONENTS = {
    "node": {
        "aliases": ["node"],
        "columns": NODE_COLUMNS,
    },
    "resource": {
        "aliases": ["msresource", "ms_resource", "resource"],
        "columns": MS_RESOURCE_COLUMNS,
    },
    "rtqps": {
        "aliases": ["msrtqps", "ms_rtqps", "rtqps"],
        "columns": MS_RTQPS_COLUMNS,
    },
    "callgraph": {
        "aliases": ["mscallgraph", "ms_callgraph", "callgraph"],
        "columns": MS_CALLGRAPH_COLUMNS,
    },
}

ARCHIVE_SUFFIXES = (".tar", ".tar.gz", ".tgz", ".gz", ".zip", ".rar", ".xz", ".7z")
SKIP_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".md", ".sh", ".pdf")
INVALID_SERVICE_IDS = {"", "nan", "none", "null", "(?)", "?", "na"}

pd = None
np = None


def log(message: str) -> None:
    print(f"[week3_4] {message}", flush=True)


def require_dependencies() -> None:
    global pd, np

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

    if missing:
        package_list = " ".join(sorted(set(missing)))
        raise RuntimeError(
            "Missing required Python package(s): "
            f"{package_list}. Install them with: pip install -r requirements.txt"
        )


def ensure_output_dir() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


def resolve_data_root(raw_root: str | None) -> Path:
    if raw_root:
        return Path(raw_root).expanduser().resolve()
    return DEFAULT_DATA_ROOT


def is_archive(path: Path) -> bool:
    lower_name = path.name.lower()
    return any(lower_name.endswith(suffix) for suffix in ARCHIVE_SUFFIXES)


def is_candidate_data_file(path: Path) -> bool:
    if not path.is_file() or path.name.startswith("."):
        return False
    lower_name = path.name.lower()
    if is_archive(path):
        return False
    if any(lower_name.endswith(suffix) for suffix in SKIP_SUFFIXES):
        return False
    return True


def path_matches_component(path: Path, component: str) -> bool:
    aliases = COMPONENTS[component]["aliases"]
    tokens = [path.name.lower(), *[part.lower() for part in path.parts]]
    return any(alias in token for alias in aliases for token in tokens)


def find_component_files(data_root: Path, component: str, max_files: int = 0) -> list[Path]:
    if not data_root.exists():
        return []

    files = [
        path
        for path in data_root.rglob("*")
        if is_candidate_data_file(path) and path_matches_component(path, component)
    ]
    files = sorted(files)
    if max_files > 0:
        files = files[:max_files]
    return files


def read_first_line(path: Path) -> str:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as file:
        return file.readline().strip()


def detect_separator(first_line: str) -> str:
    tab_count = first_line.count("\t")
    comma_count = first_line.count(",")
    return "\t" if tab_count > comma_count else ","


def split_line(first_line: str, sep: str) -> list[str]:
    return [item.strip() for item in next(csv.reader([first_line], delimiter=sep))]


def has_header(first_line: str, sep: str, expected_columns: list[str]) -> bool:
    tokens = {token.strip().lower() for token in split_line(first_line, sep)}
    expected = {column.lower() for column in expected_columns}
    return bool(tokens & expected)


def iter_csv_chunks(
    files: list[Path],
    columns: list[str],
    chunksize: int,
) -> Iterable[tuple[Path, pd.DataFrame]]:
    for path in files:
        first_line = read_first_line(path)
        if not first_line:
            continue
        sep = detect_separator(first_line)
        header = 0 if has_header(first_line, sep, columns) else None
        read_kwargs = {
            "sep": sep,
            "chunksize": chunksize,
            "low_memory": False,
        }
        if header is None:
            read_kwargs["header"] = None
            read_kwargs["names"] = columns
        else:
            read_kwargs["header"] = 0

        for chunk in pd.read_csv(path, **read_kwargs):
            chunk.columns = [str(column).strip().lower() for column in chunk.columns]
            drop_columns = [
                column
                for column in chunk.columns
                if column == "" or column.startswith("unnamed:")
            ]
            if drop_columns:
                chunk = chunk.drop(columns=drop_columns)
            chunk = chunk.rename(columns=COLUMN_ALIASES)
            missing = [column for column in columns if column not in chunk.columns]
            if missing:
                log(f"Skipping chunk from {path.name}; missing columns: {missing}")
                continue
            yield path, chunk[columns].copy()


def clean_service_series(series: pd.Series) -> pd.Series:
    cleaned = series.astype(str).str.strip()
    return cleaned.mask(cleaned.str.lower().isin(INVALID_SERVICE_IDS), "")


def save_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log(f"Saved CSV: {path}")


def save_json(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    log(f"Saved JSON: {path}")


def inspect_component_files(args: argparse.Namespace) -> None:
    ensure_output_dir()
    data_root = resolve_data_root(args.data_root)
    summary: dict[str, object] = {"data_root": str(data_root), "components": {}}

    for component, spec in COMPONENTS.items():
        files = find_component_files(data_root, component, args.max_files)
        samples: list[dict[str, object]] = []
        for path in files[: args.inspect_samples]:
            first_line = read_first_line(path)
            sep = detect_separator(first_line) if first_line else ","
            tokens = split_line(first_line, sep) if first_line else []
            samples.append(
                {
                    "path": str(path),
                    "separator": "\\t" if sep == "\t" else sep,
                    "first_line_columns_or_values": tokens[:12],
                    "has_header": has_header(first_line, sep, spec["columns"]) if first_line else False,
                }
            )
        summary["components"][component] = {
            "file_count": len(files),
            "sample_files": samples,
        }
        log(f"{component}: {len(files)} candidate files")

    save_json(summary, OUTPUT_ROOT / "inspect_summary.json")


def update_edge_accumulator(
    accumulator: dict[tuple[str, str, str], list[float]],
    grouped: pd.DataFrame,
) -> None:
    for row in grouped.itertuples(index=False):
        key = (row.um, row.dm, row.rpctype)
        values = accumulator[key]
        values[0] += float(row.call_count)
        values[1] += float(row.rt_abs_sum)
        values[2] = max(values[2], float(row.rt_abs_max))
        values[3] += float(row.positive_rt_count)
        values[4] += float(row.zero_rt_count)


def run_graph_prior(args: argparse.Namespace) -> pd.DataFrame:
    ensure_output_dir()
    data_root = resolve_data_root(args.data_root)
    files = find_component_files(data_root, "callgraph", args.max_files)
    if not files:
        raise FileNotFoundError(f"No Alibaba call graph files found under {data_root}")

    edge_accumulator: dict[tuple[str, str, str], list[float]] = defaultdict(
        lambda: [0.0, 0.0, 0.0, 0.0, 0.0]
    )
    total_rows = 0
    valid_rows = 0

    for path, chunk in iter_csv_chunks(files, MS_CALLGRAPH_COLUMNS, args.chunk_size):
        total_rows += len(chunk)
        chunk["um"] = clean_service_series(chunk["um"])
        chunk["dm"] = clean_service_series(chunk["dm"])
        chunk["rpctype"] = chunk["rpctype"].astype(str).str.strip().str.lower().replace("", "unknown")
        chunk["rt"] = pd.to_numeric(chunk["rt"], errors="coerce")
        valid = chunk[(chunk["um"] != "") & (chunk["dm"] != "") & chunk["rt"].notna()].copy()
        valid_rows += len(valid)
        if valid.empty:
            continue

        valid["rt_abs"] = valid["rt"].abs()
        valid["positive_rt"] = valid["rt"] > 0
        valid["zero_rt"] = valid["rt_abs"] == 0
        grouped = (
            valid.groupby(["um", "dm", "rpctype"], sort=False)
            .agg(
                call_count=("rt_abs", "size"),
                rt_abs_sum=("rt_abs", "sum"),
                rt_abs_max=("rt_abs", "max"),
                positive_rt_count=("positive_rt", "sum"),
                zero_rt_count=("zero_rt", "sum"),
            )
            .reset_index()
        )
        update_edge_accumulator(edge_accumulator, grouped)
        log(f"Processed call graph chunk from {path.name}; rows={len(chunk)}")

    edge_rows = [
        {
            "um": um,
            "dm": dm,
            "rpctype": rpctype,
            "call_count": values[0],
            "rt_abs_sum_ms": values[1],
            "rt_abs_max_ms": values[2],
            "positive_rt_count": values[3],
            "zero_rt_count": values[4],
        }
        for (um, dm, rpctype), values in edge_accumulator.items()
    ]
    edge_df = pd.DataFrame(edge_rows)
    if edge_df.empty:
        raise RuntimeError("No valid call graph edges were produced")

    edge_df = edge_df[edge_df["call_count"] >= args.min_call_count].copy()
    edge_df["rt_abs_mean_ms"] = edge_df["rt_abs_sum_ms"] / edge_df["call_count"].replace(0, np.nan)
    edge_df["call_weight"] = edge_df["call_count"] / edge_df["call_count"].max()
    max_rt_mean = float(edge_df["rt_abs_mean_ms"].max())
    edge_df["rt_weight"] = edge_df["rt_abs_mean_ms"] / max_rt_mean if max_rt_mean > 0 else 0.0
    edge_df["graph_score"] = edge_df["call_weight"] * (1.0 + edge_df["rt_weight"])
    edge_df = edge_df.sort_values(["graph_score", "call_count"], ascending=False).reset_index(drop=True)
    edge_df["rank"] = np.arange(1, len(edge_df) + 1)

    output_edges = edge_df.head(args.top_edges)
    save_csv(output_edges, OUTPUT_ROOT / "graph_edges.csv")

    service_scores = build_service_graph_scores(edge_df)
    save_csv(service_scores.head(args.top_services), OUTPUT_ROOT / "service_graph_scores.csv")
    save_json(
        {
            "input_files": [str(path) for path in files],
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "edge_count_after_filter": int(len(edge_df)),
            "top_edges_saved": int(len(output_edges)),
            "min_call_count": args.min_call_count,
            "graph_score": "call_weight * (1 + rt_abs_mean_weight)",
        },
        OUTPUT_ROOT / "graph_prior_metadata.json",
    )
    return edge_df


def build_service_graph_scores(edge_df: pd.DataFrame) -> pd.DataFrame:
    out_df = (
        edge_df.groupby("um")
        .agg(
            out_degree=("dm", "nunique"),
            out_call_count=("call_count", "sum"),
            out_graph_score=("graph_score", "sum"),
            out_rt_abs_mean_ms=("rt_abs_mean_ms", "mean"),
        )
        .reset_index()
        .rename(columns={"um": "msname"})
    )
    in_df = (
        edge_df.groupby("dm")
        .agg(
            in_degree=("um", "nunique"),
            in_call_count=("call_count", "sum"),
            in_graph_score=("graph_score", "sum"),
            in_rt_abs_mean_ms=("rt_abs_mean_ms", "mean"),
        )
        .reset_index()
        .rename(columns={"dm": "msname"})
    )
    services = pd.DataFrame({"msname": sorted(set(out_df["msname"]) | set(in_df["msname"]))})
    services = services.merge(out_df, on="msname", how="left").merge(in_df, on="msname", how="left")
    numeric_columns = [column for column in services.columns if column != "msname"]
    services[numeric_columns] = services[numeric_columns].fillna(0)
    services["total_degree"] = services["in_degree"] + services["out_degree"]
    services["total_call_count"] = services["in_call_count"] + services["out_call_count"]
    services["total_graph_score"] = services["in_graph_score"] + services["out_graph_score"]
    return services.sort_values(["total_graph_score", "total_call_count"], ascending=False).reset_index(drop=True)


def update_entity_stats(
    accumulator: dict[str, list[float]],
    key: str,
    cpu: float,
    memory: float,
    timestamp: float,
) -> None:
    values = accumulator[key]
    values[0] += 1.0
    values[1] += cpu
    values[2] += memory
    values[3] = max(values[3], cpu)
    values[4] = max(values[4], memory)
    values[5] = min(values[5], timestamp)
    values[6] = max(values[6], timestamp)


def stats_dict_to_frame(accumulator: dict[str, list[float]], key_name: str) -> pd.DataFrame:
    rows = []
    for key, values in accumulator.items():
        observations = values[0]
        rows.append(
            {
                key_name: key,
                "observations": observations,
                "avg_cpu_utilization": values[1] / observations if observations else 0.0,
                "avg_memory_utilization": values[2] / observations if observations else 0.0,
                "max_cpu_utilization": values[3],
                "max_memory_utilization": values[4],
                "timestamp_min": values[5],
                "timestamp_max": values[6],
            }
        )
    return pd.DataFrame(rows)


def summarize_node_resources(args: argparse.Namespace) -> pd.DataFrame:
    data_root = resolve_data_root(args.data_root)
    files = find_component_files(data_root, "node", args.max_files)
    if not files:
        log(f"No Alibaba node files found under {data_root}")
        return pd.DataFrame()

    node_stats: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0, 0.0, float("inf"), 0.0])
    total_rows = 0
    for path, chunk in iter_csv_chunks(files, NODE_COLUMNS, args.chunk_size):
        total_rows += len(chunk)
        chunk["nodeid"] = chunk["nodeid"].astype(str).str.strip()
        chunk["timestamp"] = pd.to_numeric(chunk["timestamp"], errors="coerce")
        chunk["cpu_utilization"] = pd.to_numeric(chunk["cpu_utilization"], errors="coerce")
        chunk["memory_utilization"] = pd.to_numeric(chunk["memory_utilization"], errors="coerce")
        valid = chunk.dropna(subset=["nodeid", "timestamp", "cpu_utilization", "memory_utilization"])
        for row in valid.itertuples(index=False):
            update_entity_stats(
                node_stats,
                row.nodeid,
                float(row.cpu_utilization),
                float(row.memory_utilization),
                float(row.timestamp),
            )
        log(f"Processed node chunk from {path.name}; rows={len(chunk)}")

    node_df = stats_dict_to_frame(node_stats, "nodeid")
    if node_df.empty:
        return node_df
    node_df["available_cpu_score"] = (1.0 - node_df["avg_cpu_utilization"]).clip(lower=0.0, upper=1.0)
    node_df["available_memory_score"] = (1.0 - node_df["avg_memory_utilization"]).clip(lower=0.0, upper=1.0)
    node_df["available_score"] = 0.5 * node_df["available_cpu_score"] + 0.5 * node_df["available_memory_score"]
    node_df = node_df.sort_values(["available_score", "observations"], ascending=False).reset_index(drop=True)
    save_csv(node_df, OUTPUT_ROOT / "node_resource_summary.csv")
    save_json(
        {
            "input_files": [str(path) for path in files],
            "total_rows": total_rows,
            "node_count": int(len(node_df)),
        },
        OUTPUT_ROOT / "node_resource_metadata.json",
    )
    return node_df


def summarize_ms_resources(args: argparse.Namespace, node_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    data_root = resolve_data_root(args.data_root)
    files = find_component_files(data_root, "resource", args.max_files)
    if not files:
        log(f"No Alibaba MS resource files found under {data_root}")
        return pd.DataFrame(), pd.DataFrame()

    ms_node_stats: dict[tuple[str, str], list[float]] = defaultdict(
        lambda: [0.0, 0.0, 0.0, 0.0, 0.0, float("inf"), 0.0]
    )
    total_rows = 0
    for path, chunk in iter_csv_chunks(files, MS_RESOURCE_COLUMNS, args.chunk_size):
        total_rows += len(chunk)
        chunk["msname"] = clean_service_series(chunk["msname"])
        chunk["nodeid"] = chunk["nodeid"].astype(str).str.strip()
        chunk["timestamp"] = pd.to_numeric(chunk["timestamp"], errors="coerce")
        chunk["cpu_utilization"] = pd.to_numeric(chunk["cpu_utilization"], errors="coerce")
        chunk["memory_utilization"] = pd.to_numeric(chunk["memory_utilization"], errors="coerce")
        valid = chunk[
            (chunk["msname"] != "")
            & (chunk["nodeid"] != "")
            & chunk["timestamp"].notna()
            & chunk["cpu_utilization"].notna()
            & chunk["memory_utilization"].notna()
        ]
        for row in valid.itertuples(index=False):
            update_entity_stats(
                ms_node_stats,
                (row.msname, row.nodeid),
                float(row.cpu_utilization),
                float(row.memory_utilization),
                float(row.timestamp),
            )
        log(f"Processed MS resource chunk from {path.name}; rows={len(chunk)}")

    ms_node_df = stats_dict_to_frame(
        {f"{msname}::{nodeid}": values for (msname, nodeid), values in ms_node_stats.items()},
        "ms_node_key",
    )
    if ms_node_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    split_keys = ms_node_df["ms_node_key"].str.split("::", n=1, expand=True)
    ms_node_df["msname"] = split_keys[0]
    ms_node_df["nodeid"] = split_keys[1]
    ms_node_df = ms_node_df.drop(columns=["ms_node_key"])

    ms_totals = ms_node_df.groupby("msname")["observations"].sum().rename("ms_observations")
    ms_node_df = ms_node_df.merge(ms_totals, on="msname", how="left")
    ms_node_df["observation_share"] = ms_node_df["observations"] / ms_node_df["ms_observations"]
    if not node_df.empty:
        ms_node_df = ms_node_df.merge(
            node_df[["nodeid", "available_score", "avg_cpu_utilization", "avg_memory_utilization"]].rename(
                columns={
                    "avg_cpu_utilization": "node_avg_cpu_utilization",
                    "avg_memory_utilization": "node_avg_memory_utilization",
                }
            ),
            on="nodeid",
            how="left",
        )
        ms_node_df["available_score"] = ms_node_df["available_score"].fillna(0.0)
    else:
        ms_node_df["available_score"] = 0.0
    ms_node_df["affinity_score"] = ms_node_df["observation_share"] * (1.0 + ms_node_df["available_score"])
    ms_node_df = ms_node_df.sort_values(["affinity_score", "observations"], ascending=False).reset_index(drop=True)

    ms_summary = (
        ms_node_df.groupby("msname")
        .agg(
            observations=("observations", "sum"),
            node_count=("nodeid", "nunique"),
            avg_cpu_utilization=("avg_cpu_utilization", "mean"),
            avg_memory_utilization=("avg_memory_utilization", "mean"),
            max_cpu_utilization=("max_cpu_utilization", "max"),
            max_memory_utilization=("max_memory_utilization", "max"),
            best_affinity_score=("affinity_score", "max"),
        )
        .reset_index()
    )
    ms_summary["resource_demand_score"] = (
        0.5 * ms_summary["avg_cpu_utilization"].clip(lower=0.0, upper=1.0)
        + 0.5 * ms_summary["avg_memory_utilization"].clip(lower=0.0, upper=1.0)
    )
    ms_summary = ms_summary.sort_values(["observations", "resource_demand_score"], ascending=False).reset_index(drop=True)

    save_csv(ms_summary, OUTPUT_ROOT / "ms_resource_summary.csv")
    save_csv(ms_node_df, OUTPUT_ROOT / "ms_node_affinity.csv")
    save_json(
        {
            "input_files": [str(path) for path in files],
            "total_rows": total_rows,
            "ms_count": int(len(ms_summary)),
            "ms_node_pair_count": int(len(ms_node_df)),
        },
        OUTPUT_ROOT / "ms_resource_metadata.json",
    )
    return ms_summary, ms_node_df


def run_resource_prior(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ensure_output_dir()
    node_df = summarize_node_resources(args)
    ms_summary, ms_node_df = summarize_ms_resources(args, node_df)
    if not ms_summary.empty and not ms_node_df.empty:
        placement_candidates = build_placement_candidates(ms_summary, ms_node_df, args)
        save_csv(placement_candidates, OUTPUT_ROOT / "placement_candidates.csv")
    return node_df, ms_summary, ms_node_df


def build_placement_candidates(
    ms_summary: pd.DataFrame,
    ms_node_df: pd.DataFrame,
    args: argparse.Namespace,
) -> pd.DataFrame:
    top_ms = set(ms_summary.head(args.top_services)["msname"])
    candidates = ms_node_df[ms_node_df["msname"].isin(top_ms)].copy()
    if candidates.empty:
        return candidates
    candidates["candidate_rank"] = (
        candidates.groupby("msname")["affinity_score"].rank(method="first", ascending=False).astype(int)
    )
    candidates = candidates[candidates["candidate_rank"] <= args.candidate_nodes_per_service]
    columns = [
        "msname",
        "candidate_rank",
        "nodeid",
        "observations",
        "observation_share",
        "avg_cpu_utilization",
        "avg_memory_utilization",
        "available_score",
        "affinity_score",
    ]
    return candidates[columns].sort_values(["msname", "candidate_rank"]).reset_index(drop=True)


def run_topology(args: argparse.Namespace, node_df: pd.DataFrame | None = None) -> None:
    ensure_output_dir()
    if node_df is None or node_df.empty:
        path = OUTPUT_ROOT / "node_resource_summary.csv"
        if path.exists():
            node_df = pd.read_csv(path)
        else:
            node_df = summarize_node_resources(args)
    if node_df.empty:
        raise RuntimeError("Node resource summary is required to generate topology")

    selected = node_df.sort_values(["available_score", "observations"], ascending=False).head(args.edge_nodes).copy()
    selected = selected.reset_index(drop=True)
    selected["edge_node_id"] = [f"edge_{index:03d}" for index in range(len(selected))]
    selected["tier"] = "edge"
    topology_nodes = selected[
        [
            "edge_node_id",
            "nodeid",
            "tier",
            "avg_cpu_utilization",
            "avg_memory_utilization",
            "available_score",
        ]
    ]

    links = []
    seen_pairs: set[tuple[int, int]] = set()
    node_count = len(topology_nodes)
    for index in range(node_count):
        for hop in range(1, min(args.neighbor_degree, node_count - 1) + 1):
            target = (index + hop) % node_count
            if index == target:
                continue
            pair = tuple(sorted((index, target)))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            source_row = selected.iloc[index]
            target_row = selected.iloc[target]
            utilization_gap = abs(float(source_row["available_score"]) - float(target_row["available_score"]))
            latency_ms = args.base_link_latency_ms + hop * args.hop_latency_step_ms + utilization_gap * 10.0
            links.append(
                {
                    "source": source_row["edge_node_id"],
                    "target": target_row["edge_node_id"],
                    "latency_ms": round(latency_ms, 3),
                    "bandwidth_mbps": args.link_bandwidth_mbps,
                    "link_type": "synthetic_ring_chord",
                    "is_bidirectional": True,
                }
            )

    topology_links = pd.DataFrame(links)
    save_csv(topology_nodes, OUTPUT_ROOT / "edge_topology_nodes.csv")
    save_csv(topology_links, OUTPUT_ROOT / "edge_topology_links.csv")
    save_json(
        {
            "edge_nodes": int(node_count),
            "neighbor_degree": args.neighbor_degree,
            "construction": "nodes selected by highest Alibaba available_score; links are deterministic ring chords",
        },
        OUTPUT_ROOT / "edge_topology_metadata.json",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Alibaba Microservices v2021 graph/resource preprocessing for week 3-4."
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=["inspect", "graph_prior", "resource_prior", "topology", "all"],
    )
    parser.add_argument(
        "--data-root",
        default=None,
        help="Extracted Alibaba trace root. Default: data/alibaba_microservices_2021/extracted",
    )
    parser.add_argument("--chunk-size", type=int, default=200000)
    parser.add_argument("--max-files", type=int, default=0, help="Limit files per component; 0 means all.")
    parser.add_argument("--inspect-samples", type=int, default=3)
    parser.add_argument("--min-call-count", type=int, default=2)
    parser.add_argument("--top-edges", type=int, default=100000)
    parser.add_argument("--top-services", type=int, default=5000)
    parser.add_argument("--candidate-nodes-per-service", type=int, default=3)
    parser.add_argument("--edge-nodes", type=int, default=32)
    parser.add_argument("--neighbor-degree", type=int, default=2)
    parser.add_argument("--base-link-latency-ms", type=float, default=1.0)
    parser.add_argument("--hop-latency-step-ms", type=float, default=1.0)
    parser.add_argument("--link-bandwidth-mbps", type=float, default=1000.0)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")
    if args.max_files < 0:
        raise ValueError("--max-files must be non-negative")
    if args.inspect_samples <= 0:
        raise ValueError("--inspect-samples must be positive")
    if args.min_call_count <= 0:
        raise ValueError("--min-call-count must be positive")
    if args.top_edges <= 0:
        raise ValueError("--top-edges must be positive")
    if args.top_services <= 0:
        raise ValueError("--top-services must be positive")
    if args.candidate_nodes_per_service <= 0:
        raise ValueError("--candidate-nodes-per-service must be positive")
    if args.edge_nodes <= 1:
        raise ValueError("--edge-nodes must be greater than 1")
    if args.neighbor_degree <= 0:
        raise ValueError("--neighbor-degree must be positive")


def main() -> None:
    args = parse_args()
    validate_args(args)

    if args.mode == "inspect":
        inspect_component_files(args)
    elif args.mode == "graph_prior":
        require_dependencies()
        run_graph_prior(args)
    elif args.mode == "resource_prior":
        require_dependencies()
        run_resource_prior(args)
    elif args.mode == "topology":
        require_dependencies()
        run_topology(args)
    elif args.mode == "all":
        require_dependencies()
        run_graph_prior(args)
        node_df, _, _ = run_resource_prior(args)
        run_topology(args, node_df=node_df)
    else:
        raise ValueError(f"Unsupported mode: {args.mode}")

    log("Done")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, FileNotFoundError, ValueError) as exc:
        log(f"ERROR: {exc}")
        raise SystemExit(1) from exc
