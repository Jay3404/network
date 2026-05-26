#!/usr/bin/env python3
"""
Streaming Alibaba microservices preprocessing.

This script supports full-dataset processing when raw/extracted CSV files are
too large to keep on local disk. Each shard can be ingested into a small SQLite
aggregation database and then deleted by the caller.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from alibaba_microservices_week3 import (
    MS_CALLGRAPH_COLUMNS,
    MS_RESOURCE_COLUMNS,
    NODE_COLUMNS,
    build_service_graph_scores,
    clean_service_series,
    iter_csv_chunks,
    require_dependencies,
    save_csv,
    save_json,
)


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT_DIR / "outputs" / "week3_4" / "alibaba_streaming"
DEFAULT_DB_PATH = DEFAULT_OUTPUT_DIR / "alibaba_streaming.sqlite"

pd = None
np = None


def log(message: str) -> None:
    print(f"[streaming] {message}", flush=True)


def load_dependencies() -> None:
    global pd, np
    require_dependencies()
    import pandas as pandas_module
    import numpy as numpy_module

    pd = pandas_module
    np = numpy_module


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ingested_sources (
            source_id TEXT PRIMARY KEY,
            component TEXT NOT NULL,
            file_path TEXT NOT NULL,
            total_rows INTEGER NOT NULL,
            valid_rows INTEGER NOT NULL,
            ingested_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS graph_edges (
            um TEXT NOT NULL,
            dm TEXT NOT NULL,
            rpctype TEXT NOT NULL,
            call_count REAL NOT NULL,
            rt_abs_sum_ms REAL NOT NULL,
            rt_abs_max_ms REAL NOT NULL,
            positive_rt_count REAL NOT NULL,
            zero_rt_count REAL NOT NULL,
            PRIMARY KEY (um, dm, rpctype)
        );

        CREATE TABLE IF NOT EXISTS node_stats (
            nodeid TEXT PRIMARY KEY,
            observations REAL NOT NULL,
            cpu_sum REAL NOT NULL,
            memory_sum REAL NOT NULL,
            max_cpu_utilization REAL NOT NULL,
            max_memory_utilization REAL NOT NULL,
            timestamp_min REAL NOT NULL,
            timestamp_max REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ms_node_stats (
            msname TEXT NOT NULL,
            nodeid TEXT NOT NULL,
            observations REAL NOT NULL,
            cpu_sum REAL NOT NULL,
            memory_sum REAL NOT NULL,
            max_cpu_utilization REAL NOT NULL,
            max_memory_utilization REAL NOT NULL,
            timestamp_min REAL NOT NULL,
            timestamp_max REAL NOT NULL,
            PRIMARY KEY (msname, nodeid)
        );
        """
    )
    conn.commit()


def is_ingested(conn: sqlite3.Connection, source_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM ingested_sources WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    return row is not None


def mark_ingested(
    conn: sqlite3.Connection,
    source_id: str,
    component: str,
    file_path: Path,
    total_rows: int,
    valid_rows: int,
) -> None:
    conn.execute(
        """
        INSERT INTO ingested_sources (
            source_id, component, file_path, total_rows, valid_rows, ingested_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            component,
            str(file_path),
            total_rows,
            valid_rows,
            datetime.now(timezone.utc).isoformat(),
        ),
    )


def ingest_callgraph(conn: sqlite3.Connection, file_path: Path, chunk_size: int) -> tuple[int, int]:
    total_rows = 0
    valid_rows = 0
    sql = """
        INSERT INTO graph_edges (
            um, dm, rpctype, call_count, rt_abs_sum_ms, rt_abs_max_ms,
            positive_rt_count, zero_rt_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(um, dm, rpctype) DO UPDATE SET
            call_count = graph_edges.call_count + excluded.call_count,
            rt_abs_sum_ms = graph_edges.rt_abs_sum_ms + excluded.rt_abs_sum_ms,
            rt_abs_max_ms = MAX(graph_edges.rt_abs_max_ms, excluded.rt_abs_max_ms),
            positive_rt_count = graph_edges.positive_rt_count + excluded.positive_rt_count,
            zero_rt_count = graph_edges.zero_rt_count + excluded.zero_rt_count
    """

    for _, chunk in iter_csv_chunks([file_path], MS_CALLGRAPH_COLUMNS, chunk_size):
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
                rt_abs_sum_ms=("rt_abs", "sum"),
                rt_abs_max_ms=("rt_abs", "max"),
                positive_rt_count=("positive_rt", "sum"),
                zero_rt_count=("zero_rt", "sum"),
            )
            .reset_index()
        )
        conn.executemany(
            sql,
            [
                (
                    row.um,
                    row.dm,
                    row.rpctype,
                    float(row.call_count),
                    float(row.rt_abs_sum_ms),
                    float(row.rt_abs_max_ms),
                    float(row.positive_rt_count),
                    float(row.zero_rt_count),
                )
                for row in grouped.itertuples(index=False)
            ],
        )
        log(f"Ingested callgraph chunk from {file_path.name}; rows={len(chunk)}")

    return total_rows, valid_rows


def ingest_node(conn: sqlite3.Connection, file_path: Path, chunk_size: int) -> tuple[int, int]:
    total_rows = 0
    valid_rows = 0
    sql = """
        INSERT INTO node_stats (
            nodeid, observations, cpu_sum, memory_sum, max_cpu_utilization,
            max_memory_utilization, timestamp_min, timestamp_max
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(nodeid) DO UPDATE SET
            observations = node_stats.observations + excluded.observations,
            cpu_sum = node_stats.cpu_sum + excluded.cpu_sum,
            memory_sum = node_stats.memory_sum + excluded.memory_sum,
            max_cpu_utilization = MAX(node_stats.max_cpu_utilization, excluded.max_cpu_utilization),
            max_memory_utilization = MAX(node_stats.max_memory_utilization, excluded.max_memory_utilization),
            timestamp_min = MIN(node_stats.timestamp_min, excluded.timestamp_min),
            timestamp_max = MAX(node_stats.timestamp_max, excluded.timestamp_max)
    """

    for _, chunk in iter_csv_chunks([file_path], NODE_COLUMNS, chunk_size):
        total_rows += len(chunk)
        chunk["nodeid"] = chunk["nodeid"].astype(str).str.strip()
        chunk["timestamp"] = pd.to_numeric(chunk["timestamp"], errors="coerce")
        chunk["cpu_utilization"] = pd.to_numeric(chunk["cpu_utilization"], errors="coerce")
        chunk["memory_utilization"] = pd.to_numeric(chunk["memory_utilization"], errors="coerce")
        valid = chunk.dropna(subset=["nodeid", "timestamp", "cpu_utilization", "memory_utilization"])
        valid = valid[valid["nodeid"] != ""]
        valid_rows += len(valid)
        if valid.empty:
            continue

        grouped = (
            valid.groupby("nodeid", sort=False)
            .agg(
                observations=("nodeid", "size"),
                cpu_sum=("cpu_utilization", "sum"),
                memory_sum=("memory_utilization", "sum"),
                max_cpu_utilization=("cpu_utilization", "max"),
                max_memory_utilization=("memory_utilization", "max"),
                timestamp_min=("timestamp", "min"),
                timestamp_max=("timestamp", "max"),
            )
            .reset_index()
        )
        conn.executemany(
            sql,
            [
                (
                    row.nodeid,
                    float(row.observations),
                    float(row.cpu_sum),
                    float(row.memory_sum),
                    float(row.max_cpu_utilization),
                    float(row.max_memory_utilization),
                    float(row.timestamp_min),
                    float(row.timestamp_max),
                )
                for row in grouped.itertuples(index=False)
            ],
        )
        log(f"Ingested node chunk from {file_path.name}; rows={len(chunk)}")

    return total_rows, valid_rows


def ingest_resource(conn: sqlite3.Connection, file_path: Path, chunk_size: int) -> tuple[int, int]:
    total_rows = 0
    valid_rows = 0
    sql = """
        INSERT INTO ms_node_stats (
            msname, nodeid, observations, cpu_sum, memory_sum,
            max_cpu_utilization, max_memory_utilization, timestamp_min, timestamp_max
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(msname, nodeid) DO UPDATE SET
            observations = ms_node_stats.observations + excluded.observations,
            cpu_sum = ms_node_stats.cpu_sum + excluded.cpu_sum,
            memory_sum = ms_node_stats.memory_sum + excluded.memory_sum,
            max_cpu_utilization = MAX(ms_node_stats.max_cpu_utilization, excluded.max_cpu_utilization),
            max_memory_utilization = MAX(ms_node_stats.max_memory_utilization, excluded.max_memory_utilization),
            timestamp_min = MIN(ms_node_stats.timestamp_min, excluded.timestamp_min),
            timestamp_max = MAX(ms_node_stats.timestamp_max, excluded.timestamp_max)
    """

    for _, chunk in iter_csv_chunks([file_path], MS_RESOURCE_COLUMNS, chunk_size):
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
        valid_rows += len(valid)
        if valid.empty:
            continue

        grouped = (
            valid.groupby(["msname", "nodeid"], sort=False)
            .agg(
                observations=("msname", "size"),
                cpu_sum=("cpu_utilization", "sum"),
                memory_sum=("memory_utilization", "sum"),
                max_cpu_utilization=("cpu_utilization", "max"),
                max_memory_utilization=("memory_utilization", "max"),
                timestamp_min=("timestamp", "min"),
                timestamp_max=("timestamp", "max"),
            )
            .reset_index()
        )
        conn.executemany(
            sql,
            [
                (
                    row.msname,
                    row.nodeid,
                    float(row.observations),
                    float(row.cpu_sum),
                    float(row.memory_sum),
                    float(row.max_cpu_utilization),
                    float(row.max_memory_utilization),
                    float(row.timestamp_min),
                    float(row.timestamp_max),
                )
                for row in grouped.itertuples(index=False)
            ],
        )
        log(f"Ingested resource chunk from {file_path.name}; rows={len(chunk)}")

    return total_rows, valid_rows


def ingest_file(args: argparse.Namespace) -> None:
    conn = connect_db(Path(args.db_path))
    init_db(conn)
    file_path = Path(args.file).expanduser().resolve()
    source_id = args.source_id or f"{args.component}:{file_path.name}"

    if is_ingested(conn, source_id):
        log(f"Skipping already ingested source: {source_id}")
        return
    if not file_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {file_path}")

    try:
        conn.execute("BEGIN")
        if args.component == "callgraph":
            total_rows, valid_rows = ingest_callgraph(conn, file_path, args.chunk_size)
        elif args.component == "node":
            total_rows, valid_rows = ingest_node(conn, file_path, args.chunk_size)
        elif args.component == "resource":
            total_rows, valid_rows = ingest_resource(conn, file_path, args.chunk_size)
        else:
            raise ValueError(f"Unsupported component: {args.component}")

        mark_ingested(conn, source_id, args.component, file_path, total_rows, valid_rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    log(
        f"Ingested source={source_id}; component={args.component}; "
        f"rows={total_rows}; valid_rows={valid_rows}"
    )


def read_node_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    node_df = pd.read_sql_query("SELECT * FROM node_stats", conn)
    if node_df.empty:
        return node_df
    node_df["avg_cpu_utilization"] = node_df["cpu_sum"] / node_df["observations"]
    node_df["avg_memory_utilization"] = node_df["memory_sum"] / node_df["observations"]
    node_df["available_cpu_score"] = (1.0 - node_df["avg_cpu_utilization"]).clip(lower=0.0, upper=1.0)
    node_df["available_memory_score"] = (1.0 - node_df["avg_memory_utilization"]).clip(lower=0.0, upper=1.0)
    node_df["available_score"] = 0.5 * node_df["available_cpu_score"] + 0.5 * node_df["available_memory_score"]
    columns = [
        "nodeid",
        "observations",
        "avg_cpu_utilization",
        "avg_memory_utilization",
        "max_cpu_utilization",
        "max_memory_utilization",
        "timestamp_min",
        "timestamp_max",
        "available_cpu_score",
        "available_memory_score",
        "available_score",
    ]
    return node_df[columns].sort_values(["available_score", "observations"], ascending=False).reset_index(drop=True)


def read_graph_edges(conn: sqlite3.Connection, min_call_count: int) -> pd.DataFrame:
    edge_df = pd.read_sql_query("SELECT * FROM graph_edges", conn)
    if edge_df.empty:
        return edge_df
    edge_df = edge_df[edge_df["call_count"] >= min_call_count].copy()
    if edge_df.empty:
        return edge_df
    edge_df["rt_abs_mean_ms"] = edge_df["rt_abs_sum_ms"] / edge_df["call_count"].replace(0, np.nan)
    edge_df["call_weight"] = edge_df["call_count"] / edge_df["call_count"].max()
    max_rt_mean = float(edge_df["rt_abs_mean_ms"].max())
    edge_df["rt_weight"] = edge_df["rt_abs_mean_ms"] / max_rt_mean if max_rt_mean > 0 else 0.0
    edge_df["graph_score"] = edge_df["call_weight"] * (1.0 + edge_df["rt_weight"])
    edge_df = edge_df.sort_values(["graph_score", "call_count"], ascending=False).reset_index(drop=True)
    edge_df["rank"] = np.arange(1, len(edge_df) + 1)
    return edge_df


def read_ms_summaries(
    conn: sqlite3.Connection,
    node_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ms_node_df = pd.read_sql_query("SELECT * FROM ms_node_stats", conn)
    if ms_node_df.empty:
        return pd.DataFrame(), ms_node_df

    ms_node_df["avg_cpu_utilization"] = ms_node_df["cpu_sum"] / ms_node_df["observations"]
    ms_node_df["avg_memory_utilization"] = ms_node_df["memory_sum"] / ms_node_df["observations"]
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
    return ms_summary, ms_node_df


def build_placement_candidates(
    ms_summary: pd.DataFrame,
    ms_node_df: pd.DataFrame,
    top_services: int,
    candidate_nodes_per_service: int,
) -> pd.DataFrame:
    top_ms = set(ms_summary.head(top_services)["msname"])
    candidates = ms_node_df[ms_node_df["msname"].isin(top_ms)].copy()
    if candidates.empty:
        return candidates
    candidates["candidate_rank"] = (
        candidates.groupby("msname")["affinity_score"].rank(method="first", ascending=False).astype(int)
    )
    candidates = candidates[candidates["candidate_rank"] <= candidate_nodes_per_service]
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


def build_topology(
    node_df: pd.DataFrame,
    edge_nodes: int,
    neighbor_degree: int,
    base_link_latency_ms: float,
    hop_latency_step_ms: float,
    link_bandwidth_mbps: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    selected = node_df.sort_values(["available_score", "observations"], ascending=False).head(edge_nodes).copy()
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
        for hop in range(1, min(neighbor_degree, node_count - 1) + 1):
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
            latency_ms = base_link_latency_ms + hop * hop_latency_step_ms + utilization_gap * 10.0
            links.append(
                {
                    "source": source_row["edge_node_id"],
                    "target": target_row["edge_node_id"],
                    "latency_ms": round(latency_ms, 3),
                    "bandwidth_mbps": link_bandwidth_mbps,
                    "link_type": "synthetic_ring_chord",
                    "is_bidirectional": True,
                }
            )

    return topology_nodes, pd.DataFrame(links)


def source_metadata(conn: sqlite3.Connection) -> dict[str, object]:
    sources = pd.read_sql_query("SELECT * FROM ingested_sources ORDER BY ingested_at", conn)
    if sources.empty:
        return {"sources": [], "total_rows_by_component": {}, "valid_rows_by_component": {}}
    return {
        "sources": sources.to_dict(orient="records"),
        "total_rows_by_component": sources.groupby("component")["total_rows"].sum().astype(int).to_dict(),
        "valid_rows_by_component": sources.groupby("component")["valid_rows"].sum().astype(int).to_dict(),
    }


def finalize_outputs(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = connect_db(Path(args.db_path))
    init_db(conn)

    node_df = read_node_summary(conn)
    edge_df = read_graph_edges(conn, args.min_call_count)
    ms_summary, ms_node_df = read_ms_summaries(conn, node_df)

    if not edge_df.empty:
        save_csv(edge_df.head(args.top_edges), output_dir / "graph_edges.csv")
        save_csv(build_service_graph_scores(edge_df).head(args.top_services), output_dir / "service_graph_scores.csv")
    if not node_df.empty:
        save_csv(node_df, output_dir / "node_resource_summary.csv")
    if not ms_summary.empty:
        save_csv(ms_summary, output_dir / "ms_resource_summary.csv")
        save_csv(ms_node_df, output_dir / "ms_node_affinity.csv")
        placements = build_placement_candidates(
            ms_summary,
            ms_node_df,
            args.top_services,
            args.candidate_nodes_per_service,
        )
        save_csv(placements, output_dir / "placement_candidates.csv")
    if not node_df.empty:
        topology_nodes, topology_links = build_topology(
            node_df,
            args.edge_nodes,
            args.neighbor_degree,
            args.base_link_latency_ms,
            args.hop_latency_step_ms,
            args.link_bandwidth_mbps,
        )
        save_csv(topology_nodes, output_dir / "edge_topology_nodes.csv")
        save_csv(topology_links, output_dir / "edge_topology_links.csv")

    metadata = source_metadata(conn)
    metadata.update(
        {
            "db_path": str(Path(args.db_path).expanduser().resolve()),
            "min_call_count": args.min_call_count,
            "edge_count_after_filter": int(len(edge_df)),
            "top_edges_saved": int(min(len(edge_df), args.top_edges)) if not edge_df.empty else 0,
            "node_count": int(len(node_df)),
            "ms_count": int(len(ms_summary)),
            "ms_node_pair_count": int(len(ms_node_df)),
            "edge_nodes": args.edge_nodes,
            "neighbor_degree": args.neighbor_degree,
        }
    )
    save_json(metadata, output_dir / "streaming_metadata.json")
    save_summary_json(output_dir, edge_df, node_df, ms_summary, ms_node_df)


def save_summary_json(
    output_dir: Path,
    edge_df: pd.DataFrame,
    node_df: pd.DataFrame,
    ms_summary: pd.DataFrame,
    ms_node_df: pd.DataFrame,
) -> None:
    summary: dict[str, object] = {
        "graph_edges": int(len(edge_df)),
        "nodes": int(len(node_df)),
        "microservices": int(len(ms_summary)),
        "ms_node_pairs": int(len(ms_node_df)),
    }
    if not edge_df.empty:
        summary["graph_stats"] = {
            "unique_upstream_services": int(edge_df["um"].nunique()),
            "unique_downstream_services": int(edge_df["dm"].nunique()),
            "call_count_p50": float(edge_df["call_count"].quantile(0.50)),
            "call_count_p95": float(edge_df["call_count"].quantile(0.95)),
            "call_count_p99": float(edge_df["call_count"].quantile(0.99)),
            "call_count_max": float(edge_df["call_count"].max()),
            "rt_abs_mean_p50_ms": float(edge_df["rt_abs_mean_ms"].quantile(0.50)),
            "rt_abs_mean_p95_ms": float(edge_df["rt_abs_mean_ms"].quantile(0.95)),
            "rt_abs_mean_p99_ms": float(edge_df["rt_abs_mean_ms"].quantile(0.99)),
            "rt_abs_mean_max_ms": float(edge_df["rt_abs_mean_ms"].max()),
        }
    if not node_df.empty:
        summary["resource_stats"] = {
            "available_score_p50": float(node_df["available_score"].quantile(0.50)),
            "available_score_p95": float(node_df["available_score"].quantile(0.95)),
            "available_score_p99": float(node_df["available_score"].quantile(0.99)),
        }
    if not ms_summary.empty:
        summary.setdefault("resource_stats", {})
        summary["resource_stats"].update(
            {
                "ms_node_count_p50": float(ms_summary["node_count"].quantile(0.50)),
                "ms_node_count_p95": float(ms_summary["node_count"].quantile(0.95)),
                "ms_node_count_p99": float(ms_summary["node_count"].quantile(0.99)),
                "resource_demand_score_p50": float(ms_summary["resource_demand_score"].quantile(0.50)),
                "resource_demand_score_p95": float(ms_summary["resource_demand_score"].quantile(0.95)),
                "resource_demand_score_p99": float(ms_summary["resource_demand_score"].quantile(0.99)),
            }
        )
    save_json(summary, output_dir / "streaming_summary.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Streaming Alibaba microservices preprocessing.")
    parser.add_argument("--mode", required=True, choices=["init", "is_ingested", "ingest", "finalize"])
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--component", choices=["node", "resource", "callgraph"])
    parser.add_argument("--file")
    parser.add_argument("--source-id")
    parser.add_argument("--chunk-size", type=int, default=500000)
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
    if args.min_call_count <= 0:
        raise ValueError("--min-call-count must be positive")
    if args.top_edges <= 0:
        raise ValueError("--top-edges must be positive")
    if args.top_services <= 0:
        raise ValueError("--top-services must be positive")
    if args.candidate_nodes_per_service <= 0:
        raise ValueError("--candidate-nodes-per-service must be positive")
    if args.mode == "ingest" and (not args.component or not args.file):
        raise ValueError("--mode ingest requires --component and --file")
    if args.mode == "is_ingested" and not args.source_id:
        raise ValueError("--mode is_ingested requires --source-id")


def main() -> None:
    args = parse_args()
    validate_args(args)
    load_dependencies()

    conn = connect_db(Path(args.db_path))
    init_db(conn)

    if args.mode == "init":
        log(f"Initialized streaming DB: {Path(args.db_path).expanduser().resolve()}")
    elif args.mode == "is_ingested":
        raise SystemExit(0 if is_ingested(conn, args.source_id) else 1)
    elif args.mode == "ingest":
        ingest_file(args)
    elif args.mode == "finalize":
        finalize_outputs(args)
    else:
        raise ValueError(f"Unsupported mode: {args.mode}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, FileNotFoundError, ValueError) as exc:
        log(f"ERROR: {exc}")
        raise SystemExit(1) from exc
