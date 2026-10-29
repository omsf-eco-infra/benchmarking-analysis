"""Build-time data for the static dashboard prototype."""

import hashlib
import json
import math
from pathlib import Path
import re
import tempfile

import duckdb

PRICING_FILE = "2026-04-pricing.json"
METADATA_FILE = "ross_dodecahedron_jacs.json"
RBFE_NS_PER_EDGE = 66
METRICS = {
    "ns_per_day": "Throughput (ns/day)",
    "ns_per_dollar": "Cost efficiency (ns/$)",
    "dollars_per_edge": f"Estimated complex-phase cost ($/{RBFE_NS_PER_EDGE} ns edge)",
}


def load_data(conn, data_dir, pricing_file=PRICING_FILE):
    """Load local benchmark, pricing, and system metadata into DuckDB."""
    data_dir = Path(data_dir)
    conn.read_json(str(data_dir / pricing_file)).to_table("instance_prices")
    if conn.sql("""
        SELECT instance_type FROM instance_prices
        GROUP BY instance_type HAVING count(*) > 1
    """).fetchall():
        raise ValueError("Pricing must have one row per instance type")
    conn.execute(
        "CREATE TABLE system_metadata(system VARCHAR, total_system_size BIGINT)"
    )
    metadata = json.loads((data_dir / METADATA_FILE).read_text())
    conn.executemany(
        "INSERT INTO system_metadata VALUES (?, ?)",
        [(name, value["atoms"]["complex"]) for name, value in metadata.items()],
    )
    for workload in ("md", "rbfe"):
        conn.read_parquet(
            str(data_dir / f"{workload}_benchmark_aws_data.parquet")
        ).to_table(f"{workload}_samples")


def summarize(conn, workload):
    """Return the notebook's medians for every system, instance, and MPS count."""
    if workload not in ("md", "rbfe"):
        raise ValueError(f"Unknown workload: {workload}")
    # shortcut: retain the WASM notebook unchanged during the prototype; parity
    # tests guard these queries until the notebook also uses this build module.
    phase_filter = "AND b.phase = 'complex'" if workload == "rbfe" else ""
    result = conn.execute(
        f"""
        SELECT b.system, b.mps_process_count, b.instance_type, s.total_system_size,
               median(b.metric_value) AS ns_per_day,
               median(b.metric_value / NULLIF(p.price_per_hour * 24.0, 0))
                   AS ns_per_dollar
        FROM {workload}_samples b
        LEFT JOIN instance_prices p USING (instance_type)
        LEFT JOIN system_metadata s USING (system)
        WHERE b.benchmark_type = ? {phase_filter}
        GROUP BY b.system, b.mps_process_count, b.instance_type, s.total_system_size
        ORDER BY b.system, b.mps_process_count, b.instance_type
        """,
        [workload],
    )
    columns = [column[0] for column in result.description]
    rows = [dict(zip(columns, values)) for values in result.fetchall()]
    if not rows:
        raise ValueError(f"No {workload} data available")
    for row in rows:
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", row["system"]):
            raise ValueError(f"Unsafe system path: {row['system']!r}")
        if (
            not isinstance(row["mps_process_count"], int)
            or row["mps_process_count"] < 1
        ):
            raise ValueError("MPS counts must be positive integers")
        for key in ("ns_per_day", "ns_per_dollar"):
            value = row[key]
            if value is not None and (not math.isfinite(value) or value < 0):
                raise ValueError(f"Invalid {key} for {row['system']}: {value}")
        efficiency = row["ns_per_dollar"]
        row["dollars_per_edge"] = (
            RBFE_NS_PER_EDGE / efficiency if workload == "rbfe" and efficiency else None
        )
    return rows


def mark_winners(rows, group_key):
    """Copy rows and flag one winner per group, breaking ties by instance name."""
    rows = [dict(row) for row in rows]
    for metric in METRICS:
        for row in rows:
            row[f"winner_{metric}"] = False
        for group in {row[group_key] for row in rows}:
            candidates = [
                row
                for row in rows
                if row[group_key] == group and row[metric] is not None
            ]
            if candidates:
                direction = 1 if metric == "dollars_per_edge" else -1
                winner = min(
                    candidates,
                    key=lambda row: (direction * row[metric], row["instance_type"]),
                )
                winner[f"winner_{metric}"] = True
    return rows


def build_views(conn):
    """Enumerate every dropdown state for build-time export."""
    views = []
    for workload in ("md", "rbfe"):
        rows = summarize(conn, workload)
        counts = sorted({row["mps_process_count"] for row in rows})
        systems = sorted({row["system"] for row in rows})
        for kind, selections in (("mps", counts), ("systems", systems)):
            for selection in selections:
                selected = [
                    row
                    for row in rows
                    if (
                        row["mps_process_count"] == selection
                        if kind == "mps"
                        else row["system"] == selection
                        and row["mps_process_count"] in (1, 2, 4)
                    )
                ]
                if not selected:
                    continue
                selected = mark_winners(
                    selected, "system" if kind == "mps" else "mps_process_count"
                )
                title = (
                    f"{workload.upper()} · {selection} MPS process{'es' if selection != 1 else ''}"
                    if kind == "mps"
                    else f"{workload.upper()} · {selection} MPS scaling"
                )
                metrics = list(METRICS) if workload == "rbfe" else list(METRICS)[:2]
                views.append(
                    {
                        "path": f"{workload}/{kind}/{selection}",
                        "workload": workload,
                        "kind": kind,
                        "selection": selection,
                        "title": title,
                        "charts": [
                            {
                                "metric": metric,
                                "label": METRICS[metric],
                            }
                            for metric in metrics
                        ],
                        "rows": selected,
                    }
                )
    return views


def export_views(data_dir, web_dir, pricing_file=PRICING_FILE):
    """Generate all chart datasets for Astro without network access."""
    data_dir, web_dir = Path(data_dir), Path(web_dir)
    source_names = [pricing_file, METADATA_FILE] + [
        f"{workload}_benchmark_aws_data.parquet" for workload in ("md", "rbfe")
    ]
    with duckdb.connect() as conn:
        load_data(conn, data_dir, pricing_file)
        views = build_views(conn)
    manifest = {
        "pricing_file": pricing_file,
        "rbfe_ns_per_edge": RBFE_NS_PER_EDGE,
        "sources": [
            {
                "file": name,
                "sha256": hashlib.sha256((data_dir / name).read_bytes()).hexdigest(),
            }
            for name in source_names
        ],
        "views": views,
    }
    generated = web_dir / "src" / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    content = json.dumps(manifest, indent=2, allow_nan=False) + "\n"
    with tempfile.TemporaryDirectory(dir=generated) as tmp:
        path = Path(tmp) / "manifest.json"
        path.write_text(content)
        path.replace(generated / "manifest.json")
    return manifest
