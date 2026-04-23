# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair==6.1.0",
#     "duckdb==1.5.2",
#     "marimo>=0.23.2",
#     "polars==1.40.1",
#     "sqlalchemy==2.0.49",
#     "sqlglot>=23.4"
# ]
# ///

import marimo

__generated_with = "0.23.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import polars as pl
    import json
    import os
    import sqlalchemy as sqla
    import sys

    conn = duckdb.connect()
    # conn.execute("INSTALL httpfs;")
    # conn.execute("LOAD httpfs;")
    #
    # conn.execute("""
    #     CREATE OR REPLACE SECRET s3_secret (
    #         TYPE s3,
    #         PROVIDER credential_chain
    #     );
    # """)
    pricing_file_name = "2026-04-pricing.json"
    aws_md_file_name = "md_benchmark_aws_data.parquet"
    pricing_path = str(mo.notebook_location() / "public" / pricing_file_name)
    aws_md_benchmark_path = str(mo.notebook_location() / "public" / aws_md_file_name)
    if "pyodide" in sys.modules:
        # We use this to correctly load into duckdb
        from pyodide.http import pyfetch

        pricing = await pyfetch(pricing_path)
        with open(pricing_file_name, "wb") as dst:
            dst.write(await pricing.bytes())
        md_bench = await pyfetch(aws_md_benchmark_path)
        with open(aws_md_file_name, "wb") as dst:
            dst.write(await md_bench.bytes())
    pricing_path = pricing_file_name
    aws_md_benchmark_path = aws_md_file_name

    _ = conn.read_json(pricing_path).to_table("instance_prices")
    _ = conn.read_parquet(aws_md_benchmark_path).to_table("benchmark_costs")

    return conn, mo


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Ranking
    """)
    return


@app.cell(hide_code=True)
def _(conn, mo):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE VIEW benchmark_costs_with_prices AS
        SELECT
            b.*,
            p.price_per_hour,
            (p.price_per_hour * 24.0) / NULLIF(b.metric_value, 0) AS dollars_per_ns,
            b.metric_value / NULLIF((p.price_per_hour * 24.0), 0) AS ns_per_dollar
        FROM benchmark_costs b
        LEFT JOIN instance_prices p USING (instance_type);
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo):

    mps_process_count_options = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT mps_process_count
            FROM benchmark_costs
            WHERE benchmark_type = 'md'
            ORDER BY mps_process_count
            """
        ).fetchall()
    ]

    mo.stop(
        not mps_process_count_options,
        mo.md("No MD benchmark data available for MPS selection."),
    )

    mps_process_count_dropdown = mo.ui.dropdown(
        options=mps_process_count_options,
        value=mps_process_count_options[0],
        label="MPS process count",
    )

    mps_process_count_dropdown
    return (mps_process_count_dropdown,)


@app.cell(hide_code=True)
def _(conn, mo, mps_process_count_dropdown):
    median_md_cost_mps_1 = mo.sql(
        f"""
        SELECT
            run_date,
            mps_process_count,
            instance_type,
            median(metric_value) AS ns_per_day,
            median(ns_per_dollar) AS ns_per_dollar,
            system
        FROM benchmark_costs_with_prices
        WHERE benchmark_type = 'md'
          AND mps_process_count = {mps_process_count_dropdown.value}
        GROUP BY run_date, mps_process_count, instance_type, system
        ORDER BY run_date, system, ns_per_day DESC;
        """,
        engine=conn,
    )
    return (median_md_cost_mps_1,)


@app.cell
def _():
    import altair as alt

    return (alt,)


@app.cell
def _(alt, median_md_cost_mps_1):
    _md_runtime = (
        alt.Chart(median_md_cost_mps_1)
        .transform_window(
            winner="row_number()",
            groupby=["system"],
            sort=[
                alt.SortField("ns_per_day", order="descending"),
                alt.SortField("instance_type", order="ascending"),  # tie-breaker
            ],
        )
        .mark_bar()
        .encode(
            x=alt.X(
                "instance_type:N",
                title="instance type",
                sort=["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"],
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "ns_per_day:Q",
                title="ns per day",
            ),
            color=alt.condition(
                alt.datum.winner == 1,
                alt.Color("instance_type:N", legend=None),
                alt.value("lightgray"),
            ),
            tooltip=[
                alt.Tooltip("system:N"),
                alt.Tooltip("instance_type:N"),
                alt.Tooltip("ns_per_day:Q", format=".2f"),
            ],
        )
        .facet(
            column=alt.Column(
                "system:N",
                title="MD Performance (ns/day)",
                header=alt.Header(labelOrient="bottom"),
            )
        )
        .properties(
            height=260,
            width=120,
        )
    )
    _md_cost = (
        alt.Chart(median_md_cost_mps_1)
        .transform_window(
            winner="row_number()",
            groupby=["system"],
            sort=[
                alt.SortField("ns_per_dollar", order="descending"),
                alt.SortField("instance_type", order="ascending"),  # tie-breaker
            ],
        )
        .mark_bar()
        .encode(
            x=alt.X(
                "instance_type:N",
                title="instance type",
                sort=["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"],
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "ns_per_dollar:Q",
                title="ns per dollar",
            ),
            color=alt.condition(
                alt.datum.winner == 1,
                alt.Color("instance_type:N", legend=None),
                alt.value("lightgray"),
            ),
            tooltip=[
                alt.Tooltip("system:N"),
                alt.Tooltip("instance_type:N"),
                alt.Tooltip("ns_per_dollar:Q", format=".2f"),
            ],
        )
        .facet(
            column=alt.Column(
                "system:N",
                title="MD Performance (ns/dollar)",
                header=alt.Header(labelOrient="bottom"),
            )
        )
        .properties(
            height=260,
            width=120,
        )
    )

    alt.vconcat(_md_runtime, _md_cost).configure_axis(grid=False)
    return


@app.cell
def _(conn, mo):
    df = mo.sql(
        f"""
        WITH outputs AS (
            SELECT
                filename AS output_filename,
                regexp_extract(
                    filename,
                    'runs/([^/]+)/([^/]+)/output/md_benchmark\\.out$',
                    2
                ) AS run_id,
                *
            FROM read_json(
                's3://benchmark-bucket-omsf-2026/runs/2026-03-*/**/output/md_benchmark.out'
            )
        ),
        manifests AS (
            SELECT
                filename AS manifest_filename,
                regexp_extract(
                    filename,
                    'runs/([^/]+)/([^/]+)/manifest\\.json$',
                    2
                ) AS run_id,
            	split_part(bench_task_id, ':', 4) AS ami,
            	split_part(bench_task_id, ':', 3) AS instance_type
            FROM read_json(
                's3://benchmark-bucket-omsf-2026/runs/2026-03-*/**/manifest.json'
            )
        )
        SELECT
            SPLIT_PART(m.manifest_filename, '/', -3) as date,
            o.* EXCLUDE (output_filename, run_id),
            m.* EXCLUDE (manifest_filename, run_id)
        FROM outputs o
        LEFT JOIN manifests m
            USING (run_id);
        """,
        engine=conn,
    )
    return


@app.cell(hide_code=True)
def _(conn):
    conn.execute(
        r"""
        CREATE OR REPLACE TABLE rbfe_benchmark_costs AS
        WITH manifests AS (
            SELECT
                regexp_extract(filename, 'runs/([^/]+)/([^/]+)/manifest\.json$', 2) AS run_id,
                split_part(bench_task_id, ':', 4) AS instance_type,
                split_part(bench_task_id, ':', 5) AS ami
            FROM read_json(
                's3://benchmark-bucket-omsf-2026/runs/2026-04-*/**/manifest.json'
            )
        ),
        outputs AS (
            SELECT
                regexp_extract(filename, 'runs/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS run_date,
                regexp_extract(filename, 'runs/([^/]+)/([^/]+)/output/rbfe_benchmark\.out$', 2) AS run_id,
                coalesce(try_cast(bace ->> '$.rbfe' AS DOUBLE),     try_cast(bace ->> '$.complex' AS DOUBLE))     AS bace_rbfe_complex,
                coalesce(try_cast(bace ->> '$.mm' AS DOUBLE),       try_cast(bace ->> '$.solvent' AS DOUBLE))     AS bace_rbfe_solvent,
                coalesce(try_cast(p38 ->> '$.rbfe' AS DOUBLE),      try_cast(p38 ->> '$.complex' AS DOUBLE))      AS p38_rbfe_complex,
                coalesce(try_cast(p38 ->> '$.mm' AS DOUBLE),        try_cast(p38 ->> '$.solvent' AS DOUBLE))      AS p38_rbfe_solvent,
                coalesce(try_cast(jnk1 ->> '$.rbfe' AS DOUBLE),     try_cast(jnk1 ->> '$.complex' AS DOUBLE))     AS jnk1_rbfe_complex,
                coalesce(try_cast(jnk1 ->> '$.mm' AS DOUBLE),       try_cast(jnk1 ->> '$.solvent' AS DOUBLE))     AS jnk1_rbfe_solvent,
                coalesce(try_cast(cdk2 ->> '$.rbfe' AS DOUBLE),     try_cast(cdk2 ->> '$.complex' AS DOUBLE))     AS cdk2_rbfe_complex,
                coalesce(try_cast(cdk2 ->> '$.mm' AS DOUBLE),       try_cast(cdk2 ->> '$.solvent' AS DOUBLE))     AS cdk2_rbfe_solvent,
                coalesce(try_cast(ptp1b ->> '$.rbfe' AS DOUBLE),    try_cast(ptp1b ->> '$.complex' AS DOUBLE))    AS ptp1b_rbfe_complex,
                coalesce(try_cast(ptp1b ->> '$.mm' AS DOUBLE),      try_cast(ptp1b ->> '$.solvent' AS DOUBLE))    AS ptp1b_rbfe_solvent,
                coalesce(try_cast(tyk2 ->> '$.rbfe' AS DOUBLE),     try_cast(tyk2 ->> '$.complex' AS DOUBLE))     AS tyk2_rbfe_complex,
                coalesce(try_cast(tyk2 ->> '$.mm' AS DOUBLE),       try_cast(tyk2 ->> '$.solvent' AS DOUBLE))     AS tyk2_rbfe_solvent,
                coalesce(try_cast(mcl1 ->> '$.rbfe' AS DOUBLE),     try_cast(mcl1 ->> '$.complex' AS DOUBLE))     AS mcl1_rbfe_complex,
                coalesce(try_cast(mcl1 ->> '$.mm' AS DOUBLE),       try_cast(mcl1 ->> '$.solvent' AS DOUBLE))     AS mcl1_rbfe_solvent,
                coalesce(try_cast(thrombin ->> '$.rbfe' AS DOUBLE), try_cast(thrombin ->> '$.complex' AS DOUBLE)) AS thrombin_rbfe_complex,
                coalesce(try_cast(thrombin ->> '$.mm' AS DOUBLE),   try_cast(thrombin ->> '$.solvent' AS DOUBLE)) AS thrombin_rbfe_solvent
            FROM read_json(
                's3://benchmark-bucket-omsf-2026/runs/2026-04-*/**/output/rbfe_benchmark.out'
            )
        ),
        metrics_long AS (
            SELECT
                run_date,
                run_id,
                metric_name AS benchmark_phase,
                ns_per_day
            FROM outputs
            UNPIVOT (
                ns_per_day FOR metric_name IN (
                    bace_rbfe_complex,
                    bace_rbfe_solvent,
                    p38_rbfe_complex,
                    p38_rbfe_solvent,
                    jnk1_rbfe_complex,
                    jnk1_rbfe_solvent,
                    cdk2_rbfe_complex,
                    cdk2_rbfe_solvent,
                    ptp1b_rbfe_complex,
                    ptp1b_rbfe_solvent,
                    tyk2_rbfe_complex,
                    tyk2_rbfe_solvent,
                    mcl1_rbfe_complex,
                    mcl1_rbfe_solvent,
                    thrombin_rbfe_complex,
                    thrombin_rbfe_solvent
                )
            )
        )
        SELECT
            'rbfe' AS benchmark_type,
            l.run_date,
            l.run_id,
            split_part(l.benchmark_phase, '_', 1) AS benchmark,
            split_part(l.benchmark_phase, '_', 3) AS type,
            'ns_per_day' AS metric_name,
            l.ns_per_day AS metric_value,
            m.instance_type,
            m.ami,
            p.price_per_hour,
            (p.price_per_hour * 24.0) / NULLIF(l.ns_per_day, 0) AS dollars_per_ns,
            l.ns_per_day / NULLIF(p.price_per_hour * 24.0, 0) AS ns_per_dollar
        FROM metrics_long l
        JOIN manifests m USING (run_id)
        LEFT JOIN instance_prices p USING (instance_type);
        """
    )
    return


@app.cell(hide_code=True)
def _(conn, mo):
    rbfe_benchmark_costs = mo.sql(
        f"""
        SELECT 
            instance_type,
            benchmark,
            median(metric_value) AS ns_per_day,
            median(ns_per_dollar) as ns_per_dollar
        FROM rbfe_benchmark_costs
        WHERE benchmark_type = 'rbfe' and type = 'complex'
        GROUP BY 1, 2
        ORDER BY ns_per_dollar DESC;
        """,
        engine=conn,
    )
    return (rbfe_benchmark_costs,)


@app.cell
def _(alt, rbfe_benchmark_costs):
    chart_new = (
        alt.Chart(rbfe_benchmark_costs)
        .transform_calculate(
            # round to 2 decimals so near-equal values can tie
            ns_per_day_rounded="round(datum.ns_per_day * 100) / 100"
        )
        .transform_window(
            rank="dense_rank()",
            groupby=["benchmark"],
            sort=[alt.SortField("ns_per_day_rounded", order="descending")],
        )
        .mark_bar()
        .encode(
            x=alt.X(
                "instance_type:N",
                title="instance type",
                sort=["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"],
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "ns_per_day:Q",
                title="ns per day",
            ),
            color=alt.condition(
                alt.datum.rank == 1,
                alt.Color("instance_type:N", legend=None),
                alt.value("lightgray"),
            ),
            tooltip=[
                alt.Tooltip("benchmark:N"),
                alt.Tooltip("instance_type:N"),
                alt.Tooltip("ns_per_day:Q", format=",.2f"),
                alt.Tooltip("rank:Q", title="rank"),
            ],
        )
        .facet(
            column=alt.Column(
                "benchmark:N",
                title="benchmark",
                header=alt.Header(labelOrient="bottom"),
            )
        )
        .properties(
            height=260,
            width=120,
        )
        .configure_axis(grid=False)
    )

    chart_new
    return


@app.cell(hide_code=True)
def _(alt, rbfe_benchmark_costs):
    chart = (
        alt.Chart(rbfe_benchmark_costs)
        .transform_calculate(
            # round to 2 decimals so near-equal values can tie
            ns_per_dollar_rounded="round(datum.ns_per_dollar * 100) / 100"
        )
        .transform_window(
            rank="dense_rank()",
            groupby=["benchmark"],
            sort=[alt.SortField("ns_per_dollar_rounded", order="descending")],
        )
        .mark_bar()
        .encode(
            x=alt.X(
                "instance_type:N",
                title="instance type",
                sort=["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"],
                axis=alt.Axis(labelAngle=-45),
            ),
            y=alt.Y(
                "ns_per_dollar:Q",
                title="ns per dollar",
            ),
            color=alt.condition(
                alt.datum.rank == 1,
                alt.Color("instance_type:N", legend=None),
                alt.value("lightgray"),
            ),
            tooltip=[
                alt.Tooltip("benchmark:N"),
                alt.Tooltip("instance_type:N"),
                alt.Tooltip("ns_per_dollar:Q", format=",.2f"),
                alt.Tooltip("rank:Q", title="rank"),
            ],
        )
        .facet(
            column=alt.Column(
                "benchmark:N",
                title="RBFE Performance (ns/dollar)",
                header=alt.Header(labelOrient="bottom"),
            )
        )
        .properties(
            height=260,
            width=120,
        )
        .configure_axis(grid=False)
    )

    chart
    return


if __name__ == "__main__":
    app.run()
