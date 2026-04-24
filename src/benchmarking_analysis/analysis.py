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

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
async def _():
    import marimo as mo
    import duckdb
    import polars as pl
    import json
    import os
    import sys

    conn = duckdb.connect()
    pricing_file_name = "2026-04-pricing.json"
    aws_md_file_name = "md_benchmark_aws_data.parquet"
    aws_rbfe_file_name = "rbfe_benchmark_aws_data.parquet"
    pricing_path = str(mo.notebook_location() / "public" / pricing_file_name)
    aws_md_benchmark_path = str(mo.notebook_location() / "public" / aws_md_file_name)
    aws_rbfe_benchmark_path = str(
        mo.notebook_location() / "public" / aws_rbfe_file_name
    )
    if "pyodide" in sys.modules:
        # We use this to correctly load into duckdb
        from pyodide.http import pyfetch

        pricing = await pyfetch(pricing_path)
        with open(pricing_file_name, "wb") as dst:
            dst.write(await pricing.bytes())
        md_bench = await pyfetch(aws_md_benchmark_path)
        with open(aws_md_file_name, "wb") as dst:
            dst.write(await md_bench.bytes())
        rbfe_bench = await pyfetch(aws_rbfe_benchmark_path)
        with open(aws_rbfe_file_name, "wb") as dst:
            dst.write(await rbfe_bench.bytes())
        pricing_path = pricing_file_name
        aws_md_benchmark_path = aws_md_file_name
        aws_rbfe_benchmark_path = aws_rbfe_file_name

    _ = conn.read_json(pricing_path).to_table("instance_prices")
    _ = conn.read_parquet(aws_md_benchmark_path).to_table("benchmark_costs")
    _ = conn.read_parquet(aws_rbfe_benchmark_path).to_table("rbfe_benchmark_costs")
    return conn, mo


@app.cell(hide_code=True)
def _(benchmark_costs, conn, mo):
    _df = mo.sql(
        f"""
        SELECT
            system,
            instance_type,
            mps_process_count,
            COUNT(*) AS n_runs
        FROM benchmark_costs
        WHERE benchmark_type = 'md'
        GROUP BY system, instance_type, mps_process_count
        ORDER BY system, mps_process_count, instance_type;
        """,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def _(benchmark_costs, conn, instance_prices, mo):
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
        engine=conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Molecular dynamics (MD) benchmarks

    Compare raw throughput and cost efficiency across instance types for the selected MPS process count.

    ---
    """)
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
def _(benchmark_costs_with_prices, conn, mo, mps_process_count_dropdown):
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
        engine=conn
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Relative binding free energy (RBFE) benchmarks

    Compare RBFE complex-phase throughput and cost efficiency across instance types for the selected MPS process count.

    ---
    """)
    return


@app.cell(hide_code=True)
def _(conn, instance_prices, mo, rbfe_benchmark_costs):
    _df = mo.sql(
        f"""
        CREATE OR REPLACE VIEW rbfe_benchmark_costs_with_prices AS
        SELECT
            b.*,
            p.price_per_hour,
            (p.price_per_hour * 24.0) / NULLIF(b.metric_value, 0) AS dollars_per_ns,
            b.metric_value / NULLIF((p.price_per_hour * 24.0), 0) AS ns_per_dollar
        FROM rbfe_benchmark_costs b
        LEFT JOIN instance_prices p USING (instance_type);
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    rbfe_mps_process_count_options = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT mps_process_count
            FROM rbfe_benchmark_costs
            WHERE benchmark_type = 'rbfe'
            ORDER BY mps_process_count
            """
        ).fetchall()
    ]

    mo.stop(
        not rbfe_mps_process_count_options,
        mo.md("No RBFE benchmark data available for MPS selection."),
    )

    rbfe_mps_process_count_dropdown = mo.ui.dropdown(
        options=rbfe_mps_process_count_options,
        value=rbfe_mps_process_count_options[0],
        label="RBFE MPS process count",
    )

    rbfe_mps_process_count_dropdown
    return (rbfe_mps_process_count_dropdown,)


@app.cell(hide_code=True)
def _(
    conn,
    mo,
    rbfe_benchmark_costs_with_prices,
    rbfe_mps_process_count_dropdown,
):
    median_rbfe_cost = mo.sql(
        f"""
        SELECT
            mps_process_count,
            instance_type,
            median(metric_value) AS ns_per_day,
            median(ns_per_dollar) AS ns_per_dollar,
            system
        FROM rbfe_benchmark_costs_with_prices
        WHERE benchmark_type = 'rbfe'
          AND mps_process_count = {rbfe_mps_process_count_dropdown.value}
          AND phase = 'complex'
        GROUP BY mps_process_count, instance_type, system
        ORDER BY system, ns_per_day DESC;
        """,
        engine=conn
    )
    return (median_rbfe_cost,)


@app.cell
def _(alt, median_rbfe_cost):
    _md_runtime = (
        alt.Chart(median_rbfe_cost)
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
                title="RBFE Performance (ns/day)",
                header=alt.Header(labelOrient="bottom"),
            )
        )
        .properties(
            height=260,
            width=120,
        )
    )
    _md_cost = (
        alt.Chart(median_rbfe_cost)
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
                title="RBFE Performance (ns/dollar)",
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


if __name__ == "__main__":
    app.run()
