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

__generated_with = "0.23.1"
app = marimo.App(width="full", app_title="OMSF GPU Cloud Benchmarking")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # OMSF GPU cloud benchmark analysis

    This notebook showcases an interactive benchmark dashboard for comparing AWS GPU instance types on molecular dynamics (MD) with OpenMM and relative binding free energy (RBFE) with Open Free Energy. We also showcase the use of Nvidia's Multiprocess Service (MPS) for concurrent GPU sharing across multiple processes.

    It combines benchmark throughput data with on-demand instance pricing to highlight:

    - **Raw performance** across GPU instance families
    - **Cost efficiency** metrics such as ns/day, ns per dollar, and dollars per ns
    - **MPS process-count sensitivity** through the controls below
    - **Best-instance comparisons** with winner highlighting for each benchmark system

    The benchmark was developed by Open Free Energy for more information checkout the following repo: [OpenFreeEnergy/performance_benchmarks](https://github.com/OpenFreeEnergy/performance_benchmarks)

    Additionally, the entire infrastructure for working with cloud providers to achieve this can be found here: [omsf-eco-infra/benchmarking-orchestration](https://github.com/omsf-eco-infra/benchmarking-orchestration).

    Use the dropdowns and charts to explore how instance choice and concurrency affect both performance and cost.
    """)
    return


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
    aws_md_benchmark_path = str(
        mo.notebook_location() / "public" / aws_md_file_name
    )
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
    chart_height = 260
    chart_width = 120
    chart_height, chart_width
    return chart_height, chart_width, conn, mo


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
        output=False,
        engine=conn
    )
    return


@app.cell(hide_code=True)
def _(mo):
    md_benchmarks_intro = mo.md(r"""
    Compare raw throughput and cost efficiency across instance types for the selected MPS process count. Increasing MPS is how many more simulations you run on concurrently on a GPU.
    """)
    return (md_benchmarks_intro,)


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
        label="MPS process count (Simultaneous simulations / GPU)",
    )
    return (mps_process_count_dropdown,)


@app.cell(hide_code=True)
def _(benchmark_costs_with_prices, conn, mo, mps_process_count_dropdown):
    median_md_cost_mps_1 = mo.sql(
        f"""
        SELECT
            mps_process_count,
            instance_type,
            median(metric_value) AS ns_per_day,
            median(ns_per_dollar) AS ns_per_dollar,
            system
        FROM benchmark_costs_with_prices
        WHERE benchmark_type = 'md'
          AND mps_process_count = {mps_process_count_dropdown.value}
        GROUP BY mps_process_count, instance_type, system
        ORDER BY system, ns_per_day DESC;
        """,
        output=False,
        engine=conn
    )
    return (median_md_cost_mps_1,)


@app.cell
def _():
    import altair as alt

    return (alt,)


@app.cell
def _(alt, chart_height, chart_width, median_md_cost_mps_1):
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
            height=chart_height,
            width=chart_width,
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
            height=chart_height,
            width=chart_width,
        )
    )

    md_benchmarks_chart = alt.vconcat(_md_runtime, _md_cost).configure_axis(
        grid=False
    )
    return (md_benchmarks_chart,)


@app.cell(hide_code=True)
def _(mo):
    md_mps_intro = mo.md(r"""
    Select a benchmark system to compare instance-type performance across MPS process counts `1`, `2`, and `4`.
    """)
    return (md_mps_intro,)


@app.cell
def _(conn, mo):
    md_system_options = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT system
            FROM benchmark_costs
            WHERE benchmark_type = 'md'
            ORDER BY system
            """
        ).fetchall()
    ]

    mo.stop(
        not md_system_options,
        mo.md("No MD benchmark systems available for system selection."),
    )

    md_system_dropdown = mo.ui.dropdown(
        options=md_system_options,
        value=md_system_options[0],
        label="MD system",
    )
    return (md_system_dropdown,)


@app.cell(hide_code=True)
def _(benchmark_costs_with_prices, conn, md_system_dropdown, mo):
    md_system_sql = md_system_dropdown.value.replace("'", "''")

    md_system_mps_comparison = mo.sql(
        f"""
        SELECT
            system,
            mps_process_count,
            instance_type,
            median(metric_value) AS ns_per_day,
            median(ns_per_dollar) AS ns_per_dollar
        FROM benchmark_costs_with_prices
        WHERE benchmark_type = 'md'
          AND system = '{md_system_sql}'
          AND mps_process_count IN (1, 2, 4)
        GROUP BY system, mps_process_count, instance_type
        ORDER BY mps_process_count, instance_type;
        """,
        engine=conn,
        output=False,
    )
    return (md_system_mps_comparison,)


@app.cell
def _(
    alt,
    chart_height,
    chart_width,
    md_system_dropdown,
    md_system_mps_comparison,
):
    _instance_sort = ["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"]
    _mps_sort = [1, 2, 4]
    _instance_axis = alt.X(
        "instance_type:N",
        title="instance type",
        sort=_instance_sort,
        axis=alt.Axis(labelAngle=-35),
    )
    _mps_color = alt.Color(
        "mps_process_count:O",
        title="MPS processes",
        sort=_mps_sort,
    )
    _mps_offset = alt.XOffset("mps_process_count:O", sort=_mps_sort)
    _tooltips = [
        alt.Tooltip("system:N", title="system"),
        alt.Tooltip("instance_type:N", title="instance type"),
        alt.Tooltip("mps_process_count:O", title="MPS processes"),
    ]

    _md_mps_runtime = (
        alt.Chart(md_system_mps_comparison)
        .mark_bar()
        .encode(
            x=_instance_axis,
            xOffset=_mps_offset,
            y=alt.Y("ns_per_day:Q", title="ns per day"),
            color=_mps_color,
            tooltip=_tooltips
            + [alt.Tooltip("ns_per_day:Q", title="ns/day", format=".2f")],
        )
        .properties(
            title=f"MD throughput by instance type: {md_system_dropdown.value}",
            height=chart_height,
            width=chart_width * 3,
        )
    )

    _md_mps_cost = (
        alt.Chart(md_system_mps_comparison)
        .mark_bar()
        .encode(
            x=_instance_axis,
            xOffset=_mps_offset,
            y=alt.Y("ns_per_dollar:Q", title="ns per dollar"),
            color=_mps_color,
            tooltip=_tooltips
            + [alt.Tooltip("ns_per_dollar:Q", title="ns/$", format=".2f")],
        )
        .properties(
            title=f"MD cost efficiency by instance type: {md_system_dropdown.value}",
            height=chart_height,
            width=chart_width * 3,
        )
    )

    md_mps_chart = (
        alt.vconcat(_md_mps_runtime, _md_mps_cost)
        .resolve_scale(color="shared")
        .configure_axis(grid=False)
    )
    return (md_mps_chart,)


@app.cell(hide_code=True)
def _(mo):
    rbfe_benchmarks_intro = mo.md(r"""
    Compare RBFE complex-phase throughput and cost efficiency across instance types for the selected MPS process count.
    """)
    return (rbfe_benchmarks_intro,)


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
        output=False,
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
        output=False,
        engine=conn
    )
    return (median_rbfe_cost,)


@app.cell
def _(alt, chart_height, chart_width, median_rbfe_cost):
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
            height=chart_height,
            width=chart_width,
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
            height=chart_height,
            width=chart_width,
        )
    )

    rbfe_benchmarks_chart = alt.vconcat(_md_runtime, _md_cost).configure_axis(
        grid=False
    )
    return (rbfe_benchmarks_chart,)


@app.cell(hide_code=True)
def _(mo):
    rbfe_mps_intro = mo.md(r"""
    ### RBFE scaling by MPS process count

    Select a benchmark system to compare complex-phase RBFE performance across MPS process counts `1`, `2`, and `4`.
    """)
    return (rbfe_mps_intro,)


@app.cell
def _(conn, mo):
    rbfe_system_options = [
        row[0]
        for row in conn.execute(
            """
            SELECT DISTINCT system
            FROM rbfe_benchmark_costs
            WHERE benchmark_type = 'rbfe'
              AND phase = 'complex'
            ORDER BY system
            """
        ).fetchall()
    ]

    mo.stop(
        not rbfe_system_options,
        mo.md("No RBFE benchmark systems available for system selection."),
    )

    rbfe_system_dropdown = mo.ui.dropdown(
        options=rbfe_system_options,
        value=rbfe_system_options[0],
        label="RBFE system",
    )
    return (rbfe_system_dropdown,)


@app.cell(hide_code=True)
def _(conn, mo, rbfe_benchmark_costs_with_prices, rbfe_system_dropdown):
    rbfe_system_sql = rbfe_system_dropdown.value.replace("'", "''")

    rbfe_system_mps_comparison = mo.sql(
        f"""
        SELECT
            system,
            mps_process_count,
            instance_type,
            median(metric_value) AS ns_per_day,
            median(ns_per_dollar) AS ns_per_dollar
        FROM rbfe_benchmark_costs_with_prices
        WHERE benchmark_type = 'rbfe'
          AND phase = 'complex'
          AND system = '{rbfe_system_sql}'
          AND mps_process_count IN (1, 2, 4)
        GROUP BY system, mps_process_count, instance_type
        ORDER BY instance_type, mps_process_count;
        """,
        engine=conn,
        output=False,
    )
    return (rbfe_system_mps_comparison,)


@app.cell
def _(
    alt,
    chart_height,
    chart_width,
    rbfe_system_dropdown,
    rbfe_system_mps_comparison,
):
    _instance_sort = ["g4dn.xlarge", "g5.xlarge", "g6e.xlarge"]
    _mps_sort = [1, 2, 4]
    _instance_axis = alt.X(
        "instance_type:N",
        title="instance type",
        sort=_instance_sort,
        axis=alt.Axis(labelAngle=-35),
    )
    _mps_color = alt.Color(
        "mps_process_count:O",
        title="MPS processes",
        sort=_mps_sort,
    )
    _mps_offset = alt.XOffset("mps_process_count:O", sort=_mps_sort)
    _tooltips = [
        alt.Tooltip("system:N", title="system"),
        alt.Tooltip("instance_type:N", title="instance type"),
        alt.Tooltip("mps_process_count:O", title="MPS processes"),
    ]

    _rbfe_mps_runtime = (
        alt.Chart(rbfe_system_mps_comparison)
        .mark_bar()
        .encode(
            x=_instance_axis,
            xOffset=_mps_offset,
            y=alt.Y("ns_per_day:Q", title="ns per day"),
            color=_mps_color,
            tooltip=_tooltips
            + [alt.Tooltip("ns_per_day:Q", title="ns/day", format=".2f")],
        )
        .properties(
            title=f"RBFE throughput by instance type: {rbfe_system_dropdown.value}",
            height=chart_height,
            width=chart_width,
        )
    )

    _rbfe_mps_cost = (
        alt.Chart(rbfe_system_mps_comparison)
        .mark_bar()
        .encode(
            x=_instance_axis,
            xOffset=_mps_offset,
            y=alt.Y("ns_per_dollar:Q", title="ns per dollar"),
            color=_mps_color,
            tooltip=_tooltips
            + [alt.Tooltip("ns_per_dollar:Q", title="ns/$", format=".2f")],
        )
        .properties(
            title=f"RBFE cost efficiency by instance type: {rbfe_system_dropdown.value}",
            height=chart_height,
            width=chart_width,
        )
    )

    rbfe_mps_chart = (
        alt.vconcat(_rbfe_mps_runtime, _rbfe_mps_cost)
        .resolve_scale(color="shared")
        .configure_axis(grid=False)
    )
    return (rbfe_mps_chart,)


@app.cell
def _(
    md_benchmarks_chart,
    md_benchmarks_intro,
    mo,
    mps_process_count_dropdown,
):
    mo.vstack(
        [
            mo.md("## Molecular Dynamics (MD) benchmarks using OpenMM"),
            md_benchmarks_intro,
            mps_process_count_dropdown,
            md_benchmarks_chart,
        ]
    )
    return


@app.cell
def _(md_mps_chart, md_mps_intro, md_system_dropdown, mo):
    mo.vstack(
        [
            mo.md("## MPS scaling by MPS Process Count"),
            md_mps_intro,
            md_system_dropdown,
            md_mps_chart,
        ]
    )
    return


@app.cell
def _(
    mo,
    rbfe_benchmarks_chart,
    rbfe_benchmarks_intro,
    rbfe_mps_process_count_dropdown,
):
    mo.vstack(
        [
            rbfe_benchmarks_intro,
            rbfe_mps_process_count_dropdown,
            rbfe_benchmarks_chart,
        ]
    )
    return


@app.cell
def _(mo, rbfe_mps_chart, rbfe_mps_intro, rbfe_system_dropdown):
    mo.vstack([rbfe_mps_intro, rbfe_system_dropdown, rbfe_mps_chart])
    return


if __name__ == "__main__":
    app.run()
