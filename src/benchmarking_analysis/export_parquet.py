import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs;")
conn.execute("LOAD httpfs;")
conn.read_json("2026-04-pricing.json").create_view("instance_prices")

conn.execute("""
    CREATE OR REPLACE SECRET s3_secret (
        TYPE s3,
        PROVIDER credential_chain
    );
""")
conn.execute(
    r"""
    CREATE OR REPLACE TEMP VIEW md_outputs_raw AS
    SELECT
        regexp_extract(filename, 'runs/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS run_date,
        regexp_extract(filename, 'runs/([^/]+)/([^/]+)/output/md_benchmark\.out$', 2)     AS run_id,
        bace,
        p38,
        jnk1,
        cdk2,
        ptp1b,
        tyk2,
        mcl1,
        thrombin
    FROM read_json(
        's3://benchmark-bucket-omsf-2026/runs/2026-*-*/**/output/md_benchmark.out'
    );
    """
)

conn.execute(
    r"""
    CREATE OR REPLACE TEMP VIEW md_manifests_raw AS
    SELECT
        regexp_extract(filename, 'runs/([^/]+)/([^/]+)/manifest\.json$', 2) AS run_id,
        instance_type,
        ami_id AS ami,
        mps_process_count,
    FROM read_json(
        's3://benchmark-bucket-omsf-2026/runs/2026-*-*/**/manifest.json'
    )
    WHERE schema_version = 4;
    """
)

conn.execute(
    """
    CREATE OR REPLACE TEMP VIEW md_metrics_long AS
    SELECT
        run_date,
        run_id,
        regexp_replace(metric_name, '_ns_per_day$', '') AS benchmark,
        ns_per_day
    FROM (
        SELECT
            run_date,
            run_id,
            bace AS bace_ns_per_day,
            p38 AS p38_ns_per_day,
            jnk1 AS jnk1_ns_per_day,
            cdk2 AS cdk2_ns_per_day,
            ptp1b AS ptp1b_ns_per_day,
            tyk2 AS tyk2_ns_per_day,
            mcl1 AS mcl1_ns_per_day,
            thrombin AS thrombin_ns_per_day
        FROM md_outputs_raw
    )
    UNPIVOT (
        ns_per_day FOR metric_name IN (
            bace_ns_per_day,
            p38_ns_per_day,
            jnk1_ns_per_day,
            cdk2_ns_per_day,
            ptp1b_ns_per_day,
            tyk2_ns_per_day,
            mcl1_ns_per_day,
            thrombin_ns_per_day
        )
    );
    """
)

conn.execute(
    """
    COPY (
        SELECT
            'md' AS benchmark_type,
            mps_process_count,
            l.run_date,
            l.run_id,
            l.benchmark as system,
            'ns_per_day' AS metric_name,
            l.ns_per_day AS metric_value,
            m.instance_type,
            m.ami,
        FROM md_metrics_long l
        JOIN md_manifests_raw m USING (run_id)
        LEFT JOIN instance_prices p USING (instance_type)
    ) TO 'md_benchmark_aws_data.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)
