import duckdb

conn = duckdb.connect()
conn.execute("INSTALL httpfs;")
conn.execute("LOAD httpfs;")


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
        CAST(regexp_extract(filename, 'runs/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS DATE) AS run_date,
        regexp_extract(filename, 'runs/([^/]+)/([^/]+)/output/md_benchmark\.out$', 2) AS run_id,
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
    CREATE OR REPLACE TEMP VIEW rbfe_outputs_raw AS
    SELECT
        CAST(regexp_extract(filename, 'runs/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS DATE) AS run_date,
        regexp_extract(filename, 'runs/([^/]+)/([^/]+)/output/rbfe_benchmark\.out$', 2) AS run_id,
        bace,
        p38,
        jnk1,
        cdk2,
        ptp1b,
        tyk2,
        mcl1,
        thrombin
    FROM read_json(
        's3://benchmark-bucket-omsf-2026/runs/2026-*-*/**/output/rbfe_benchmark.out'
    );
    """
)

conn.execute(
    r"""
    CREATE OR REPLACE TEMP VIEW manifests_raw AS
    SELECT
        regexp_extract(filename, 'runs/([^/]+)/([^/]+)/manifest\.json$', 2) AS run_id,
        instance_type,
        ami_id AS ami,
        mps_process_count
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
    CREATE OR REPLACE TEMP VIEW rbfe_metrics_long AS
    SELECT
        run_date,
        run_id,
        regexp_replace(metric_name, '_(complex|solvent)$', '') AS benchmark,
        CASE
            WHEN metric_name LIKE '%_complex' THEN 'complex'
            WHEN metric_name LIKE '%_solvent' THEN 'solvent'
        END AS phase,
        metric_value
    FROM (
        SELECT
            run_date,
            run_id,
            CAST(bace->>'complex' AS DOUBLE) AS bace_complex,
            CAST(bace->>'solvent' AS DOUBLE) AS bace_solvent,
            CAST(p38->>'complex' AS DOUBLE) AS p38_complex,
            CAST(p38->>'solvent' AS DOUBLE) AS p38_solvent,
            CAST(jnk1->>'complex' AS DOUBLE) AS jnk1_complex,
            CAST(jnk1->>'solvent' AS DOUBLE) AS jnk1_solvent,
            CAST(cdk2->>'complex' AS DOUBLE) AS cdk2_complex,
            CAST(cdk2->>'solvent' AS DOUBLE) AS cdk2_solvent,
            CAST(ptp1b->>'complex' AS DOUBLE) AS ptp1b_complex,
            CAST(ptp1b->>'solvent' AS DOUBLE) AS ptp1b_solvent,
            CAST(tyk2->>'complex' AS DOUBLE) AS tyk2_complex,
            CAST(tyk2->>'solvent' AS DOUBLE) AS tyk2_solvent,
            CAST(mcl1->>'complex' AS DOUBLE) AS mcl1_complex,
            CAST(mcl1->>'solvent' AS DOUBLE) AS mcl1_solvent,
            CAST(thrombin->>'complex' AS DOUBLE) AS thrombin_complex,
            CAST(thrombin->>'solvent' AS DOUBLE) AS thrombin_solvent
        FROM rbfe_outputs_raw
    )
    UNPIVOT (
        metric_value FOR metric_name IN (
            bace_complex,
            bace_solvent,
            p38_complex,
            p38_solvent,
            jnk1_complex,
            jnk1_solvent,
            cdk2_complex,
            cdk2_solvent,
            ptp1b_complex,
            ptp1b_solvent,
            tyk2_complex,
            tyk2_solvent,
            mcl1_complex,
            mcl1_solvent,
            thrombin_complex,
            thrombin_solvent
        )
    );
    """
)

conn.execute(
    """
    COPY (
        SELECT
            'md' AS benchmark_type,
            m.mps_process_count,
            l.run_date,
            l.run_id,
            l.benchmark AS system,
            NULL::VARCHAR AS phase,
            'ns_per_day' AS metric_name,
            l.ns_per_day AS metric_value,
            m.instance_type,
            m.ami
        FROM md_metrics_long l
        JOIN manifests_raw m USING (run_id)
        WHERE isfinite(l.ns_per_day)
    ) TO 'data/md_benchmark_aws_data.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

conn.execute(
    """
    COPY (
        SELECT
            'rbfe' AS benchmark_type,
            m.mps_process_count,
            l.run_date,
            l.run_id,
            l.benchmark AS system,
            l.phase,
            'ns_per_day' AS metric_name,
            l.metric_value,
            m.instance_type,
            m.ami
        FROM rbfe_metrics_long l
        JOIN manifests_raw m USING (run_id)
        WHERE isfinite(l.metric_value)
    ) TO 'data/rbfe_benchmark_aws_data.parquet'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)
