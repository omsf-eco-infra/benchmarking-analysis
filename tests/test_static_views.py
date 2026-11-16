"""Checks for the build-time dashboard, including parity with notebook SQL."""

import ast
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import duckdb

from benchmarking_analysis.static_views import (
    METRICS,
    PRICING_FILE,
    build_views,
    export_views,
    load_data,
    mark_winners,
    summarize,
)

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ast.parse((ROOT / "src/benchmarking_analysis/analysis.py").read_text())


def notebook_sql(fragment, **variables):
    """Read an existing SQL f-string without importing or running marimo."""
    for node in ast.walk(NOTEBOOK):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "sql" or not node.args:
            continue
        expression = node.args[0]
        if fragment in ast.unparse(expression):
            return eval(
                compile(ast.Expression(expression), "<notebook SQL>", "eval"),
                {"__builtins__": {}},
                variables,
            )
    raise AssertionError(f"Notebook SQL not found: {fragment}")


class NotebookParityTests(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect()
        self.addCleanup(self.conn.close)
        load_data(self.conn, ROOT / "data")
        self.conn.execute("CREATE VIEW benchmark_costs AS SELECT * FROM md_samples")
        self.conn.execute(
            "CREATE VIEW rbfe_benchmark_costs AS SELECT * FROM rbfe_samples"
        )
        for name in ("benchmark_costs_with_prices", "rbfe_benchmark_costs_with_prices"):
            self.conn.execute(notebook_sql(f"CREATE OR REPLACE VIEW {name}"))

    def test_every_view_matches_original_notebook_queries(self):
        views = build_views(self.conn)
        self.assertEqual(len(views), 22)
        for view in views:
            with self.subTest(path=view["path"]):
                workload, kind, selection = (
                    view["workload"],
                    view["kind"],
                    view["selection"],
                )
                if kind == "mps":
                    variables = {
                        "mps_process_count_dropdown": SimpleNamespace(value=selection),
                        "rbfe_mps_process_count_dropdown": SimpleNamespace(
                            value=selection
                        ),
                    }
                    # The output alias differs from the per-system query's order.
                    fragment = (
                        "AND mps_process_count = {mps_process_count_dropdown.value}"
                        if workload == "md"
                        else "AND mps_process_count = {rbfe_mps_process_count_dropdown.value}"
                    )
                else:
                    variables = {f"{workload}_system_sql": selection.replace("'", "''")}
                    fragment = f"AND system = '{{{workload}_system_sql}}'"
                result = self.conn.execute(notebook_sql(fragment, **variables))
                columns = [item[0] for item in result.description]
                expected = [dict(zip(columns, values)) for values in result.fetchall()]

                def sort_key(row):
                    return (
                        row["system"],
                        row["mps_process_count"],
                        row["instance_type"],
                    )

                actual = [{key: row[key] for key in columns} for row in view["rows"]]
                self.assertEqual(
                    sorted(actual, key=sort_key), sorted(expected, key=sort_key)
                )
                for row in view["rows"]:
                    if workload == "rbfe":
                        self.assertAlmostEqual(
                            row["dollars_per_edge"], 66 / row["ns_per_dollar"]
                        )
                    else:
                        self.assertIsNone(row["dollars_per_edge"])

    def test_export_is_complete_and_deterministic(self):
        with tempfile.TemporaryDirectory() as temp:
            web = Path(temp)
            first = export_views(ROOT / "data", web)
            second = export_views(ROOT / "data", web)
            self.assertEqual(first, second)
            self.assertEqual(first["pricing_file"], PRICING_FILE)
            self.assertEqual(len(first["sources"]), 4)
            saved = json.loads((web / "src/generated/manifest.json").read_text())
            self.assertEqual(first, saved)
            self.assertEqual(sum(len(view["charts"]) for view in saved["views"]), 55)
            for view in saved["views"]:
                metrics = (
                    list(METRICS) if view["workload"] == "rbfe" else list(METRICS)[:2]
                )
                self.assertEqual([chart["metric"] for chart in view["charts"]], metrics)
                self.assertTrue(all(chart["label"] for chart in view["charts"]))
                self.assertTrue(all("src" not in chart for chart in view["charts"]))
            self.assertFalse((web / "public/charts").exists())


class EdgeCaseTests(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect()
        self.addCleanup(self.conn.close)
        self.conn.execute("""
            CREATE TABLE instance_prices(instance_type VARCHAR, price_per_hour DOUBLE);
            INSERT INTO instance_prices VALUES ('g4dn.xlarge', 1), ('g5.xlarge', 1), ('zero', 0);
            CREATE TABLE system_metadata(system VARCHAR, total_system_size BIGINT);
            INSERT INTO system_metadata VALUES ('bace', 49052);
            CREATE TABLE md_samples(benchmark_type VARCHAR, system VARCHAR,
                mps_process_count INTEGER, instance_type VARCHAR, metric_value DOUBLE, phase VARCHAR);
            CREATE TABLE rbfe_samples AS SELECT * FROM md_samples;
            INSERT INTO md_samples VALUES
                ('md', 'bace', 1, 'g4dn.xlarge', 10, NULL),
                ('md', 'bace', 1, 'g4dn.xlarge', 30, NULL),
                ('md', 'bace', 1, 'g4dn.xlarge', 50, NULL),
                ('md', 'bace', 1, 'g4dn.xlarge', NULL, NULL),
                ('md', 'bace', 1, 'g5.xlarge', 30, NULL),
                ('md', 'bace', 1, 'unknown', 0, NULL),
                ('md', 'bace', 1, 'zero', 20, NULL),
                ('md', 'bace', 8, 'g4dn.xlarge', 240, NULL),
                ('rbfe', 'bace', 1, 'g4dn.xlarge', 9999, NULL);
            INSERT INTO rbfe_samples VALUES
                ('rbfe', 'bace', 1, 'g4dn.xlarge', 48, 'complex'),
                ('rbfe', 'bace', 1, 'g4dn.xlarge', 9999, 'solvent'),
                ('rbfe', 'bace', 1, 'unknown', 0, 'complex');
        """)

    def test_medians_null_prices_zero_prices_and_ties(self):
        rows = summarize(self.conn, "md")
        selected = mark_winners(
            [row for row in rows if row["mps_process_count"] == 1], "system"
        )
        by_instance = {row["instance_type"]: row for row in selected}
        self.assertEqual(by_instance["g4dn.xlarge"]["ns_per_day"], 30)
        self.assertEqual(by_instance["g4dn.xlarge"]["ns_per_dollar"], 30 / 24)
        for metric in ("ns_per_day", "ns_per_dollar"):
            self.assertTrue(by_instance["g4dn.xlarge"][f"winner_{metric}"])
            self.assertFalse(by_instance["g5.xlarge"][f"winner_{metric}"])
        for instance in ("unknown", "zero"):
            self.assertIsNone(by_instance[instance]["ns_per_dollar"])
            self.assertFalse(by_instance[instance]["winner_ns_per_dollar"])
        self.assertNotIn("winner_ns_per_day", rows[0])

    def test_rbfe_uses_only_complex_phase_and_handles_undefined_cost(self):
        rows = summarize(self.conn, "rbfe")
        self.assertEqual(rows[0]["ns_per_day"], 48)
        self.assertEqual(rows[0]["dollars_per_edge"], 33)
        self.assertIsNone(rows[1]["dollars_per_edge"])
        winners = mark_winners(rows, "system")
        self.assertTrue(winners[0]["winner_dollars_per_edge"])
        self.assertFalse(winners[1]["winner_dollars_per_edge"])

    def test_scaling_limits_counts_but_comparisons_include_all_counts(self):
        views = build_views(self.conn)
        self.assertTrue(any(view["path"] == "md/mps/8" for view in views))
        scaling = next(view for view in views if view["path"] == "md/systems/bace")
        self.assertEqual({row["mps_process_count"] for row in scaling["rows"]}, {1})

    def test_winner_grouping_does_not_mix_systems(self):
        rows = summarize(self.conn, "rbfe")
        rows.append(dict(rows[0], system="cdk2", ns_per_day=1000))
        marked = mark_winners(rows, "system")
        self.assertEqual(sum(row["winner_ns_per_day"] for row in marked), 2)
        for row in marked:
            self.assertTrue(all(f"winner_{metric}" in row for metric in METRICS))

    def test_unsafe_or_invalid_inputs_fail(self):
        with self.assertRaises(ValueError):
            summarize(self.conn, "bad")
        self.conn.execute("UPDATE md_samples SET system = '../unsafe'")
        with self.assertRaisesRegex(ValueError, "Unsafe system path"):
            summarize(self.conn, "md")

    def test_invalid_metrics_and_mps_counts_fail(self):
        self.conn.execute("DELETE FROM md_samples")
        for count, value in ((0, 30), (1, -1), (1, float("nan")), (1, float("inf"))):
            with self.subTest(count=count, value=value):
                self.conn.execute("DELETE FROM md_samples")
                self.conn.execute(
                    "INSERT INTO md_samples VALUES ('md', 'bace', ?, 'g4dn.xlarge', ?, NULL)",
                    [count, value],
                )
                with self.assertRaises(ValueError):
                    summarize(self.conn, "md")

    def test_empty_workload_fails_instead_of_publishing_empty_pages(self):
        self.conn.execute("DELETE FROM md_samples")
        with self.assertRaisesRegex(ValueError, "No md data"):
            summarize(self.conn, "md")

    def test_duplicate_pricing_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)
            (path / PRICING_FILE).write_text(
                json.dumps(
                    [
                        {"instance_type": "g5.xlarge", "price_per_hour": 1},
                        {"instance_type": "g5.xlarge", "price_per_hour": 2},
                    ]
                )
            )
            with duckdb.connect() as conn:
                with self.assertRaisesRegex(ValueError, "one row per instance"):
                    load_data(conn, path)


if __name__ == "__main__":
    unittest.main()
