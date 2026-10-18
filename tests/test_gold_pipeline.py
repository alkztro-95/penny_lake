import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class GoldPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gold_sql = (
            PROJECT_ROOT
            / "src"
            / "penny_lake_etl"
            / "transformations"
            / "gold_monthly_spending.sql"
        ).read_text()
        cls.gold_pipeline = (
            PROJECT_ROOT / "resources" / "penny_lake_gold.pipeline.yml"
        ).read_text()
        cls.refresh_job = (
            PROJECT_ROOT / "resources" / "expense_refresh.job.yml"
        ).read_text()

    def test_gold_answers_monthly_spending_question(self):
        self.assertIn("CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_spending", self.gold_sql)
        self.assertIn("DATE_TRUNC('MONTH', expense_date)", self.gold_sql)
        self.assertIn("SUM(amount_crc) AS total_amount_crc", self.gold_sql)
        self.assertIn("COUNT(*) AS expense_count", self.gold_sql)
        self.assertIn("GROUP BY", self.gold_sql)
        self.assertNotIn("INVALID_TEST", self.gold_sql)

    def test_gold_enforces_month_area_and_essentiality_expectations(self):
        for name in (
            "valid_spending_month",
            "valid_spending_area",
            "valid_essentiality",
        ):
            with self.subTest(expectation=name):
                self.assertIn(f"CONSTRAINT {name} EXPECT (", self.gold_sql)
        self.assertEqual(self.gold_sql.count("ON VIOLATION FAIL UPDATE"), 3)
        self.assertIn("IDENTIFIER(:silver_expenses_table)", self.gold_sql)

    def test_gold_pipeline_uses_gold_catalog_and_refresh_job_runs_it_after_silver(self):
        self.assertIn("catalog: ${var.gold_catalog}", self.gold_pipeline)
        self.assertIn("silver_expenses_table:", self.gold_pipeline)
        self.assertIn("gold_monthly_spending.sql", self.gold_pipeline)
        self.assertIn("task_key: refresh_gold", self.refresh_job)
        self.assertIn("pipeline_id: ${resources.pipelines.penny_lake_gold.id}", self.refresh_job)
        self.assertLess(
            self.refresh_job.index("task_key: refresh_silver"),
            self.refresh_job.index("task_key: refresh_gold"),
        )


if __name__ == "__main__":
    unittest.main()
