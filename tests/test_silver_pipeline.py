import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRANSFORMATIONS = PROJECT_ROOT / "src" / "penny_lake_etl" / "transformations"


class SilverPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.categories_sql = (TRANSFORMATIONS / "silver_categories.sql").read_text()
        cls.expenses_sql = (TRANSFORMATIONS / "silver_expenses.sql").read_text()
        cls.pipeline = (
            PROJECT_ROOT / "resources" / "penny_lake_silver.pipeline.yml"
        ).read_text()

    def test_defines_category_view_and_expense_streaming_flow(self):
        self.assertIn("CREATE OR REFRESH MATERIALIZED VIEW silver_categories", self.categories_sql)
        self.assertIn("CREATE OR REFRESH STREAMING TABLE _int_expenses_expectations", self.expenses_sql)
        self.assertIn("CREATE OR REFRESH STREAMING TABLE _int_expenses_enriched", self.expenses_sql)
        self.assertIn("CREATE OR REFRESH STREAMING TABLE silver_expenses", self.expenses_sql)
        self.assertIn("CLUSTER BY (expense_date, spending_area)", self.expenses_sql)
        self.assertIn("'pipelines.autoOptimize.managed' = 'true'", self.expenses_sql)
        self.assertIn("CREATE FLOW silver_expenses_scd1 AS", self.expenses_sql)
        self.assertIn("IDENTIFIER(:bronze_expenses_table)", self.categories_sql)
        self.assertIn("STREAM(IDENTIFIER(:bronze_expenses_table))", self.expenses_sql)
        self.assertIn("silver_categories.sql", self.pipeline)
        self.assertIn("silver_expenses.sql", self.pipeline)
        self.assertIn("bronze_expenses_table:", self.pipeline)
        self.assertNotIn("bronze_exports_table:", self.pipeline)

    def test_categories_are_distinct_from_flattened_bronze_expenses(self):
        self.assertIn("SELECT DISTINCT", self.categories_sql)
        self.assertIn("LOWER(TRIM(primary_category))", self.categories_sql)
        self.assertNotIn("EXPLODE", self.categories_sql)
        self.assertIn("AI_CLASSIFY(", self.categories_sql)
        self.assertIn("AS spending_area", self.categories_sql)
        self.assertIn("AS essentiality", self.categories_sql)

    def test_expenses_join_the_category_view_before_scd1(self):
        self.assertIn("LEFT JOIN silver_categories AS category", self.expenses_sql)
        self.assertIn("LOWER(TRIM(expense.primary_category)) = category.category_key", self.expenses_sql)
        self.assertIn("category.spending_area", self.expenses_sql)
        self.assertIn("category.essentiality", self.expenses_sql)
        self.assertIn("AS normalized_category", self.categories_sql)
        enriched_query = self.expenses_sql.split(
            "CREATE OR REFRESH STREAMING TABLE _int_expenses_enriched", 1
        )[1].split("CREATE OR REFRESH STREAMING TABLE silver_expenses", 1)[0]
        self.assertNotIn("expense.*", enriched_query)
        self.assertNotIn("expense.categories", enriched_query)
        self.assertNotIn("expense.primary_category,", enriched_query)
        self.assertNotIn("ARRAY_SORT(ARRAY_DISTINCT(categories))", self.expenses_sql)

    def test_invalid_expenses_are_dropped_by_expectations(self):
        for name in (
            "valid_expense_id",
            "valid_amount",
            "valid_expense_date",
            "valid_primary_category",
            "supported_currency",
            "valid_exported_at",
            "valid_crc_amount",
        ):
            with self.subTest(expectation=name):
                self.assertIn(f"CONSTRAINT {name} EXPECT (", self.expenses_sql)
        self.assertEqual(self.expenses_sql.count("ON VIOLATION DROP ROW"), 7)
        self.assertIn("original_currency IN ('USD', 'CRC')", self.expenses_sql)

    def test_final_expenses_use_scd1_by_expense_id_and_export_time(self):
        self.assertIn("AUTO CDC INTO silver_expenses", self.expenses_sql)
        self.assertIn("KEYS (expense_id)", self.expenses_sql)
        self.assertIn("SEQUENCE BY exported_at", self.expenses_sql)
        self.assertIn("STORED AS SCD TYPE 1", self.expenses_sql)
        self.assertNotIn("source_file_path", self.expenses_sql)
        self.assertNotIn("source_file_modified_at", self.expenses_sql)
        self.assertNotIn("ingested_at", self.expenses_sql)

    def test_conversion_rate_parameter_is_preserved(self):
        self.assertIn("CAST(:usd_to_crc_rate AS DECIMAL(18, 6))", self.expenses_sql)
        self.assertIn("usd_to_crc_rate:", self.pipeline)
