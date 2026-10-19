import runpy
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = PROJECT_ROOT / "src" / "apply_governance.py"
JOB = PROJECT_ROOT / "resources" / "expense_refresh.job.yml"


class GovernanceNotebookTests(unittest.TestCase):
    def setUp(self):
        self.parameters = {
            "bronze_catalog": "dev_penny_lake_01_bronze",
            "silver_catalog": "dev_penny_lake_02_silver",
            "gold_catalog": "dev_penny_lake_03_gold",
            "governance_catalog": "dev_penny_lake_04_governance",
            "schema": "expenses",
            "gold_reader_principal": "alkztro@hotmail.com",
            "catalog_full_access_principal": "alfredo.castro@ulatina.net",
        }
        self.runtime = ModuleType("databricks.sdk.runtime")
        self.runtime.dbutils = Mock()
        self.runtime.dbutils.widgets.get.side_effect = self.parameters.__getitem__
        self.runtime.spark = Mock()

    def run_notebook(self):
        with patch.dict("sys.modules", {"databricks.sdk.runtime": self.runtime}):
            runpy.run_path(str(NOTEBOOK))

    def test_masks_amount_from_reader_and_only_grants_protected_gold_table(self):
        self.run_notebook()
        statements = [call.args[0] for call in self.runtime.spark.sql.call_args_list]

        merges = [statement for statement in statements if "MERGE INTO" in statement]
        self.assertEqual(len(merges), 2)
        reader_merge = next(statement for statement in merges if "alkztro@hotmail.com" in statement)
        self.assertIn("FALSE AS can_view_amount", reader_merge)
        owner_merge = next(
            statement for statement in merges if "alfredo.castro@ulatina.net" in statement
        )
        self.assertIn("TRUE AS can_view_amount", owner_merge)

        function = next(statement for statement in statements if "CREATE OR REPLACE FUNCTION" in statement)
        self.assertIn("WHEN EXISTS", function)
        self.assertIn("ELSE CAST(NULL AS DECIMAL(38, 4))", function)

        protected_table = (
            "`dev_penny_lake_03_gold`.`expenses`.`gold_monthly_spending_reader`"
        )
        self.assertTrue(
            any(
                f"INSERT OVERWRITE TABLE {protected_table}" in statement
                and "`dev_penny_lake_03_gold`.`expenses`.`gold_monthly_spending`" in statement
                for statement in statements
            )
        )
        self.assertTrue(
            any("SET MASK `dev_penny_lake_04_governance`.`governance`.`mask_gold_amount`" in statement
                for statement in statements)
        )
        select_grants = [statement for statement in statements if "GRANT SELECT" in statement]
        self.assertEqual(
            select_grants,
            [f"GRANT SELECT ON TABLE {protected_table} TO `alkztro@hotmail.com`"],
        )
        self.assertTrue(any("ALTER MATERIALIZED VIEW" in statement and "SET TAGS" in statement
                            for statement in statements))
        self.assertTrue(any("sensitivity' = 'sensitive'" in statement for statement in statements))

    def test_invalid_principal_fails_before_any_sql_is_executed(self):
        invalid_principals = (
            ("gold_reader_principal", "reader'; GRANT SELECT ON TABLE secret"),
            ("catalog_full_access_principal", "owner'; GRANT SELECT ON TABLE secret"),
        )
        for name, principal in invalid_principals:
            with self.subTest(principal=name):
                self.parameters[name] = principal
                with self.assertRaisesRegex(ValueError, "valid email principal"):
                    self.run_notebook()
                self.runtime.spark.sql.assert_not_called()
                self.runtime.spark.sql.reset_mock()
                self.parameters[name] = (
                    "alkztro@hotmail.com"
                    if name == "gold_reader_principal"
                    else "alfredo.castro@ulatina.net"
                )

    def test_refresh_job_applies_governance_after_gold(self):
        job = JOB.read_text()
        self.assertLess(job.index("task_key: refresh_gold"), job.index("task_key: apply_governance"))
        governance_task = job.split("- task_key: apply_governance", 1)[1]
        self.assertIn("task_key: refresh_gold", governance_task)
        self.assertIn("governance_catalog: ${var.governance_catalog}", governance_task)
        self.assertIn("gold_reader_principal: ${var.gold_reader_principal}", governance_task)
        self.assertIn(
            "catalog_full_access_principal: ${var.catalog_full_access_principal}",
            governance_task,
        )


if __name__ == "__main__":
    unittest.main()
