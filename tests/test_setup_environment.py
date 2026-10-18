import runpy
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch


NOTEBOOK = Path(__file__).resolve().parents[1] / "src" / "setup_environment.py"


class SetupEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.parameters = {
            "bronze_catalog": "dev_penny_lake_01_bronze",
            "silver_catalog": "dev_penny_lake_02_silver",
            "gold_catalog": "dev_penny_lake_03_gold",
            "governance_catalog": "dev_penny_lake_04_governance",
            "schema": "expenses",
            "landing_schema": "landing",
            "transactions_volume": "transactions_volume",
            "autoloader_state_volume": "autoloader_state",
            "catalog_full_access_principal": "alfredo.castro@ulatina.net",
        }
        self.runtime = ModuleType("databricks.sdk.runtime")
        self.runtime.dbutils = Mock()
        self.runtime.dbutils.widgets.get.side_effect = self.parameters.__getitem__
        self.runtime.spark = Mock()

    def run_notebook(self):
        with patch.dict("sys.modules", {"databricks.sdk.runtime": self.runtime}), patch("builtins.print") as output:
            runpy.run_path(str(NOTEBOOK))
        return output

    def test_creates_expected_objects_for_each_environment(self):
        for environment in ("dev", "test", "prod"):
            with self.subTest(environment=environment):
                self.runtime.spark.reset_mock()
                for layer, number in (
                    ("bronze", "01"),
                    ("silver", "02"),
                    ("gold", "03"),
                    ("governance", "04"),
                ):
                    self.parameters[f"{layer}_catalog"] = f"{environment}_penny_lake_{number}_{layer}"
                output = self.run_notebook()
                expected = []
                for layer in ("bronze", "silver", "gold", "governance"):
                    catalog = self.parameters[f"{layer}_catalog"]
                    expected.extend([
                        f"CREATE CATALOG IF NOT EXISTS `{catalog}`",
                        f"GRANT ALL PRIVILEGES ON CATALOG `{catalog}` TO `alfredo.castro@ulatina.net`",
                        f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{'governance' if layer == 'governance' else 'expenses'}`",
                    ])
                bronze = self.parameters["bronze_catalog"]
                expected.extend([
                    f"CREATE SCHEMA IF NOT EXISTS `{bronze}`.`landing`",
                    f"CREATE VOLUME IF NOT EXISTS `{bronze}`.`landing`.`transactions_volume`",
                    f"CREATE VOLUME IF NOT EXISTS `{bronze}`.`landing`.`autoloader_state`",
                ])
                self.assertEqual([call.args[0] for call in self.runtime.spark.sql.call_args_list], expected)
                output.assert_any_call(
                    f"Upload transaction JSON files to: /Volumes/{bronze}/landing/transactions_volume/"
                )
                output.assert_any_call(
                    f"Auto Loader schema and checkpoint state: /Volumes/{bronze}/landing/autoloader_state/"
                )

    def test_rerun_uses_the_same_non_destructive_statements(self):
        self.run_notebook()
        first_run = list(self.runtime.spark.sql.call_args_list)
        self.runtime.spark.reset_mock()
        self.run_notebook()
        self.assertEqual(self.runtime.spark.sql.call_args_list, first_run)
        self.assertTrue(
            all(
                "IF NOT EXISTS" in call.args[0]
                or call.args[0].startswith("GRANT ALL PRIVILEGES ON CATALOG")
                for call in first_run
            )
        )

    def test_invalid_parameters_fail_before_creating_any_objects(self):
        for name in self.parameters.keys() - {"catalog_full_access_principal"}:
            for value in ("", "invalid.name", "invalid/path", "catalog`; DROP CATALOG x", "a" * 256):
                with self.subTest(name=name, value=value):
                    with patch.dict(self.parameters, {name: value}):
                        with self.assertRaisesRegex(ValueError, name):
                            self.run_notebook()
                    self.runtime.spark.sql.assert_not_called()

    def test_invalid_catalog_access_principal_fails_before_creating_any_objects(self):
        for principal in ("", "reader'; GRANT SELECT ON TABLE secret", "not-an-email"):
            with (
                self.subTest(principal=principal),
                patch.dict(self.parameters, {"catalog_full_access_principal": principal}),
                self.assertRaisesRegex(ValueError, "valid email principal"),
            ):
                self.run_notebook()
            self.runtime.spark.sql.assert_not_called()

    def test_duplicate_catalogs_fail_before_creating_any_objects(self):
        self.parameters["silver_catalog"] = self.parameters["bronze_catalog"].upper()
        with self.assertRaisesRegex(ValueError, "distinct catalogs"):
            self.run_notebook()
        self.runtime.spark.sql.assert_not_called()

    def test_creation_errors_fail_the_job(self):
        self.runtime.spark.sql.side_effect = RuntimeError("PERMISSION_DENIED")
        with self.assertRaisesRegex(RuntimeError, "PERMISSION_DENIED"):
            self.run_notebook()
        self.assertEqual(self.runtime.spark.sql.call_count, 1)


if __name__ == "__main__":
    unittest.main()
