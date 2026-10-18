import runpy
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, Mock, call, patch


NOTEBOOK = Path(__file__).resolve().parents[1] / "src" / "bronze_ingest.py"


class BronzeIngestTests(unittest.TestCase):
    def setUp(self):
        self.parameters = {
            "bronze_catalog": "dev_penny_lake_01_bronze",
            "table_schema": "expenses",
            "landing_schema": "landing",
            "transactions_volume": "transactions_volume",
            "autoloader_state_volume": "autoloader_state",
        }
        self.spark = Mock()
        self.reader = MagicMock()
        self.spark.readStream = self.reader
        self.reader.format.return_value = self.reader
        self.reader.option.return_value = self.reader
        self.source = MagicMock()
        self.reader.load.return_value = self.source
        self.source.select.return_value = self.source
        self.write_stream = MagicMock()
        self.write_stream.option.return_value = self.write_stream
        self.write_stream.foreachBatch.return_value = self.write_stream
        self.write_stream.trigger.return_value = self.write_stream
        self.source.writeStream = self.write_stream
        self.query = MagicMock()
        self.write_stream.start.return_value = self.query

        runtime = ModuleType("databricks.sdk.runtime")
        runtime.dbutils = Mock()
        runtime.dbutils.widgets.get.side_effect = self.parameters.__getitem__
        runtime.spark = self.spark
        pyspark_sql = ModuleType("pyspark.sql")
        pyspark_sql.functions = MagicMock()
        with patch.dict("sys.modules", {
            "databricks.sdk.runtime": runtime,
            "pyspark.sql": pyspark_sql,
        }):
            self.notebook = runpy.run_path(str(NOTEBOOK))

    def test_uses_volume_for_schema_and_checkpoint_state(self):
        self.reader.option.assert_any_call(
            "cloudFiles.schemaLocation",
            "/Volumes/dev_penny_lake_01_bronze/landing/autoloader_state/bronze_expenses/schema/",
        )
        self.write_stream.option.assert_called_once_with(
            "checkpointLocation",
            "/Volumes/dev_penny_lake_01_bronze/landing/autoloader_state/bronze_expenses/checkpoint/",
        )
        self.reader.load.assert_called_once_with(
            "/Volumes/dev_penny_lake_01_bronze/landing/transactions_volume/"
        )
        self.write_stream.trigger.assert_called_once_with(availableNow=True)
        self.query.awaitTermination.assert_called_once_with()

    def test_appends_each_batch_once_to_both_delta_tables(self):
        batch = MagicMock()
        batch.filter.return_value.limit.return_value.count.return_value = 0
        batch.groupBy.return_value.count.return_value.filter.return_value.limit.return_value.count.return_value = 0
        flat = batch.select.return_value.select.return_value
        writer = Mock()
        self.notebook["ingest_batch"].__globals__["write_delta_batch"] = writer
        self.notebook["ingest_batch"](batch, 7)

        writer.assert_has_calls([
            call(batch, "`dev_penny_lake_01_bronze`.`expenses`.`bronze_expense_exports`",
                 "dev_penny_lake_01_bronze_expenses_bronze_exports", 7),
            call(flat, "`dev_penny_lake_01_bronze`.`expenses`.`bronze_expenses_raw`",
                 "dev_penny_lake_01_bronze_expenses_bronze_expenses", 7),
        ])

    def test_delta_writes_use_idempotent_batch_transactions(self):
        frame = MagicMock()
        self.notebook["write_delta_batch"](frame, "`bronze`.`expenses`.`table`", "ingest-app", 7)
        writer = frame.write.format.return_value.mode.return_value
        frame.write.format.assert_called_once_with("delta")
        frame.write.format.return_value.mode.assert_called_once_with("append")
        writer.option.assert_called_once_with("mergeSchema", "true")
        writer.option.return_value.option.assert_called_once_with("txnAppId", "ingest-app")
        writer.option.return_value.option.return_value.option.assert_called_once_with("txnVersion", 7)
        writer.option.return_value.option.return_value.option.return_value.saveAsTable.assert_called_once_with(
            "`bronze`.`expenses`.`table`"
        )

    def test_invalid_export_fails_before_writing(self):
        batch = MagicMock()
        batch.filter.return_value.limit.return_value.count.return_value = 1
        with self.assertRaisesRegex(ValueError, "Invalid expense export"):
            self.notebook["ingest_batch"](batch, 2)
        batch.write.format.assert_not_called()

    def test_duplicate_snapshot_versions_fail_before_writing(self):
        batch = MagicMock()
        batch.filter.return_value.limit.return_value.count.return_value = 0
        batch.groupBy.return_value.count.return_value.filter.return_value.limit.return_value.count.return_value = 1
        with self.assertRaisesRegex(ValueError, "share an exportedAt timestamp"):
            self.notebook["ingest_batch"](batch, 2)
        batch.write.format.assert_not_called()


if __name__ == "__main__":
    unittest.main()
