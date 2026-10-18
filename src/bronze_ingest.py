# Databricks notebook source
import re

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import functions as F

# COMMAND ----------

parameters: dict[str, str] = {}
for name in (
    "bronze_catalog",
    "table_schema",
    "landing_schema",
    "transactions_volume",
    "autoloader_state_volume",
):
    dbutils.widgets.text(name, "")
    value = dbutils.widgets.get(name)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", value):
        raise ValueError(f"{name} must be a valid catalog, schema, or Volume identifier.")
    parameters[name] = value

catalog = parameters["bronze_catalog"]
table_schema = parameters["table_schema"]
landing_schema = parameters["landing_schema"]
input_volume = parameters["transactions_volume"]
state_volume = parameters["autoloader_state_volume"]
input_path = f"/Volumes/{catalog}/{landing_schema}/{input_volume}/"
state_root = f"/Volumes/{catalog}/{landing_schema}/{state_volume}/bronze_expenses/"
schema_location = f"{state_root}schema/"
checkpoint_location = f"{state_root}checkpoint/"
export_table = f"`{catalog}`.`{table_schema}`.`bronze_expense_exports`"
expense_table = f"`{catalog}`.`{table_schema}`.`bronze_expenses_raw`"

# COMMAND ----------


def write_delta_batch(frame, table: str, app_id: str, batch_id: int) -> None:
    (
        frame.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .saveAsTable(table)
    )


def ingest_batch(batch_df, batch_id: int) -> None:
    invalid_export = (
        "expenses IS NULL OR try_cast(exportedAt AS TIMESTAMP) IS NULL "
        "OR _rescued_data IS NOT NULL"
    )
    if batch_df.filter(invalid_export).limit(1).count():
        raise ValueError(f"Invalid expense export found in Auto Loader batch {batch_id}.")
    if batch_df.groupBy("exportedAt").count().filter("count > 1").limit(1).count():
        raise ValueError(f"Multiple exports share an exportedAt timestamp in batch {batch_id}.")

    exports = batch_df
    expenses = (
        exports.select(
            F.explode("expenses").alias("expense"),
            "exportedAt",
            "source_file_path",
            "source_file_modified_at",
            "ingested_at",
        )
        .select(
            F.col("expense.id").alias("expense_id"),
            F.col("expense.amount").alias("amount"),
            F.col("expense.categories").alias("categories"),
            F.col("expense.category").alias("primary_category"),
            F.col("expense.currency").alias("currency"),
            F.col("expense.date").alias("expense_date"),
            F.col("expense.description").alias("description"),
            F.col("exportedAt").alias("exported_at"),
            "source_file_path",
            "source_file_modified_at",
            "ingested_at",
        )
    )
    target = f"{catalog}_{table_schema}_bronze"
    write_delta_batch(exports, export_table, f"{target}_exports", batch_id)
    write_delta_batch(expenses, expense_table, f"{target}_expenses", batch_id)


source = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .option("multiLine", "true")
    .option("mode", "FAILFAST")
    .option("pathGlobFilter", "*.json")
    .load(input_path)
    .select(
        "*",
        F.col("_metadata.file_path").alias("source_file_path"),
        F.col("_metadata.file_modification_time").alias("source_file_modified_at"),
        F.current_timestamp().alias("ingested_at"),
    )
)

query = (
    source.writeStream
    .option("checkpointLocation", checkpoint_location)
    .foreachBatch(ingest_batch)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
