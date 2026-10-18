# Databricks notebook source
import re

from databricks.sdk.runtime import dbutils, spark

# COMMAND ----------

dbutils.widgets.text("bronze_catalog", "dev_penny_lake_01_bronze")
dbutils.widgets.text("silver_catalog", "dev_penny_lake_02_silver")
dbutils.widgets.text("schema", "expenses")

bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")
schema = dbutils.widgets.get("schema")
for name, value in (
    ("bronze_catalog", bronze_catalog),
    ("silver_catalog", silver_catalog),
    ("schema", schema),
):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", value):
        raise ValueError(f"{name} must be a valid catalog or schema identifier.")

bronze_expenses = f"`{bronze_catalog}`.`{schema}`.`bronze_expenses_raw`"
silver_expenses = f"`{silver_catalog}`.`{schema}`.`silver_expenses`"

# COMMAND ----------

# Metrics before Silver: source-row counts and required-field nulls by input file.
display(spark.sql(f"""
    SELECT
        source_file_path,
        COUNT(*) AS bronze_rows,
        COUNT(DISTINCT expense_id) AS distinct_expense_ids,
        COUNT_IF(
            expense_id IS NULL OR TRIM(CAST(expense_id AS STRING)) = ''
            OR TRY_CAST(amount AS DECIMAL(18, 4)) IS NULL
            OR TRY_CAST(expense_date AS DATE) IS NULL
            OR currency IS NULL OR UPPER(TRIM(currency)) NOT IN ('USD', 'CRC')
        ) AS rows_missing_required_fields
    FROM {bronze_expenses}
    GROUP BY source_file_path
    ORDER BY source_file_path
"""))  # noqa: F821

# COMMAND ----------

# Metrics after Silver: one latest accepted row per expense_id (SCD Type 1).
display(spark.sql(f"""
    SELECT
        COUNT(*) AS silver_rows,
        COUNT(DISTINCT expense_id) AS distinct_expense_ids,
        COUNT_IF(
            expense_id IS NULL OR original_amount IS NULL
            OR expense_date IS NULL OR original_currency IS NULL
            OR original_currency NOT IN ('USD', 'CRC')
        ) AS rows_missing_required_fields,
        SUM(amount_crc) AS total_estimated_amount_crc
    FROM {silver_expenses}
"""))  # noqa: F821
