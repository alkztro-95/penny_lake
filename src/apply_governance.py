# Databricks notebook source
import re

from databricks.sdk.runtime import dbutils, spark

# COMMAND ----------

parameters: dict[str, str] = {}
for name in (
    "bronze_catalog",
    "silver_catalog",
    "gold_catalog",
    "governance_catalog",
    "schema",
):
    dbutils.widgets.text(name, "")
    value = dbutils.widgets.get(name)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", value):
        raise ValueError(
            f"{name} must be a non-empty identifier of at most 255 characters "
            "using letters, digits, and underscores, starting with a letter or underscore."
        )
    parameters[name] = value

dbutils.widgets.text("gold_reader_principal", "")
reader = dbutils.widgets.get("gold_reader_principal")
if not re.fullmatch(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", reader):
    raise ValueError("gold_reader_principal must be a valid email principal.")

bronze_catalog = parameters["bronze_catalog"]
silver_catalog = parameters["silver_catalog"]
gold_catalog = parameters["gold_catalog"]
governance_catalog = parameters["governance_catalog"]
schema = parameters["schema"]

bronze_table = f"`{bronze_catalog}`.`{schema}`.`bronze_expenses_raw`"
silver_table = f"`{silver_catalog}`.`{schema}`.`silver_expenses`"
gold_view = f"`{gold_catalog}`.`{schema}`.`gold_monthly_spending`"
gold_reader_table = f"`{gold_catalog}`.`{schema}`.`gold_monthly_spending_reader`"
mapping_table = f"`{governance_catalog}`.`governance`.`gold_amount_access_map`"
mask_function = f"`{governance_catalog}`.`governance`.`mask_gold_amount`"

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {mapping_table} ("
          "principal STRING NOT NULL, "
          "can_view_amount BOOLEAN NOT NULL"
          ") USING DELTA")
spark.sql(
    f"""
    MERGE INTO {mapping_table} AS target
    USING (
      SELECT LOWER('{reader}') AS principal, FALSE AS can_view_amount
    ) AS source
    ON LOWER(target.principal) = source.principal
    WHEN MATCHED THEN UPDATE SET can_view_amount = source.can_view_amount
    WHEN NOT MATCHED THEN INSERT (principal, can_view_amount)
      VALUES (source.principal, source.can_view_amount)
    """
)
spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION {mask_function}(amount DECIMAL(38, 4))
    RETURNS DECIMAL(38, 4)
    RETURN CASE
      WHEN EXISTS (
        SELECT 1
        FROM {mapping_table}
        WHERE LOWER(principal) = LOWER(CURRENT_USER())
          AND can_view_amount
      ) THEN amount
      ELSE CAST(NULL AS DECIMAL(38, 4))
    END
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {gold_reader_table} (
      spending_month DATE,
      spending_area STRING,
      essentiality STRING,
      expense_count BIGINT,
      total_amount_crc DECIMAL(38, 4)
    ) USING DELTA
    """
)
spark.sql(
    f"""
    INSERT OVERWRITE TABLE {gold_reader_table}
    SELECT spending_month, spending_area, essentiality, expense_count, total_amount_crc
    FROM {gold_view}
    """
)
spark.sql(
    f"ALTER TABLE {gold_reader_table} "
    f"ALTER COLUMN total_amount_crc SET MASK {mask_function}"
)

# COMMAND ----------

for table, column in (
    (bronze_table, "amount"),
    (silver_table, "original_amount"),
    (silver_table, "amount_crc"),
    (silver_table, "amount_usd_original"),
):
    spark.sql(
        f"ALTER TABLE {table} ALTER COLUMN {column} "
        "SET TAGS ('sensitivity' = 'sensitive', 'domain' = 'financial')"
    )

spark.sql(
    f"ALTER MATERIALIZED VIEW {gold_view} ALTER COLUMN total_amount_crc "
    "SET TAGS ('sensitivity' = 'sensitive', 'domain' = 'financial')"
)
spark.sql(
    f"ALTER TABLE {gold_reader_table} ALTER COLUMN total_amount_crc "
    "SET TAGS ('sensitivity' = 'sensitive', 'domain' = 'financial')"
)

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG `{gold_catalog}` TO `{reader}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{gold_catalog}`.`{schema}` TO `{reader}`")
spark.sql(f"GRANT SELECT ON TABLE {gold_reader_table} TO `{reader}`")

print(f"Governance applied. Gold reader access is limited to {gold_reader_table}.")
