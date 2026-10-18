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
    "landing_schema",
    "transactions_volume",
    "autoloader_state_volume",
):
    dbutils.widgets.text(name, "")
    value = dbutils.widgets.get(name)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,254}", value):
        raise ValueError(
            f"{name} must be a non-empty identifier of at most 255 characters "
            "using letters, digits, and underscores, starting with a letter or underscore."
        )
    parameters[name] = value

dbutils.widgets.text("catalog_full_access_principal", "")
catalog_full_access_principal = dbutils.widgets.get("catalog_full_access_principal")
if not re.fullmatch(
    r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    catalog_full_access_principal,
):
    raise ValueError("catalog_full_access_principal must be a valid email principal.")

catalogs = [
    parameters[name]
    for name in ("bronze_catalog", "silver_catalog", "gold_catalog", "governance_catalog")
]
if len({catalog.lower() for catalog in catalogs}) != len(catalogs):
    raise ValueError("Bronze, silver, gold, and governance must use distinct catalogs.")

# COMMAND ----------

schema = parameters["schema"]
for catalog in catalogs:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(
        f"GRANT ALL PRIVILEGES ON CATALOG `{catalog}` "
        f"TO `{catalog_full_access_principal}`"
    )
    catalog_schema = "governance" if catalog == parameters["governance_catalog"] else schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{catalog_schema}`")

bronze_catalog = parameters["bronze_catalog"]
landing_schema = parameters["landing_schema"]
volume = parameters["transactions_volume"]
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{bronze_catalog}`.`{landing_schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{bronze_catalog}`.`{landing_schema}`.`{volume}`")
state_volume = parameters["autoloader_state_volume"]
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{bronze_catalog}`.`{landing_schema}`.`{state_volume}`")

# COMMAND ----------

print("Environment setup completed.")
print(f"Full catalog access granted to: {catalog_full_access_principal}")
for catalog in catalogs:
    catalog_schema = "governance" if catalog == parameters["governance_catalog"] else schema
    print(f"Tables schema: {catalog}.{catalog_schema}")
print(f"Upload transaction JSON files to: /Volumes/{bronze_catalog}/{landing_schema}/{volume}/")
print(f"Auto Loader schema and checkpoint state: /Volumes/{bronze_catalog}/{landing_schema}/{state_volume}/")
