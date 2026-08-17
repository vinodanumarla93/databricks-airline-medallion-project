# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingest Landing Data
# MAGIC Reads raw CSV files from the landing zone and writes them as Delta tables
# MAGIC with minimal transformation (schema + ingestion metadata only).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "airline_medallion_dev")
dbutils.widgets.text("landing_path", "/Volumes/main/airline_medallion_dev/landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing_path = dbutils.widgets.get("landing_path")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

def ingest(name: str):
    """Read a CSV from landing and write a bronze Delta table with metadata."""
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{landing_path}/{name}.csv")
        .withColumn("_ingested_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    target = f"{catalog}.{schema}.bronze_{name}"
    df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(target)
    print(f"Wrote {df.count()} rows to {target}")

# COMMAND ----------

for entity in ["customers", "flights", "bookings"]:
    ingest(entity)
