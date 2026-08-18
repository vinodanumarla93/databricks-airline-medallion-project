# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze Layer - Ingest Landing Data
# MAGIC Reads raw CSV files from the landing zone and writes them as Delta tables
# MAGIC with minimal transformation (schema + ingestion metadata only).

# COMMAND ----------

from pyspark.sql import functions as F

# Free Edition defaults: catalog is "workspace"; schema/volume are created below.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "airline_medallion")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
landing_path = f"/Volumes/{catalog}/{schema}/{volume}"

# On Free Edition you cannot create catalogs, but you can create schemas and
# volumes inside the built-in "workspace" catalog.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
print(f"Upload your CSVs to: {landing_path}")

# COMMAND ----------

# DBTITLE 1,Copy files to volume
# Copy CSV files from workspace datasets to landing volume
source_path = "/Workspace/Users/vinodanumarla93@gmail.com/databricks-airline-medallion-project/datasets"

for file in ["customers.csv", "flights.csv", "bookings.csv"]:
    source = f"{source_path}/{file}"
    destination = f"{landing_path}/{file}"
    dbutils.fs.cp(source, destination, recurse=False)
    print(f"Copied {file} to {landing_path}")

# COMMAND ----------

def ingest(name: str):
    """Read a CSV from landing and write a bronze Delta table with metadata."""
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{landing_path}/{name}.csv")
        .withColumn("_ingested_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    target = f"{catalog}.{schema}.bronze_{name}"
    df.write.mode("overwrite").option("mergeSchema", True).saveAsTable(target)
    print(f"Wrote {df.count()} rows to {target}")

# COMMAND ----------

for entity in ["customers", "flights", "bookings"]:
    ingest(entity)