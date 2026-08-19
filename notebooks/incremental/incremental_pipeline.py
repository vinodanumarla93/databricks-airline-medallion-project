# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental ETL Pipeline (hands-on)
# MAGIC
# MAGIC Goal: learn how a pipeline processes **only new data** each run instead of
# MAGIC reprocessing everything. We use two core Databricks tools:
# MAGIC
# MAGIC - **Auto Loader** (`cloudFiles`) for incremental *ingestion* — it remembers
# MAGIC   which files it has already seen using a checkpoint, so re-runs only pick up
# MAGIC   new files.
# MAGIC - **MERGE** for incremental *transformation* — upsert new/changed rows into
# MAGIC   the Silver table instead of overwriting it.
# MAGIC
# MAGIC Lesson flow: load "Day 1" data, build Bronze + Silver, then simulate "Day 2"
# MAGIC files arriving and re-run to watch only the new rows flow through.

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "airline_medallion")
dbutils.widgets.text("volume", "landing")
# Path to this repo's incremental datasets inside the workspace Git folder.
dbutils.widgets.text(
    "repo_datasets",
    "/Workspace/Users/vinodanumarla93@gmail.com/databricks-airline-medallion-project/datasets/airline-databricks-datasets",
)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
repo_datasets = dbutils.widgets.get("repo_datasets")

# Auto Loader watches this folder. New files dropped here = new data.
inc_landing = f"/Volumes/{catalog}/{schema}/{volume}/bookings_incremental"
# Checkpoint stores which files Auto Loader has already processed.
checkpoint = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/bookings_bronze"

def tbl(layer, name):
    return f"{catalog}.{schema}.{layer}_{name}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
dbutils.fs.mkdirs(inc_landing)
print("Incremental landing folder:", inc_landing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simulate "Day 1" data landing
# MAGIC In real projects, files land in cloud storage from an upstream system. Here
# MAGIC we copy Day 1's file into the watched folder to simulate that arrival.

# COMMAND ----------

dbutils.fs.cp(f"{repo_datasets}/bookings_day1.csv", f"{inc_landing}/bookings_day1.csv")
print("Files currently in landing:")
for f_ in dbutils.fs.ls(inc_landing):
    print(" -", f_.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze: incremental ingestion with Auto Loader
# MAGIC `cloudFiles` reads only files it hasn't seen before. We run it in batch mode
# MAGIC with `.trigger(availableNow=True)` — process everything available right now,
# MAGIC then stop (as opposed to a always-on stream). Re-running later picks up only
# MAGIC newly added files, thanks to the checkpoint.

# COMMAND ----------

bookings_schema = "booking_id STRING, customer_id STRING, flight_id STRING, amount DOUBLE, booking_date DATE"

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(bookings_schema)
    .load(inc_landing)
    .withColumn("_ingested_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    bronze_stream.writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(tbl("bronze", "bookings_inc"))
)

# Wait for the batch to finish before moving on.
for q in spark.streams.active:
    q.awaitTermination()

print("Bronze row count:", spark.table(tbl("bronze", "bookings_inc")).count())
display(spark.table(tbl("bronze", "bookings_inc")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: incremental upsert with MERGE
# MAGIC Bronze is append-only history. Silver should be the clean, deduplicated
# MAGIC current state keyed by `booking_id`. We MERGE Bronze into Silver:
# MAGIC update rows that changed, insert rows that are new. Re-running is safe
# MAGIC (idempotent) — the same row won't be duplicated.

# COMMAND ----------

# Create the Silver table once (empty) with the right schema.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tbl("silver", "bookings_inc")} (
    booking_id STRING,
    customer_id STRING,
    flight_id STRING,
    amount DOUBLE,
    booking_date DATE,
    _ingested_ts TIMESTAMP
)
""")

# COMMAND ----------

# Build the latest snapshot per booking_id from Bronze (guard against dupes by
# keeping the most recently ingested version of each key).
from pyspark.sql.window import Window

w = Window.partitionBy("booking_id").orderBy(F.col("_ingested_ts").desc())
bronze_latest = (
    spark.table(tbl("bronze", "bookings_inc"))
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn", "_source_file")
)
bronze_latest.createOrReplaceTempView("bookings_updates")

# COMMAND ----------

spark.sql(f"""
MERGE INTO {tbl("silver", "bookings_inc")} AS target
USING bookings_updates AS source
ON target.booking_id = source.booking_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Silver row count:", spark.table(tbl("silver", "bookings_inc")).count())
display(spark.table(tbl("silver", "bookings_inc")).orderBy("booking_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Simulate "Day 2" — new files arrive
# MAGIC Now drop the two streaming batches into the same landing folder. These are
# MAGIC brand-new bookings that didn't exist on Day 1.

# COMMAND ----------

dbutils.fs.cp(f"{repo_datasets}/streaming_day1.csv", f"{inc_landing}/streaming_day1.csv")
dbutils.fs.cp(f"{repo_datasets}/streaming_day2.csv", f"{inc_landing}/streaming_day2.csv")
print("Files in landing now:")
for f_ in dbutils.fs.ls(inc_landing):
    print(" -", f_.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Re-run Bronze ingestion — only NEW files are read
# MAGIC This is the whole point of incremental processing. Auto Loader checks its
# MAGIC checkpoint, skips `bookings_day1.csv` (already processed), and ingests only
# MAGIC the two new streaming files. Watch the row count grow by exactly 4.

# COMMAND ----------

bronze_stream_2 = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(bookings_schema)
    .load(inc_landing)
    .withColumn("_ingested_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    bronze_stream_2.writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(tbl("bronze", "bookings_inc"))
)

for q in spark.streams.active:
    q.awaitTermination()

print("Bronze row count after Day 2:", spark.table(tbl("bronze", "bookings_inc")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Re-run the Silver MERGE — new rows upserted
# MAGIC Same MERGE as before. The 4 new bookings get inserted; existing rows are
# MAGIC untouched. Because MERGE is idempotent, re-running never creates duplicates.

# COMMAND ----------

w2 = Window.partitionBy("booking_id").orderBy(F.col("_ingested_ts").desc())
bronze_latest_2 = (
    spark.table(tbl("bronze", "bookings_inc"))
    .withColumn("_rn", F.row_number().over(w2))
    .filter("_rn = 1")
    .drop("_rn", "_source_file")
)
bronze_latest_2.createOrReplaceTempView("bookings_updates")

spark.sql(f"""
MERGE INTO {tbl("silver", "bookings_inc")} AS target
USING bookings_updates AS source
ON target.booking_id = source.booking_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Silver row count after Day 2:", spark.table(tbl("silver", "bookings_inc")).count())
display(spark.table(tbl("silver", "bookings_inc")).orderBy("booking_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inspect Delta history (proof of incremental writes)
# MAGIC Delta records every write as a versioned commit. You'll see separate
# MAGIC versions for the Day 1 and Day 2 loads — evidence each run appended
# MAGIC incrementally rather than rewriting everything.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {tbl('bronze', 'bookings_inc')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. (Optional) Reset to start over
# MAGIC Run this to wipe tables + checkpoint so you can redo the lesson from scratch.

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {tbl('bronze', 'bookings_inc')}")
# spark.sql(f"DROP TABLE IF EXISTS {tbl('silver', 'bookings_inc')}")
# dbutils.fs.rm(inc_landing, recurse=True)
# dbutils.fs.rm(checkpoint, recurse=True)
# print("Reset complete. Re-run from section 2.")
