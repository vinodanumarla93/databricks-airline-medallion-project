# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Clean, Validate, Conform
# MAGIC Applies data quality rules, routes bad records to quarantine tables,
# MAGIC and produces conformed Silver tables.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "airline_medallion")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

def tbl(layer, name):
    return f"{catalog}.{schema}.{layer}_{name}"

# COMMAND ----------

# MAGIC %md ## Customers - dedupe and validate email presence

bronze_customers = spark.table(tbl("bronze", "customers"))
valid_customers = bronze_customers.filter(F.col("email").isNotNull() & (F.trim(F.col("email")) != ""))
bad_customers = bronze_customers.filter(F.col("email").isNull() | (F.trim(F.col("email")) == "")) \
    .withColumn("_dq_reason", F.lit("missing_email"))

valid_customers.dropDuplicates(["customer_id"]).write.mode("overwrite").saveAsTable(tbl("silver", "customers"))
bad_customers.write.mode("overwrite").saveAsTable(tbl("quarantine", "customers"))

# COMMAND ----------

# MAGIC %md ## Flights - straightforward conform

spark.table(tbl("bronze", "flights")).dropDuplicates(["flight_id"]) \
    .write.mode("overwrite").saveAsTable(tbl("silver", "flights"))

# COMMAND ----------

# MAGIC %md ## Bookings - validate seats and referential integrity

bronze_bookings = spark.table(tbl("bronze", "bookings"))
silver_customers = spark.table(tbl("silver", "customers")).select("customer_id").distinct()
known_ids = [r.customer_id for r in silver_customers.collect()]

# Valid = positive seats AND known customer
valid_bookings = bronze_bookings.filter(
    (F.col("seats") > 0) & (F.col("customer_id").isin(known_ids))
)
bad_bookings = bronze_bookings.withColumn(
    "_dq_reason",
    F.when(F.col("seats") <= 0, F.lit("invalid_seats"))
     .when(~F.col("customer_id").isin(known_ids), F.lit("unknown_customer"))
     .otherwise(F.lit(None))
).filter(F.col("_dq_reason").isNotNull())

valid_bookings.write.mode("overwrite").saveAsTable(tbl("silver", "bookings"))
bad_bookings.write.mode("overwrite").saveAsTable(tbl("quarantine", "bookings"))

print(f"Silver bookings: {valid_bookings.count()}, Quarantined: {bad_bookings.count()}")
