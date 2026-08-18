# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold Layer - Business Aggregates
# MAGIC Produces analytics tables: route revenue, customer revenue, state revenue.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "airline_medallion")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

def tbl(layer, name):
    return f"{catalog}.{schema}.{layer}_{name}"

bookings = spark.table(tbl("silver", "bookings"))
flights = spark.table(tbl("silver", "flights"))
customers = spark.table(tbl("silver", "customers"))

# COMMAND ----------

# MAGIC %md ## Route revenue

# COMMAND ----------

route_revenue = (
    bookings.join(flights, "flight_id")
    .groupBy("origin", "destination", "airline")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.sum("seats").alias("total_seats"),
        F.count("*").alias("booking_count"),
    )
)
route_revenue.write.mode("overwrite").saveAsTable(tbl("gold", "route_revenue"))

# COMMAND ----------

# MAGIC %md ## Customer revenue

# COMMAND ----------

customer_revenue = (
    bookings.groupBy("customer_id")
    .agg(F.sum("amount").alias("total_revenue"), F.count("*").alias("booking_count"))
    .join(customers.select("customer_id", "first_name", "last_name", "state"), "customer_id")
)
customer_revenue.write.mode("overwrite").saveAsTable(tbl("gold", "customer_revenue"))

# COMMAND ----------

# MAGIC %md ## State revenue

# COMMAND ----------

state_revenue = (
    customer_revenue.groupBy("state")
    .agg(
        F.sum("total_revenue").alias("total_revenue"),
        F.countDistinct("customer_id").alias("customer_count"),
    )
    .orderBy(F.desc("total_revenue"))
)
state_revenue.write.mode("overwrite").saveAsTable(tbl("gold", "state_revenue"))