# Databricks Airline Medallion Project

This project demonstrates an end-to-end Data Engineering solution on Databricks using a simplified airline booking domain.

## Key Concepts Covered

- Databricks Workspace & Notebooks
- PySpark DataFrame Transformations
- Spark SQL
- Delta Lake
- Medallion Architecture (Bronze, Silver, Gold)
- Data Quality Validation
- Quarantine Tables for Bad Records
- Delta Time Travel & History
- CDC and MERGE Operations
- SCD Type 1 & Type 2
- Structured Streaming
- Incremental Processing
- Workflow Orchestration
- Databricks SQL Dashboards
- Git Integration
- Databricks CLI
- Databricks Asset Bundles
- Table Optimization (OPTIMIZE, VACUUM)

## Architecture

Landing → Bronze → Silver → Gold

## Business Scenario

A simplified airline booking platform where customer, flight, and booking data are ingested, validated, transformed, and aggregated to generate analytics such as route revenue, customer revenue, and state-level revenue.

## Project Structure

data/
notebooks/
sql/
workflows/
datasets/
databricks.yml

## Expected Outcomes

- Build production-style ETL pipelines
- Practice Databricks development using UI and CLI
- Learn Delta Lake features and performance optimization
- Gain hands-on experience with common Data Engineering interview topics
