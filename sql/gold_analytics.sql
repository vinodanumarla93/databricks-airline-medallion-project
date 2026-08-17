-- Gold layer analytics queries for Databricks SQL dashboards.
-- Free Edition defaults: catalog = workspace, schema = airline_medallion.
-- Replace ${catalog} and ${schema} or set them as query parameters.

-- Top routes by revenue
SELECT origin, destination, airline, total_revenue, booking_count
FROM ${catalog}.${schema}.gold_route_revenue
ORDER BY total_revenue DESC
LIMIT 10;

-- Top customers by revenue
SELECT customer_id, first_name, last_name, state, total_revenue, booking_count
FROM ${catalog}.${schema}.gold_customer_revenue
ORDER BY total_revenue DESC
LIMIT 10;

-- Revenue by state
SELECT state, total_revenue, customer_count
FROM ${catalog}.${schema}.gold_state_revenue
ORDER BY total_revenue DESC;

-- Data quality: quarantined record counts
SELECT 'customers' AS entity, count(*) AS quarantined FROM ${catalog}.${schema}.quarantine_customers
UNION ALL
SELECT 'bookings' AS entity, count(*) AS quarantined FROM ${catalog}.${schema}.quarantine_bookings;
