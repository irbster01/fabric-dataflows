-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "529f0d5c-c0bc-4f97-abf5-3fd2babe9910",
-- META       "default_lakehouse_name": "ServicePoint",
-- META       "default_lakehouse_workspace_id": "4241c9c7-5818-4c56-9220-faf632741770",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "529f0d5c-c0bc-4f97-abf5-3fd2babe9910"
-- META         },
-- META         {
-- META           "id": "1b643108-97a2-432c-a0c0-391df26e0eea"
-- META         },
-- META         {
-- META           "id": "74be8ffb-fa88-4278-9a30-358e88a4f13a"
-- META         },
-- META         {
-- META           "id": "c46fe4a6-e106-4e77-993b-efc9ce22e551"
-- META         },
-- META         {
-- META           "id": "0a6e0b32-26e7-4e4e-891d-833423d2e910"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "c747215b-369d-4837-894b-f281326ab180",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "c747215b-369d-4837-894b-f281326ab180",
-- META           "type": "Lakewarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE SCHEMA IF NOT EXISTS silver_lighthouse;

CREATE MATERIALIZED LAKE VIEW silver_lighthouse.service_agg_mlv AS
SELECT
  COALESCE(CAST(client_id AS STRING), '<<NULL>>') AS client_key,
  TO_DATE(DATE_TRUNC('month', CAST(service_date AS DATE))) AS month_start,
  COUNT(*) AS rows_cnt,
  COUNT(DISTINCT service_id) AS distinct_cnt,
  SUM(CASE WHEN service_id IS NULL THEN 1 ELSE 0 END) AS null_service_rows,
  (COUNT(DISTINCT service_id)
    + CASE WHEN SUM(CASE WHEN service_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END
  ) AS computed_count
FROM ServicePoint.dbo.`raw_sp_services`
WHERE CAST(service_date AS DATE) >= CAST('2025-08-30' AS DATE)
GROUP BY
  COALESCE(CAST(client_id AS STRING), '<<NULL>>'),
  TO_DATE(DATE_TRUNC('month', CAST(service_date AS DATE)));


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- quick counts
SELECT COUNT(*) FROM `voa-data-warehouse`.`ServicePoint`.`silver_lighthouse`.`client_month_possible_vs_actual`;

-- sample rows
SELECT client_id, month_start, possible_sessions_in_month, service_count FROM `voa-data-warehouse`.`ServicePoint`.`silver_lighthouse`.`client_month_possible_vs_actual` ORDER BY client_id, month_start LIMIT 50;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- this is Spark SQL in a Lakehouse notebook
CREATE SCHEMA IF NOT EXISTS silver_lighthouse;

-- If you already have a Lakehouse *view* named silver_lighthouse.v3_monthly_client_attend:
CREATE OR REPLACE TABLE silver_lighthouse.v3_monthly_client_attend_tbl AS
SELECT * FROM silver_lighthouse.v3_monthly_client_attend;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Create the target schema if needed
CREATE SCHEMA IF NOT EXISTS silver_lighthouse;

-- Materialize the dataset as a real Delta table
CREATE OR REPLACE TABLE silver_lighthouse.new_monthly_client_attend_table
USING DELTA
AS
SELECT
  o.client_id,
  o.month_start,
  o.possible_sessions_in_month,
  o.service_count,
  o.month_name,
  c.entry_date,
  c.exit_date,
  c.program,
  c.days_enrolled,
  d.client_first_name,
  d.client_last_name,
  r.lighthouse_location_enrolling,
  r.school_name
FROM silver_lighthouse.client_month_possible_vs_actual AS o
LEFT JOIN dbo.`2526_lh_clients`           AS c ON o.client_id = c.client_id
LEFT JOIN dbo.raw_sp_demographics         AS d ON o.client_id = d.client_id
LEFT JOIN dbo.raw_lighthouse_registration AS r ON o.client_id = r.client_id;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

SHOW TABLES IN `voa-data-warehouse`.`ServicePoint`.`silver_lighthouse`;
SHOW TABLES IN `voa-data-warehouse`.`ServicePoint`.`dbo`;





-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- create a persisted Delta table from the SQL endpoint view
CREATE OR REPLACE TABLE `voa-data-warehouse`.`ServicePoint`.`silver_lighthouse`.`new_monthly_client_attend_table` AS
SELECT *
FROM `voa-data-warehouse`.`ServicePoint`.`dbo`.`new_monthly_client_attend`;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- create a real Delta table from the existing view
CREATE OR REPLACE TABLE `voa-data-warehouse`.`ServicePoint`.`silver_lighthouse`.`new_monthly_client_attend_table` AS
SELECT *
FROM `voa-data-warehouse`.`ServicePoint`.`silver_lighthouse`.`new_monthly_client_attend`;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- Run this in a Spark SQL notebook cell attached to workspace "voa-data-warehouse" / lakehouse "ServicePoint"

CREATE SCHEMA IF NOT EXISTS `voa-data-warehouse`.`ServicePoint`.`silver_lighthouse`;

CREATE OR REPLACE TABLE `voa-data-warehouse`.`ServicePoint`.`silver_lighthouse`.`service_agg` AS
SELECT
  COALESCE(CAST(client_id AS STRING), '<<NULL>>') AS client_key,
  TO_DATE(DATE_TRUNC('month', CAST(service_date AS DATE))) AS month_start,
  COUNT(*) AS rows_cnt,
  COUNT(DISTINCT service_id) AS distinct_cnt,
  SUM(CASE WHEN service_id IS NULL THEN 1 ELSE 0 END) AS null_service_rows,
  (COUNT(DISTINCT service_id)
    + CASE WHEN SUM(CASE WHEN service_id IS NULL THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END
  ) AS computed_count
FROM `voa-data-warehouse`.`ServicePoint`.`dbo`.`raw_sp_services`
WHERE CAST(service_date AS DATE) >= DATE('2025-08-30')
GROUP BY
  COALESCE(CAST(client_id AS STRING), '<<NULL>>'),
  TO_DATE(DATE_TRUNC('month', CAST(service_date AS DATE)));


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
