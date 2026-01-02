# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # BUN Bundle Implementation - Fabric SQL Only
# 
# **Purpose**: Transform BUN services billing according to bundle rules
# 
# **Business Rule**: 
# - **4+ PRIMARY BUN services per month = $900 bundle** (first service gets $900, others get $0)
# - **<4 PRIMARY services = all get $0**
# 
# **Critical Filters**:
# - ✅ PRIMARY services only (`Primary Flag = 'Primary'`)
# - ✅ BILLABLE services only (`non_billable = false/empty`)
# - ✅ BUN service types only (`Service Type LIKE 'BUN:%'`)

# MARKDOWN ********************

# ## Step 1: Extract BUN Services (Run in Fabric)
# 
# Get all PRIMARY BILLABLE BUN services from Bronze lakehouse

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) as total_rows
# MAGIC FROM transform_credible_services
# MAGIC WHERE `Service Type` LIKE 'BUN%'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Apply Bundle Rules with Complete SQL Transform
# 
# This does the entire bundle transformation in one SQL query

# CELL ********************

-- Complete BUN Bundle Transformation
-- Run this in Fabric to get corrected billing data

WITH bun_services_filtered AS (
    SELECT 
        [Client ID],
        [Client Name],
        [Service Date], 
        [Service Type],
        [Adjusted Rate],
        [Units],
        [episode_id],
        [Employee ID],
        [emp_name],
        [Primary Flag],
        [non_billable],
        [Status],
        YEAR([Service Date]) as service_year,
        MONTH([Service Date]) as service_month,
        FORMAT([Service Date], 'yyyy-MM') as year_month,
        FORMAT([Service Date], 'MMMM yyyy') as month_name
    FROM Bronze.dbo.transform_credible_services
    WHERE 
        [Service Type] LIKE 'BUN:%'
        AND [Primary Flag] = 'Primary'                 -- PRIMARY ONLY
        AND ([non_billable] IS NULL OR [non_billable] = '' OR [non_billable] = 'No')  -- BILLABLE ONLY
        AND [Status] NOT IN ('Cancelled', 'Test', 'Deleted')
        AND [Service Date] >= '2024-01-01'
),

client_month_counts AS (
    SELECT 
        [Client ID],
        service_year,
        service_month,
        COUNT(*) as primary_service_count
    FROM bun_services_filtered
    GROUP BY [Client ID], service_year, service_month
),

bun_with_bundle_rules AS (
    SELECT 
        b.*,
        c.primary_service_count,
        
        -- Apply bundle rule
        CASE 
            WHEN c.primary_service_count >= 4 THEN
                CASE 
                    WHEN ROW_NUMBER() OVER (PARTITION BY b.[Client ID], b.service_year, b.service_month ORDER BY b.[Service Date]) = 1 
                    THEN 900.00  -- First PRIMARY service gets $900
                    ELSE 0.00    -- Other PRIMARY services get $0
                END
            ELSE 0.00  -- <4 PRIMARY services = all get $0
        END as transformed_rate,
        
        -- Bundle status
        CASE 
            WHEN c.primary_service_count >= 4 THEN 'BUNDLE_APPLIED'
            ELSE 'NO_BUNDLE_INSUFFICIENT_PRIMARY_SERVICES'
        END as bundle_status,
        
        -- Bundle notes
        CASE 
            WHEN c.primary_service_count >= 4 THEN
                CASE 
                    WHEN ROW_NUMBER() OVER (PARTITION BY b.[Client ID], b.service_year, b.service_month ORDER BY b.[Service Date]) = 1 
                    THEN 'PRIMARY BUNDLE SERVICE - $900'
                    ELSE 'PRIMARY SERVICE BUNDLED TO $0'
                END
            ELSE 'NO BUNDLE - ONLY ' + CAST(c.primary_service_count AS VARCHAR) + ' PRIMARY SERVICES'
        END as bundle_note
        
    FROM bun_services_filtered b
    INNER JOIN client_month_counts c 
        ON b.[Client ID] = c.[Client ID] 
        AND b.service_year = c.service_year 
        AND b.service_month = c.service_month
)

SELECT 
    [Client ID],
    [Client Name],
    [Service Date],
    [Service Type],
    [Adjusted Rate] as original_rate,
    transformed_rate,
    (transformed_rate - [Adjusted Rate]) as rate_difference,
    [Units],
    [episode_id],
    [Employee ID],
    [emp_name],
    [Primary Flag],
    [non_billable],
    [Status],
    month_name,
    primary_service_count,
    bundle_status,
    bundle_note

FROM bun_with_bundle_rules
ORDER BY [Client ID], [Service Date];

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Create Silver Lakehouse Table (Run in Fabric)
# 
# Create the target table for corrected BUN billing

# CELL ********************

-- Create Silver Lakehouse table for corrected BUN billing
-- Run this in Fabric to create the target table

CREATE TABLE Silver.dbo.corrected_bun_billing (
    client_id NVARCHAR(50),
    client_name NVARCHAR(255),
    service_date DATE,
    service_type NVARCHAR(255),
    original_rate DECIMAL(10,2),
    transformed_rate DECIMAL(10,2),
    rate_difference DECIMAL(10,2),
    units INT,
    episode_id NVARCHAR(50),
    employee_id NVARCHAR(50),
    emp_name NVARCHAR(255),
    primary_flag NVARCHAR(20),
    non_billable NVARCHAR(10),
    status NVARCHAR(50),
    month_name NVARCHAR(50),
    primary_service_count INT,
    bundle_status NVARCHAR(100),
    bundle_note NVARCHAR(255),
    transformation_date DATETIME2 DEFAULT GETDATE()
);

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Insert Transformed Data into Silver Lakehouse
# 
# Run this to populate the corrected billing table

# CELL ********************

-- Insert transformed BUN data into Silver lakehouse
-- This populates Silver.dbo.corrected_bun_billing with bundle-corrected data

INSERT INTO Silver.dbo.corrected_bun_billing (
    client_id, client_name, service_date, service_type, 
    original_rate, transformed_rate, rate_difference,
    units, episode_id, employee_id, emp_name,
    primary_flag, non_billable, status, month_name,
    primary_service_count, bundle_status, bundle_note
)

-- Copy the entire bundle transformation query from Step 2
WITH bun_services_filtered AS (
    SELECT 
        [Client ID],
        [Client Name],
        [Service Date], 
        [Service Type],
        [Adjusted Rate],
        [Units],
        [episode_id],
        [Employee ID],
        [emp_name],
        [Primary Flag],
        [non_billable],
        [Status],
        YEAR([Service Date]) as service_year,
        MONTH([Service Date]) as service_month,
        FORMAT([Service Date], 'yyyy-MM') as year_month,
        FORMAT([Service Date], 'MMMM yyyy') as month_name
    FROM Bronze.dbo.transform_credible_services
    WHERE 
        [Service Type] LIKE 'BUN:%'
        AND [Primary Flag] = 'Primary'                 -- PRIMARY ONLY
        AND ([non_billable] IS NULL OR [non_billable] = '' OR [non_billable] = 'No')  -- BILLABLE ONLY
        AND [Status] NOT IN ('Cancelled', 'Test', 'Deleted')
        AND [Service Date] >= '2024-01-01'
),

client_month_counts AS (
    SELECT 
        [Client ID],
        service_year,
        service_month,
        COUNT(*) as primary_service_count
    FROM bun_services_filtered
    GROUP BY [Client ID], service_year, service_month
),

bun_with_bundle_rules AS (
    SELECT 
        b.*,
        c.primary_service_count,
        
        -- Apply bundle rule
        CASE 
            WHEN c.primary_service_count >= 4 THEN
                CASE 
                    WHEN ROW_NUMBER() OVER (PARTITION BY b.[Client ID], b.service_year, b.service_month ORDER BY b.[Service Date]) = 1 
                    THEN 900.00  -- First PRIMARY service gets $900
                    ELSE 0.00    -- Other PRIMARY services get $0
                END
            ELSE 0.00  -- <4 PRIMARY services = all get $0
        END as transformed_rate,
        
        -- Bundle status
        CASE 
            WHEN c.primary_service_count >= 4 THEN 'BUNDLE_APPLIED'
            ELSE 'NO_BUNDLE_INSUFFICIENT_PRIMARY_SERVICES'
        END as bundle_status,
        
        -- Bundle notes
        CASE 
            WHEN c.primary_service_count >= 4 THEN
                CASE 
                    WHEN ROW_NUMBER() OVER (PARTITION BY b.[Client ID], b.service_year, b.service_month ORDER BY b.[Service Date]) = 1 
                    THEN 'PRIMARY BUNDLE SERVICE - $900'
                    ELSE 'PRIMARY SERVICE BUNDLED TO $0'
                END
            ELSE 'NO BUNDLE - ONLY ' + CAST(c.primary_service_count AS VARCHAR) + ' PRIMARY SERVICES'
        END as bundle_note
        
    FROM bun_services_filtered b
    INNER JOIN client_month_counts c 
        ON b.[Client ID] = c.[Client ID] 
        AND b.service_year = c.service_year 
        AND b.service_month = c.service_month
)

SELECT 
    [Client ID],
    [Client Name],
    [Service Date],
    [Service Type],
    [Adjusted Rate] as original_rate,
    transformed_rate,
    (transformed_rate - [Adjusted Rate]) as rate_difference,
    [Units],
    [episode_id],
    [Employee ID],
    [emp_name],
    [Primary Flag],
    [non_billable],
    [Status],
    month_name,
    primary_service_count,
    bundle_status,
    bundle_note

FROM bun_with_bundle_rules
ORDER BY [Client ID], [Service Date];

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Bundle Compliance Summary Report
# 
# Run this to get summary statistics on bundle compliance

# CELL ********************

-- BUN Bundle Compliance Summary Report
-- Run this to see overall bundle compliance statistics

SELECT 
    'BUN Bundle Compliance Summary' as report_type,
    COUNT(DISTINCT client_id) as total_clients,
    COUNT(DISTINCT CONCAT(client_id, '_', month_name)) as total_client_months,
    
    -- Bundle statistics
    SUM(CASE WHEN bundle_status = 'BUNDLE_APPLIED' THEN 1 ELSE 0 END) as services_with_bundle,
    SUM(CASE WHEN bundle_status LIKE 'NO_BUNDLE%' THEN 1 ELSE 0 END) as services_without_bundle,
    
    -- Financial impact
    SUM(original_rate) as total_original_billing,
    SUM(transformed_rate) as total_corrected_billing,
    SUM(rate_difference) as total_revenue_impact,
    
    -- Client-month summaries
    COUNT(DISTINCT CASE WHEN bundle_status = 'BUNDLE_APPLIED' THEN CONCAT(client_id, '_', month_name) END) as client_months_with_bundles,
    COUNT(DISTINCT CASE WHEN bundle_status LIKE 'NO_BUNDLE%' THEN CONCAT(client_id, '_', month_name) END) as client_months_without_bundles,
    
    -- Compliance rate
    ROUND(
        COUNT(DISTINCT CASE WHEN bundle_status = 'BUNDLE_APPLIED' THEN CONCAT(client_id, '_', month_name) END) * 100.0 
        / COUNT(DISTINCT CONCAT(client_id, '_', month_name)), 
        2
    ) as bundle_compliance_rate_percent

FROM Silver.dbo.corrected_bun_billing;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Detailed Client Bundle Analysis
# 
# See which clients have bundles vs. which don't

# CELL ********************

-- Detailed client bundle analysis
-- Shows bundle eligibility by client and month

SELECT 
    client_name,
    month_name,
    primary_service_count,
    bundle_status,
    SUM(original_rate) as month_original_total,
    SUM(transformed_rate) as month_corrected_total,
    SUM(rate_difference) as month_revenue_impact,
    
    -- Bundle compliance check
    CASE 
        WHEN primary_service_count >= 4 AND SUM(transformed_rate) = 900 THEN 'COMPLIANT_BUNDLE'
        WHEN primary_service_count >= 4 AND SUM(transformed_rate) != 900 THEN 'NON_COMPLIANT_SHOULD_BE_900'
        WHEN primary_service_count < 4 AND SUM(transformed_rate) = 0 THEN 'COMPLIANT_NO_BUNDLE'
        WHEN primary_service_count < 4 AND SUM(transformed_rate) > 0 THEN 'NON_COMPLIANT_SHOULD_BE_ZERO'
        ELSE 'UNKNOWN'
    END as compliance_status

FROM Silver.dbo.corrected_bun_billing
GROUP BY client_id, client_name, month_name, primary_service_count, bundle_status
ORDER BY client_name, month_name;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7: Missing Bundle Revenue Analysis
# 
# Find clients who should have bundles but don't

# CELL ********************

-- Missing Bundle Revenue Analysis
-- Identifies potential compliance issues and revenue opportunities

WITH client_month_summary AS (
    SELECT 
        client_id,
        client_name,
        month_name,
        primary_service_count,
        SUM(original_rate) as current_billing,
        SUM(transformed_rate) as corrected_billing,
        SUM(rate_difference) as revenue_impact,
        MAX(bundle_status) as bundle_status
    FROM Silver.dbo.corrected_bun_billing
    GROUP BY client_id, client_name, month_name, primary_service_count
)

SELECT 
    client_name,
    month_name,
    primary_service_count,
    current_billing,
    corrected_billing,
    revenue_impact,
    bundle_status,
    
    -- Compliance analysis
    CASE 
        WHEN primary_service_count >= 4 AND current_billing != 900 THEN 'BUNDLE_MISSING_REVENUE_OPPORTUNITY'
        WHEN primary_service_count >= 4 AND current_billing = 900 THEN 'BUNDLE_COMPLIANT'
        WHEN primary_service_count < 4 AND current_billing > 0 THEN 'OVERBILLING_SHOULD_BE_ZERO'
        WHEN primary_service_count < 4 AND current_billing = 0 THEN 'COMPLIANT_NO_BUNDLE'
        ELSE 'REVIEW_NEEDED'
    END as revenue_opportunity_type,
    
    -- Calculate opportunity
    CASE 
        WHEN primary_service_count >= 4 THEN (900 - current_billing)
        WHEN primary_service_count < 4 THEN (0 - current_billing)
        ELSE 0
    END as revenue_opportunity_amount

FROM client_month_summary
WHERE 
    -- Focus on compliance issues
    (primary_service_count >= 4 AND current_billing != 900)  -- Missing bundles
    OR (primary_service_count < 4 AND current_billing > 0)   -- Overbilling
ORDER BY 
    ABS(revenue_opportunity_amount) DESC,  -- Biggest opportunities first
    client_name, 
    month_name;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✅ Implementation Complete!
# 
# **You now have**:
# 1. ✅ SQL to extract PRIMARY BILLABLE BUN services only
# 2. ✅ Complete bundle transformation logic (4+ = $900, <4 = $0)
# 3. ✅ Silver lakehouse table creation
# 4. ✅ Data population with corrected billing
# 5. ✅ Compliance reporting and analysis
# 6. ✅ Revenue opportunity identification
# 
# **Next Steps**:
# 1. Run the SQL queries in your Fabric workspace
# 2. Verify the filtering (PRIMARY + billable only)
# 3. Check the bundle transformations
# 4. Create Power BI dashboard from Silver table
# 5. Set up monthly automated processing
# 
# **Target**: `Silver.dbo.corrected_bun_billing` with all bundle rules applied!
