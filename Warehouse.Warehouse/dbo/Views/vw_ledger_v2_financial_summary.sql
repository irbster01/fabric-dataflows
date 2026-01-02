-- Auto Generated (Do not modify) 3F09912E6B2854830950D474F80A38C80CE8FC29F49438D982F6FB02562D1D81
-- Create the financial summary view
CREATE VIEW [dbo].[vw_ledger_v2_financial_summary] AS
SELECT 
    COUNT(*) AS total_services,
    COUNT(DISTINCT service_id) AS unique_services,
    COUNT(DISTINCT client_name) AS unique_clients,
    MIN(service_date) AS earliest_service_date,
    MAX(service_date) AS latest_service_date,
    SUM(rate) AS total_revenue,
    AVG(rate) AS average_rate,
    SUM(balance) AS total_outstanding_balance,
    COUNT(CASE WHEN status = 'PAID' THEN 1 END) AS paid_services,
    COUNT(CASE WHEN status != 'PAID' THEN 1 END) AS unpaid_services,
    ROUND(COUNT(CASE WHEN status = 'PAID' THEN 1 END) * 100.0 / COUNT(*), 2) AS payment_rate_percent
FROM [dbo].[cleaned-credible-ledger-v2];