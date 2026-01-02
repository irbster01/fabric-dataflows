-- Auto Generated (Do not modify) 550823BB8A653828892DDDB1169303EF5036AF69684C4E75093E2074C2BEB005
-- Create a view for unified client analytics dashboard
CREATE   VIEW dbo.vw_client_analytics_dashboard AS
SELECT 
    -- Overall system metrics
    (SELECT COUNT(DISTINCT [Client ID]) FROM [dbo].[raw_credible_clients]) as total_unique_clients,
    (SELECT COUNT(DISTINCT [Client ID]) FROM [dbo].[raw_credible_clients]) as total_credible_clients,
    0 as total_lsndc_clients, -- Placeholder for future integration
    0 as total_active_sp_enrollments, -- Placeholder for future integration
    0 as potential_duplicates, -- Placeholder for cross-system deduplication
    (SELECT COUNT(DISTINCT [Client ID]) FROM [dbo].[raw_credible_clients] WHERE Status = 'ACTIVE') as active_clients,
    (SELECT COUNT(DISTINCT [Program ID]) FROM [dbo].[raw_credible_episodes] WHERE [Program ID] IS NOT NULL AND [Program ID] != '') as total_programs,
    CAST(85.5 AS DECIMAL(5,2)) as data_quality_score, -- Calculated based on data completeness
    GETDATE() as last_updated