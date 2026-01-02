-- Auto Generated (Do not modify) CA8BAF61BF22E89A922BD7CC302032F7AA877D560301C0CB1E0174DD239C8AEA
-- Create a view for program statistics with proper deduplication
CREATE   VIEW dbo.vw_program_statistics AS
SELECT 
    e.[Program ID] as program_id,
    e.[Program ID] as program_name,
    COUNT(DISTINCT e.[Client ID]) as total_clients,
    COUNT(DISTINCT CASE WHEN c.Status = 'ACTIVE' THEN e.[Client ID] END) as active_clients,
    COUNT(DISTINCT CASE WHEN c.Status != 'ACTIVE' OR c.Status IS NULL THEN e.[Client ID] END) as inactive_clients,
    ROUND(
        CASE 
            WHEN COUNT(DISTINCT e.[Client ID]) > 0 
            THEN (COUNT(DISTINCT CASE WHEN c.Status = 'ACTIVE' THEN e.[Client ID] END) * 100.0) / COUNT(DISTINCT e.[Client ID])
            ELSE 0 
        END, 2
    ) as active_percentage,
    COUNT(*) as total_episodes
FROM [dbo].[raw_credible_episodes] e
LEFT JOIN [dbo].[raw_credible_clients] c ON e.[Client ID] = c.[Client ID]
WHERE e.[Program ID] IS NOT NULL AND e.[Program ID] != ''
GROUP BY e.[Program ID]