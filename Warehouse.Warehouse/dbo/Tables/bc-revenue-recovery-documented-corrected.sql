CREATE TABLE [dbo].[bc-revenue-recovery-documented-corrected] (

	[analysis_title] varchar(53) NOT NULL, 
	[documented_successful_interventions] int NULL, 
	[total_revenue_recovered] float NULL, 
	[avg_recovery_per_intervention] float NULL, 
	[smallest_recovery] float NULL, 
	[largest_recovery] float NULL, 
	[earliest_intervention] date NULL, 
	[latest_intervention] date NULL, 
	[analysis_date] date NULL
);