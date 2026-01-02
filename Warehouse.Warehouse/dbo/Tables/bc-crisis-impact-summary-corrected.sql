CREATE TABLE [dbo].[bc-crisis-impact-summary-corrected] (

	[crisis_description] varchar(51) NOT NULL, 
	[total_config_errors] int NULL, 
	[total_balance_at_risk] float NULL, 
	[avg_balance_per_error] float NULL, 
	[millions_at_risk] float NULL, 
	[errors_per_day_average] numeric(17,6) NULL, 
	[crisis_start_service_date] date NULL, 
	[latest_service_date] date NULL, 
	[undocumented_fixes] int NULL, 
	[pct_undocumented] numeric(26,12) NULL, 
	[analysis_date] date NULL
);