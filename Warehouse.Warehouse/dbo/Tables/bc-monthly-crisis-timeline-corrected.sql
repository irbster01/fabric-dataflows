CREATE TABLE [dbo].[bc-monthly-crisis-timeline-corrected] (

	[year] int NULL, 
	[month] int NULL, 
	[month_year] varchar(50) NULL, 
	[config_errors] int NULL, 
	[total_services] int NULL, 
	[error_percentage] float NULL, 
	[valid_charges_total] float NULL, 
	[estimated_lost_revenue] float NULL
);