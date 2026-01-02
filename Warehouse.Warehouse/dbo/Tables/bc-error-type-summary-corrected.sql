CREATE TABLE [dbo].[bc-error-type-summary-corrected] (

	[analysis_type] varchar(39) NOT NULL, 
	[error_type] varchar(19) NOT NULL, 
	[record_count] int NULL, 
	[total_balance] float NULL, 
	[avg_balance] float NULL, 
	[percentage] numeric(26,12) NULL, 
	[analysis_date] date NULL
);