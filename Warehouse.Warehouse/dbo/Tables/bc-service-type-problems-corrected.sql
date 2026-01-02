CREATE TABLE [dbo].[bc-service-type-problems-corrected] (

	[analysis_title] varchar(47) NOT NULL, 
	[cpt_code] varchar(8000) NULL, 
	[visit_type] varchar(8000) NULL, 
	[program_description] varchar(8000) NULL, 
	[error_count] int NULL, 
	[percentage_of_total_errors] numeric(26,12) NULL, 
	[first_error_date] date NULL, 
	[last_error_date] date NULL, 
	[analysis_date] date NULL
);