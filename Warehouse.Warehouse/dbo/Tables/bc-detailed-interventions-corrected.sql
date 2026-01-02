CREATE TABLE [dbo].[bc-detailed-interventions-corrected] (

	[analysis_title] varchar(39) NOT NULL, 
	[service_id] varchar(8000) NULL, 
	[intervention_type] varchar(25) NOT NULL, 
	[cpt_code] varchar(8000) NULL, 
	[program] varchar(8000) NULL, 
	[service_date] date NULL, 
	[revenue_recovered] float NULL, 
	[full_intervention_note] varchar(2000) NULL
);