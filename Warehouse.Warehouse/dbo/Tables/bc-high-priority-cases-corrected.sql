CREATE TABLE [dbo].[bc-high-priority-cases-corrected] (

	[analysis_title] varchar(39) NOT NULL, 
	[service_id] varchar(8000) NULL, 
	[client_name] varchar(8000) NULL, 
	[cpt_code] varchar(8000) NULL, 
	[program_desc] varchar(8000) NULL, 
	[service_date] date NULL, 
	[age_category] varchar(22) NOT NULL, 
	[notes_preview] varchar(800) NULL, 
	[analysis_date] date NULL
);