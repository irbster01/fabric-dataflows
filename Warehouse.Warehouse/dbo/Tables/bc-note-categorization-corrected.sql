CREATE TABLE [dbo].[bc-note-categorization-corrected] (

	[analysis_title] varchar(48) NOT NULL, 
	[note_category] varchar(20) NOT NULL, 
	[unique_services_count] int NULL, 
	[percentage_of_errors] numeric(26,12) NULL, 
	[analysis_date] date NULL
);