CREATE TABLE [analysis].[h2022_analysis] (

	[visit_id] int NULL, 
	[service_id] int NULL, 
	[claim_id] varchar(50) NULL, 
	[client_name] varchar(255) NULL, 
	[service_type] varchar(255) NULL, 
	[status] varchar(50) NULL, 
	[rate] float NULL, 
	[balance] float NULL, 
	[service_date] date NULL, 
	[program] varchar(255) NULL, 
	[employee_name] varchar(255) NULL, 
	[primary_payer_code] varchar(50) NULL, 
	[days_since_service] int NULL, 
	[icn] varchar(50) NULL, 
	[created_date] date NULL, 
	[data_source] varchar(26) NOT NULL
);