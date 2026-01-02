CREATE TABLE [dbo].[raw_lsndc_entries] (

	[Client ID] bigint NULL, 
	[First Name] varchar(8000) NULL, 
	[Last Name] varchar(8000) NULL, 
	[Entry Date] date NULL, 
	[Entry Exit ID] bigint NULL, 
	[Exit Date] date NULL, 
	[Provider] varchar(8000) NULL
);