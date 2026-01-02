CREATE TABLE [dbo].[bc-recovery-recommendations-corrected] (

	[analysis_title] varchar(51) NOT NULL, 
	[recommendation_title] varchar(55) NOT NULL, 
	[priority_level] varchar(26) NOT NULL, 
	[detailed_actions] varchar(400) NOT NULL, 
	[financial_impact] varchar(70) NOT NULL, 
	[estimated_timeline] varchar(56) NOT NULL, 
	[responsible_parties] varchar(62) NOT NULL, 
	[analysis_date] date NULL
);