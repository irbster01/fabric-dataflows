[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Risk Management_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Risk Management" = let
  Source = SharePoint.Tables("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/", [ApiVersion = 15]),
  #"Navigation 1" = Source{[Id = "9212e592-f021-4e8b-9dd8-9657661d8d64"]}[Items],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 1", {"ComplianceAssetId", "OData__ColorTag", "Title", "ContentTypeId", "ServerRedirectedEmbedUrl", "ServerRedirectedEmbedUri", "Id", "FileSystemObjectType", "GUID", "FirstUniqueAncestorSecurableObject", "RoleAssignments", "AttachmentFiles", "ContentType", "GetDlpPolicyTip", "FieldValuesAsHtml", "FieldValuesAsText", "FieldValuesForEdit", "File", "Folder", "LikedByInformation", "ParentList", "Properties", "Versions", "Author", "Editor", "Attachments", "OData__UIVersionString", "EditorId", "AuthorId", "Created", "ID"}),
  #"Reordered columns" = Table.ReorderColumns(#"Removed columns", {"Program", "Division", "FinancialPerformance", "OData_2.AdequateCashflow", "OData_3.AnnualOutcomes", "OData_4.6-monthOutco", "OData_5.AnnualClientSurvey", "OData_6.ClientSurveyResult", "OData_7.CompleteSafetyDril", "OData_8.IncidentReporting", "OData_9.M365UserMaintenanc", "OData_10.SystemAdminUpdate", "OData_11.ProgramPolicyInfo", "OData_12.Retention", "OData_13.RequiredTraining", "OData_14.TrianingActivity", "OData_15.ClientBase", "Modified"}),
  #"Replaced value" = Table.ReplaceValue(#"Reordered columns", "Yes", 5, Replacer.ReplaceValue, {"FinancialPerformance"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "No", 0, Replacer.ReplaceValue, {"OData_2.AdequateCashflow"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", "Yes", 5, Replacer.ReplaceValue, {"OData_3.AnnualOutcomes"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "No", 0, Replacer.ReplaceValue, {"OData_4.6-monthOutco"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", "Yes", 3, Replacer.ReplaceValue, {"OData_5.AnnualClientSurvey"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 4", "No", 0, Replacer.ReplaceValue, {"OData_6.ClientSurveyResult"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", "Yes", 5, Replacer.ReplaceValue, {"OData_7.CompleteSafetyDril"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", "No", 0, Replacer.ReplaceValue, {"OData_8.IncidentReporting"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", "Yes", 4, Replacer.ReplaceValue, {"OData_8.IncidentReporting"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", "Yes", 3, Replacer.ReplaceValue, {"OData_9.M365UserMaintenanc"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "No", 0, Replacer.ReplaceValue, {"OData_9.M365UserMaintenanc"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "No", 0, Replacer.ReplaceValue, {"OData_10.SystemAdminUpdate"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "Yes", 3, Replacer.ReplaceValue, {"OData_11.ProgramPolicyInfo"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "No", 0, Replacer.ReplaceValue, {"OData_11.ProgramPolicyInfo"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 14", "No", 0, Replacer.ReplaceValue, {"OData_6.ClientSurveyResult"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 5", "No", 0, Replacer.ReplaceValue, {"OData_12.Retention"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", "Yes", 4, Replacer.ReplaceValue, {"OData_12.Retention"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", "Yes", 5, Replacer.ReplaceValue, {"OData_13.RequiredTraining"}),
  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 17", "No", 0, Replacer.ReplaceValue, {"OData_13.RequiredTraining"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 18", "No", 0, Replacer.ReplaceValue, {"OData_14.TrianingActivity"}),
  #"Replaced value 20" = Table.ReplaceValue(#"Replaced value 19", "Yes", 3, Replacer.ReplaceValue, {"OData_14.TrianingActivity"}),
  #"Replaced value 21" = Table.ReplaceValue(#"Replaced value 20", "Yes", 5, Replacer.ReplaceValue, {"OData_15.ClientBase"}),
  #"Replaced value 22" = Table.ReplaceValue(#"Replaced value 21", "No", 0, Replacer.ReplaceValue, {"OData_15.ClientBase"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replaced value 22", {{"OData_15.ClientBase", Int64.Type}, {"OData_14.TrianingActivity", Int64.Type}, {"OData_13.RequiredTraining", Int64.Type}, {"OData_12.Retention", Int64.Type}, {"OData_11.ProgramPolicyInfo", Int64.Type}, {"OData_10.SystemAdminUpdate", Int64.Type}, {"OData_9.M365UserMaintenanc", Int64.Type}, {"OData_8.IncidentReporting", Int64.Type}, {"OData_7.CompleteSafetyDril", Int64.Type}, {"OData_6.ClientSurveyResult", Int64.Type}, {"OData_5.AnnualClientSurvey", Int64.Type}, {"OData_4.6-monthOutco", Int64.Type}, {"OData_3.AnnualOutcomes", Int64.Type}, {"FinancialPerformance", Int64.Type}, {"OData_2.AdequateCashflow", Int64.Type}}),
  #"Inserted sum" = Table.AddColumn(#"Changed column type", "Sum", each List.Sum({[OData_15.ClientBase], [OData_14.TrianingActivity], [OData_13.RequiredTraining], [OData_12.Retention], [OData_11.ProgramPolicyInfo], [OData_10.SystemAdminUpdate], [OData_9.M365UserMaintenanc], [OData_8.IncidentReporting], [OData_7.CompleteSafetyDril], [OData_6.ClientSurveyResult], [OData_5.AnnualClientSurvey], [#"OData_4.6-monthOutco"], [OData_3.AnnualOutcomes], [OData_2.AdequateCashflow], [FinancialPerformance]}), type nullable Int64.Type),
  #"Reordered columns 1" = Table.ReorderColumns(#"Inserted sum", {"Division", "Program", "Sum", "FinancialPerformance", "OData_2.AdequateCashflow", "OData_3.AnnualOutcomes", "OData_4.6-monthOutco", "OData_5.AnnualClientSurvey", "OData_6.ClientSurveyResult", "OData_7.CompleteSafetyDril", "OData_8.IncidentReporting", "OData_9.M365UserMaintenanc", "OData_10.SystemAdminUpdate", "OData_11.ProgramPolicyInfo", "OData_12.Retention", "OData_13.RequiredTraining", "OData_14.TrianingActivity", "OData_15.ClientBase", "Modified"}),
  #"Renamed columns" = Table.RenameColumns(#"Reordered columns 1", {{"Division", "division"}, {"Program", "program"}, {"Sum", "total_score"}, {"OData_2.AdequateCashflow", "cashflow"}, {"OData_3.AnnualOutcomes", "annual_outcomes"}, {"OData_4.6-monthOutco", "6mo_outcome"}, {"OData_5.AnnualClientSurvey", "client_survey"}, {"OData_6.ClientSurveyResult", "client_surveyResult"}, {"OData_7.CompleteSafetyDril", "safety_drill"}, {"OData_8.IncidentReporting", "incident_reporting"}, {"OData_9.M365UserMaintenanc", "365_user_maintenance"}, {"OData_10.SystemAdminUpdate", "system_admin_update"}, {"OData_11.ProgramPolicyInfo", "program_policy"}, {"OData_12.Retention", "retention"}, {"OData_13.RequiredTraining", "training_completion"}, {"OData_14.TrianingActivity", "training_quality"}, {"OData_15.ClientBase", "client_base"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"Modified", type datetime}})
in
  #"Changed column type 1";
shared #"Risk Management_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "Risk Management", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
