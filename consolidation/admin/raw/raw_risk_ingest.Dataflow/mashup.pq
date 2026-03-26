[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Risk Management_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Risk Management" = let
  Source = SharePoint.Tables("https://voanorthlaorg.sharepoint.com/sites/TheVOAHub/", [ApiVersion = 15]),
  #"Navigation 1" = Source{[Id = "9212e592-f021-4e8b-9dd8-9657661d8d64"]}[Items],
  #"Expanded test" = Table.ExpandRecordColumn(#"Navigation 1", "test", {"ProgramName", "ProgramNumber", "Division"}, {"ProgramName", "ProgramNumber", "Division.1"}),

  #"Removed columns" =
    Table.RemoveColumns(
      #"Expanded test",
      {
        "FileSystemObjectType","Id","ServerRedirectedEmbedUri","ServerRedirectedEmbedUrl","ContentTypeId","OData__ColorTag","ComplianceAssetId",
        "Editor","Author","Versions","Properties","ParentList","LikedByInformation","Folder","File","FieldValuesForEdit","FieldValuesAsText",
        "FieldValuesAsHtml","GetDlpPolicyTip","ContentType","AttachmentFiles","RoleAssignments","FirstUniqueAncestorSecurableObject","GUID",
        "Attachments","OData__UIVersionString","EditorId","AuthorId"
      }
    ),

  #"Renamed columns" =
    Table.RenameColumns(
      #"Removed columns",
      {
        {"OData_15.ClientBaseNotes", "client_base_notes"},
        {"OData_13.RequiredTrainingN", "required_training_notes"},
        {"OData_12.RetentionNotes", "retention_notes"},
        {"OData_11.ProgramPoliciesNo", "policies_notes"},
        {"OData_6.ClientSurveyResult0", "client_survey_results_notes"},
        {"OData_5.AnnualClientSurvey0", "annual_client_survey_notes"},
        {"OData_4.6-monthOutco0", "6_month_outcome_notes"},
        {"OData_3.AnnualOutcomesNote", "annual_outcomes_notes"},
        {"OData_1.FinancialPerforman", "financial_performance_notes"},
        {"CalendarYear", "calendar_year"},
        {"SurveyType", "survey_type"},
        {"Status", "status"}
      }
    ),
  #"Replaced value 22" = Table.ReplaceValue(#"Renamed columns", "No", 0, Replacer.ReplaceValue, {"OData_11.ProgramPolicyInfo"}),
  #"Replaced value 23" = Table.ReplaceValue(#"Replaced value 22", "Yes", 3, Replacer.ReplaceValue, {"OData_11.ProgramPolicyInfo"}),

  #"Removed columns 1" = Table.RemoveColumns(#"Replaced value 23", {"testId"}),

  #"Renamed columns 1" =
    Table.RenameColumns(
      #"Removed columns 1",
      {
        {"Program", "program"},
        {"OData_15.ClientBase", "client_base"},
        {"OData_12.Retention", "retention"},
        {"OData_10.SystemAdminUpdate", "sys_admin_updated"},
        {"OData_9.M365UserMaintenanc", "m365_user_maintenance"},
        {"OData_8.IncidentReporting", "incident_reporting"},
        {"OData_7.CompleteSafetyDril", "safety_drills"},
        {"OData_6.ClientSurveyResult", "client_survey_results"},
        {"OData_5.AnnualClientSurvey", "annual_client_survey"},
        {"OData_4.6-monthOutco", "6mo_outcomes"},
        {"OData_3.AnnualOutcomes", "annual_outcomes"},
        {"FinancialPerformance", "financial_performance"},
        {"OData_14.TrainingActivityN", "training_activity_notes"},
        {"OData_10.SystemAdminUpdate0", "sys_admin_ntoes"},
        {"OData_9.M365UserMaintenanc0", "m365_user_notes"},
        {"OData_7.CompleteSafetyDril0", "safety_drill_notes"}
      }
    ),

  #"Changed column type" =
    Table.TransformColumnTypes(
      #"Renamed columns 1",
      {
        {"client_survey_results_notes", type text},
        {"annual_client_survey_notes", type text},
        {"6_month_outcome_notes", type text},
        {"financial_performance_notes", type text}
      }
    ),

  #"Renamed columns 2" =
    Table.RenameColumns(
      #"Changed column type",
      {{"OData_2.AdequateCashflowNo", "adequate_cashflow_notes"}, {"OData_8.IncidentReportingD", "incident_reporting_notes"}}
    ),

  #"Changed column type 1" =
    Table.TransformColumnTypes(
      #"Renamed columns 2",
      {
        {"incident_reporting_notes", type text},
        {"financial_performance_notes", type text},
        {"adequate_cashflow_notes", type text},
        {"annual_outcomes_notes", type text},
        {"6_month_outcome_notes", type text},
        {"calendar_year", type text},
        {"survey_type", type text},
        {"status", type text},
        {"program", type text}
      }
    ),

  #"Renamed columns 3" =
    Table.RenameColumns(
      #"Changed column type 1",
      {{"OData_14.TrianingActivity", "training_activity"}, {"OData_2.AdequateCashflow", "adequate_cashflow"}}
    ),

  #"Changed column type 2" =
    Table.TransformColumnTypes(
      #"Renamed columns 3",
      {
        {"Title", type text},
        {"Division", type text},
        {"financial_performance", type text},
        {"adequate_cashflow", type text},
        {"annual_outcomes", type text},
        {"6mo_outcomes", type text},
        {"annual_client_survey", type text},
        {"client_survey_results", type text},
        {"safety_drills", type text},
        {"incident_reporting", type text},
        {"m365_user_maintenance", type text},
        {"sys_admin_updated", type text},
        {"OData_11.ProgramPolicyInfo", type text}
      }
    ),

  #"Renamed columns 4" = Table.RenameColumns(#"Changed column type 2", {{"OData_13.RequiredTraining", "required_training"}}),

  #"Changed column type 3" =
    Table.TransformColumnTypes(
      #"Renamed columns 4",
      {
        {"retention", type text},
        {"required_training", type text},
        {"training_activity", type text},
        {"client_base", type text},
        {"safety_drill_notes", type text},
        {"m365_user_notes", type text},
        {"sys_admin_ntoes", type text},
        {"policies_notes", type text},
        {"retention_notes", type text},
        {"required_training_notes", type text},
        {"training_activity_notes", type text},
        {"client_base_notes", type text},
        {"ID", Int64.Type},
        {"Modified", type datetime},
        {"Created", type datetime}
      }
    ),

  #"Renamed columns 5" =
    Table.RenameColumns(
      #"Changed column type 3",
      {{"Division", "division"}, {"Title", "title"}, {"Modified", "modified"}, {"Created", "created"}, {"ID", "id"}}
    ),

  // Map Yes/No to numeric codes (your existing mappings)
  #"Replaced value"   = Table.ReplaceValue(#"Renamed columns 5", "Yes", "5", Replacer.ReplaceText, {"financial_performance"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value",    "No",  "0", Replacer.ReplaceText, {"financial_performance"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1",  "Yes", "5", Replacer.ReplaceText, {"adequate_cashflow"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2",  "No",  "0", Replacer.ReplaceText, {"adequate_cashflow"}),

  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3",  "Yes", "3", Replacer.ReplaceValue, {"6mo_outcomes"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4",  "No",  "0", Replacer.ReplaceValue, {"6mo_outcomes"}),

  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5",  "Yes", "5", Replacer.ReplaceValue, {"annual_client_survey"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6",  "No",  "0", Replacer.ReplaceValue, {"annual_client_survey"}),

  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7",  "No",  "0", Replacer.ReplaceValue, {"client_survey_results"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8",  "Yes", "3", Replacer.ReplaceValue, {"client_survey_results"}),

  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9",  "Yes", "5", Replacer.ReplaceValue, {"safety_drills"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", "No",  "0", Replacer.ReplaceText,  {"safety_drills"}),

  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", "Yes", "4", Replacer.ReplaceValue, {"incident_reporting"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", "No",  "0", Replacer.ReplaceText,  {"incident_reporting"}),

  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", "No",  "0", Replacer.ReplaceValue, {"client_base"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", "Yes", "5", Replacer.ReplaceValue, {"client_base"}),

  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", "No",  "0", Replacer.ReplaceValue, {"retention"}),

  #"Removed columns 2" = Table.RemoveColumns(#"Replaced value 16", {"title"}),

  // FIX 1: m365_user_maintenance had no "No" mapping before numeric cast
  #"Replaced value 17"  = Table.ReplaceValue(#"Removed columns 2", "Yes", "3", Replacer.ReplaceValue, {"m365_user_maintenance"}),
  #"Replaced value 17b" = Table.ReplaceValue(#"Replaced value 17", "No",  "0", Replacer.ReplaceValue, {"m365_user_maintenance"}),

  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 17b", "Yes", "3", Replacer.ReplaceValue, {"sys_admin_updated"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 18",  "No",  "0", Replacer.ReplaceText,  {"sys_admin_updated"}),

  #"Replaced value 20" = Table.ReplaceValue(#"Replaced value 19", "Yes", "4", Replacer.ReplaceValue, {"retention"}),

  // Keep your N/A handling
  #"Replaced value 21" = Table.ReplaceValue(#"Replaced value 20", "N/A", "5", Replacer.ReplaceValue, {"annual_outcomes"}),

  // Rename before coercion
  #"Renamed columns 6" = Table.RenameColumns(#"Replaced value 21", {{"OData_11.ProgramPolicyInfo", "program_policy"}}),

  // FIX 2: safe numeric coercion (prevents Lakehouse write failures on non-numeric values)
  ToInt64 = (x as any) as nullable number =>
    let
      t = try Text.Trim(Text.From(x)) otherwise null,
      n = if t = null or t = "" then null else (try Number.FromText(t) otherwise try Number.From(x) otherwise null),
      i = if n = null then null else Int64.From(n)
    in
      i,

  #"Coerce numeric columns" =
    Table.TransformColumns(
      #"Renamed columns 6",
      {
        {"financial_performance", each ToInt64(_), Int64.Type},
        {"adequate_cashflow",     each ToInt64(_), Int64.Type},
        {"annual_outcomes",       each ToInt64(_), Int64.Type},
        {"6mo_outcomes",          each ToInt64(_), Int64.Type},
        {"annual_client_survey",  each ToInt64(_), Int64.Type},
        {"client_survey_results", each ToInt64(_), Int64.Type},
        {"safety_drills",         each ToInt64(_), Int64.Type},
        {"incident_reporting",    each ToInt64(_), Int64.Type},
        {"m365_user_maintenance", each ToInt64(_), Int64.Type},
        {"sys_admin_updated",     each ToInt64(_), Int64.Type},
        {"retention",             each ToInt64(_), Int64.Type},
        {"client_base",           each ToInt64(_), Int64.Type},
        {"program_policy",        each ToInt64(_), Int64.Type},
        {"required_training",     each ToInt64(_), Int64.Type},
        {"training_activity",     each ToInt64(_), Int64.Type}
      }
    ),

  #"Removed columns 3" = Table.RemoveColumns(#"Coerce numeric columns", {"Division.1"}),

  #"Reordered columns" =
    Table.ReorderColumns(
      #"Removed columns 3",
      {
        "division","ProgramNumber","ProgramName",
        "financial_performance","adequate_cashflow","annual_outcomes","6mo_outcomes","annual_client_survey","client_survey_results",
        "safety_drills","incident_reporting","m365_user_maintenance","sys_admin_updated","program_policy","retention",
        "required_training","training_activity","client_base",
        "program","status","survey_type","calendar_year",
        "incident_reporting_notes","financial_performance_notes","adequate_cashflow_notes","annual_outcomes_notes","6_month_outcome_notes",
        "annual_client_survey_notes","client_survey_results_notes","safety_drill_notes","m365_user_notes","sys_admin_ntoes",
        "policies_notes","retention_notes","required_training_notes","training_activity_notes","client_base_notes",
        "id","modified","created"
      }
    ),

  #"Changed column type 7" = Table.TransformColumnTypes(#"Reordered columns", {{"ProgramNumber", type text}, {"ProgramName", type text}}),
  #"Replaced value 24" = Table.ReplaceValue(#"Changed column type 7", 5, 3, Replacer.ReplaceValue, {"annual_client_survey"}),
  #"Replaced value 25" = Table.ReplaceValue(#"Replaced value 24", 3, 4, Replacer.ReplaceValue, {"client_survey_results"}),
  #"Inserted sum" = Table.AddColumn(#"Replaced value 25", "Sum", each List.Sum({[financial_performance], [adequate_cashflow], [6mo_outcomes], [annual_client_survey], [client_survey_results], [safety_drills], [incident_reporting], [m365_user_maintenance], [sys_admin_updated], [program_policy], [retention], [client_base]}), Int64.Type),
  #"Renamed columns 7" = Table.RenameColumns(#"Inserted sum", {{"Sum", "6_mo_total"}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Renamed columns 7", "6_mo_max_poss", each 47), {{"6_mo_max_poss", Int64.Type}}),
  #"Reordered columns 1" = Table.ReorderColumns(#"Added custom", {"division", "ProgramNumber", "ProgramName", "financial_performance", "adequate_cashflow", "annual_outcomes", "6mo_outcomes", "annual_client_survey", "client_survey_results", "safety_drills", "incident_reporting", "m365_user_maintenance", "sys_admin_updated", "program_policy", "retention", "required_training", "training_activity", "client_base", "6_mo_total", "6_mo_max_poss", "program", "status", "survey_type", "calendar_year", "incident_reporting_notes", "financial_performance_notes", "adequate_cashflow_notes", "annual_outcomes_notes", "6_month_outcome_notes", "annual_client_survey_notes", "client_survey_results_notes", "safety_drill_notes", "m365_user_notes", "sys_admin_ntoes", "policies_notes", "retention_notes", "required_training_notes", "training_activity_notes", "client_base_notes", "id", "modified", "created"}),
  #"Removed columns 4" = Table.RemoveColumns(#"Reordered columns 1", {"program"})
in
  #"Removed columns 4";
shared #"Risk Management_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "Risk Management", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "risk_zero_counter_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared risk_zero_counter = let
  Source = #"Risk Management",
  #"Replaced value" = Table.ReplaceValue(Source, 0, 1, Replacer.ReplaceValue, {"financial_performance"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", 5, 0, Replacer.ReplaceValue, {"financial_performance"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Replaced value 1", 0, 1, Replacer.ReplaceValue, {"adequate_cashflow"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", 5, 0, Replacer.ReplaceValue, {"adequate_cashflow"}),
  #"Replaced value 4" = Table.ReplaceValue(#"Replaced value 3", 0, 1, Replacer.ReplaceValue, {"6mo_outcomes"}),
  #"Replaced value 5" = Table.ReplaceValue(#"Replaced value 4", 3, 0, Replacer.ReplaceValue, {"6mo_outcomes"}),
  #"Replaced value 6" = Table.ReplaceValue(#"Replaced value 5", 0, 1, Replacer.ReplaceValue, {"annual_client_survey"}),
  #"Replaced value 7" = Table.ReplaceValue(#"Replaced value 6", 5, 0, Replacer.ReplaceValue, {"annual_client_survey"}),
  #"Replaced value 8" = Table.ReplaceValue(#"Replaced value 7", 0, 1, Replacer.ReplaceValue, {"client_survey_results"}),
  #"Replaced value 9" = Table.ReplaceValue(#"Replaced value 8", 3, 0, Replacer.ReplaceValue, {"client_survey_results"}),
  #"Replaced value 10" = Table.ReplaceValue(#"Replaced value 9", 5, 0, Replacer.ReplaceValue, {"safety_drills"}),
  #"Replaced value 11" = Table.ReplaceValue(#"Replaced value 10", 4, 0, Replacer.ReplaceValue, {"incident_reporting"}),
  #"Replaced value 12" = Table.ReplaceValue(#"Replaced value 11", 3, 0, Replacer.ReplaceValue, {"m365_user_maintenance"}),
  #"Replaced value 13" = Table.ReplaceValue(#"Replaced value 12", 3, 0, Replacer.ReplaceValue, {"sys_admin_updated"}),
  #"Replaced value 14" = Table.ReplaceValue(#"Replaced value 13", 0, 1, Replacer.ReplaceValue, {"program_policy"}),
  #"Replaced value 15" = Table.ReplaceValue(#"Replaced value 14", 3, 0, Replacer.ReplaceValue, {"program_policy"}),
  #"Replaced value 16" = Table.ReplaceValue(#"Replaced value 15", 0, 1, Replacer.ReplaceValue, {"retention"}),
  #"Replaced value 17" = Table.ReplaceValue(#"Replaced value 16", 4, 0, Replacer.ReplaceValue, {"retention"}),
  #"Replaced value 18" = Table.ReplaceValue(#"Replaced value 17", 0, 1, Replacer.ReplaceValue, {"client_base"}),
  #"Replaced value 19" = Table.ReplaceValue(#"Replaced value 18", 5, 0, Replacer.ReplaceValue, {"client_base"}),
  #"Removed columns" = Table.RemoveColumns(#"Replaced value 19", {"incident_reporting_notes", "financial_performance_notes", "adequate_cashflow_notes", "annual_outcomes_notes", "6_month_outcome_notes", "annual_client_survey_notes", "client_survey_results_notes", "safety_drill_notes", "m365_user_notes", "sys_admin_ntoes", "policies_notes", "retention_notes", "required_training_notes", "training_activity_notes", "client_base_notes", "id", "modified", "created", "6_mo_total", "6_mo_max_poss"}),
  #"Replaced value 20" = Table.ReplaceValue(#"Removed columns", 5, 0, Replacer.ReplaceValue, {"annual_outcomes"}),
  #"Replaced value 21" = Table.ReplaceValue(#"Replaced value 20", 3, 0, Replacer.ReplaceValue, {"annual_client_survey"}),
  #"Replaced value 22" = Table.ReplaceValue(#"Replaced value 21", 4, 0, Replacer.ReplaceValue, {"client_survey_results"})
in
  #"Replaced value 22";
shared risk_zero_counter_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "risk_zero_counter", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "division_average_by_item_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared division_average_by_item = let
  Source = #"Risk Management",
  #"Filtered rows" = Table.SelectRows(Source, each ([ProgramNumber] <> "5002")),
  #"Grouped rows" = Table.Group(#"Filtered rows", {"division"}, {{"6mo_outcomes", each List.Average([6mo_outcomes]), type nullable number}, {"annual_client_survey", each List.Average([annual_client_survey]), type nullable number}, {"cclient_survey_results", each List.Average([client_survey_results]), type nullable number}, {"safety_drills", each List.Average([safety_drills]), type nullable number}, {"incident_reporting", each List.Average([incident_reporting]), type nullable number}, {"m365_user_maintenance", each List.Average([m365_user_maintenance]), type nullable number}, {"sys_admin_updated", each List.Average([sys_admin_updated]), type nullable number}, {"program_p[olicy", each List.Average([program_policy]), type nullable number}, {"retention", each List.Average([retention]), type nullable number}, {"financial_performance", each List.Average([financial_performance]), type nullable number}, {"adequate_cashflow", each List.Average([adequate_cashflow]), type nullable number}}),
  #"Inserted sum" = Table.TransformColumnTypes(Table.AddColumn(#"Grouped rows", "Sum", each List.Sum({[6mo_outcomes], [annual_client_survey], [cclient_survey_results], [safety_drills], [incident_reporting], [m365_user_maintenance], [sys_admin_updated], [#"program_p[olicy"], [financial_performance], [adequate_cashflow], [retention]})), {{"Sum", type number}}),
  #"Renamed columns" = Table.RenameColumns(#"Inserted sum", {{"Sum", "avg_total"}})
in
  #"Renamed columns";
shared division_average_by_item_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "49b28ef6-6017-4b73-8e5a-61dab4008c9a"]}[Data],
  TableNavigation = Navigation_2{[Id = "division_average_by_item", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
