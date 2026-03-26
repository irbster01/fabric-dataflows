[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Progress Monitoring_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Progress Monitoring" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Progress Monitoring"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Drill down" = #"Filtered rows"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Drill down", null, true),
  #"Expanded Data" = Table.ExpandTableColumn(#"Imported Excel workbook", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32"}),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded Data", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Organization", type text}, {"School", type text}, {"Student ID", Int64.Type}, {"Client ID", Int64.Type}, {"Student Name", type text}, {"Grade Level", type text}, {"Sex/Gender", type text}, {"Race/Ethnicity", type text}, {"Ethnicity ", type text}, {"Case Manager", type text}, {"Goal", type text}, {"Metric", type text}, {"Baseline", type number}, {"Target", Int64.Type}, {"Baseline Date", type date}, {"Baseline Reporting Period", type text}, {"Target Set Date", type date}, {"Grading Period", type text}, {"Grading Period Progress", type text}, {"Non-Numeric Value", Int64.Type}, {"Metric Value", Int64.Type}, {"Review Date", type date}, {"Latest Progress All Grading Periods", type text}, {"Goal Narrative", type text}, {"Enrollment Begin Date", type date}, {"Enrollment End Date", type date}, {"Enrollment Status", type text}, {"School Year", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Sheet1", "Organization"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"School", "school_name"}, {"Student ID", "sasid"}, {"Client ID", "client_id"}, {"Student Name", "client_full_name"}, {"Grade Level", "grade"}, {"Sex/Gender", "gender"}, {"Race/Ethnicity", "race"}, {"Ethnicity ", "ethnicity"}, {"Case Manager", "case_manager"}, {"Goal", "goal"}, {"Metric", "metric"}, {"Baseline", "baseline"}, {"Target", "target"}, {"Baseline Date", "baseline_date"}, {"Baseline Reporting Period", "baseline_report_period"}, {"Target Set Date", "target_set_date"}, {"Grading Period", "grading_period"}, {"Grading Period Progress", "grading_period_progress"}, {"Non-Numeric Value", "nonNumeric_value"}, {"Metric Value", "metric_value"}, {"Days in Reporting Period", "days_in_report_prd"}, {"Excused Absences", "excused_absences"}, {"Unexcused Absences", "unexcused_absences"}, {"Review Date", "review_date"}, {"Latest Progress All Grading Periods", "latest_progress"}, {"Progress Notes", "progress_notes"}, {"Goal Narrative", "goal_narrative"}, {"Enrollment Begin Date", "entry_date"}, {"Enrollment End Date", "exit_date"}, {"Enrollment Status", "enrollment_status"}, {"School Year", "school_year"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"Sheet1_1", "Sheet", "false"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns 1", {{"unexcused_absences", type text}, {"excused_absences", type text}, {"days_in_report_prd", type text}, {"progress_notes", type text}, {"client_id", type text}, {"sasid", type text}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type 1", "short_name_academics", each if Text.Contains([metric], "Lang Arts") then "ELA" else if Text.Contains([metric], "English") then "ELA" else if Text.Contains([metric], "Math") then "Math" else if Text.Contains([metric], "Reading") then "ELA" else if Text.Contains([metric], "Social Studies") then "Social Studies" else if Text.Contains([metric], "Science") then "Science" else if Text.Contains([metric], "Credit Completion") then "Credit Completion" else "NOT ACADEMIC"),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "attendance_type", each if Text.Contains([metric], "Tardies") then "Tardies" else if Text.Contains([metric], "days absent") then "Absences" else if Text.Contains([metric], "Attendance Rate (%)") then "Attendance Rate" else "NOT ATTENDANCE"),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Inserted conditional column 1", {{"attendance_type", type text}, {"short_name_academics", type text}}),
  #"Sorted rows" = Table.Sort(#"Changed column type 2", {{"entry_date", Order.Descending}})
in
  #"Sorted rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "cis_client_list_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared cis_client_list = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Progress Monitoring"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Drill down" = #"Filtered rows"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Drill down", null, true),
  #"Expanded Data" = Table.ExpandTableColumn(#"Imported Excel workbook", "Data", {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32"}, {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "Column28", "Column29", "Column30", "Column31", "Column32"}),
  #"Promoted headers" = Table.PromoteHeaders(#"Expanded Data", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Organization", type text}, {"School", type text}, {"Student ID", Int64.Type}, {"Client ID", Int64.Type}, {"Student Name", type text}, {"Grade Level", type text}, {"Sex/Gender", type text}, {"Race/Ethnicity", type text}, {"Ethnicity ", type text}, {"Case Manager", type text}, {"Goal", type text}, {"Metric", type text}, {"Baseline", type number}, {"Target", Int64.Type}, {"Baseline Date", type date}, {"Baseline Reporting Period", type text}, {"Target Set Date", type date}, {"Grading Period", type text}, {"Grading Period Progress", type text}, {"Non-Numeric Value", Int64.Type}, {"Metric Value", Int64.Type}, {"Review Date", type date}, {"Latest Progress All Grading Periods", type text}, {"Goal Narrative", type text}, {"Enrollment Begin Date", type date}, {"Enrollment End Date", type date}, {"Enrollment Status", type text}, {"School Year", type text}}),
  #"Removed columns" = Table.RemoveColumns(#"Changed column type", {"Sheet1", "Organization"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"School", "school_name"}, {"Student ID", "sasid"}, {"Client ID", "client_id"}, {"Student Name", "client_full_name"}, {"Grade Level", "grade"}, {"Sex/Gender", "gender"}, {"Race/Ethnicity", "race"}, {"Ethnicity ", "ethnicity"}, {"Case Manager", "case_manager"}, {"Goal", "goal"}, {"Metric", "metric"}, {"Baseline", "baseline"}, {"Target", "target"}, {"Baseline Date", "baseline_date"}, {"Baseline Reporting Period", "baseline_report_period"}, {"Target Set Date", "target_set_date"}, {"Grading Period", "grading_period"}, {"Grading Period Progress", "grading_period_progress"}, {"Non-Numeric Value", "nonNumeric_value"}, {"Metric Value", "metric_value"}, {"Days in Reporting Period", "days_in_report_prd"}, {"Excused Absences", "excused_absences"}, {"Unexcused Absences", "unexcused_absences"}, {"Review Date", "review_date"}, {"Latest Progress All Grading Periods", "latest_progress"}, {"Progress Notes", "progress_notes"}, {"Goal Narrative", "goal_narrative"}, {"Enrollment Begin Date", "entry_date"}, {"Enrollment End Date", "exit_date"}, {"Enrollment Status", "enrollment_status"}, {"School Year", "school_year"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"Sheet1_1", "Sheet", "false"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns 1", {{"unexcused_absences", type text}, {"excused_absences", type text}, {"days_in_report_prd", type text}, {"progress_notes", type text}, {"client_id", type text}, {"sasid", type text}}),
  #"Removed other columns" = Table.SelectColumns(#"Changed column type 1", {"sasid", "client_id", "client_full_name", "school_name"}),
  #"Removed duplicates" = Table.Distinct(#"Removed other columns", {"client_id"}),
  #"Removed duplicates 1" = Table.Distinct(#"Removed duplicates", {"sasid"})
in
  #"Removed duplicates 1";
shared cis_client_list_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "cis_client_list", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Progress Monitoring_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_progress_monitoring", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
