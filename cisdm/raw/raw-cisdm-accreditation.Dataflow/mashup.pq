[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared SchoolYear = "2526 CIS" meta [Description = "School year folder (e.g., '2526 CIS' for 2025-2026)"];
shared WorkspaceId = "4241c9c7-5818-4c56-9220-faf632741770" meta [Description = "Fabric workspace ID"];
shared LakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea" meta [Description = "Lakehouse ID"];
shared GetAccreditationFolder = () => 
  let
    ConnectToLakehouse = Lakehouse.Contents(null),
    NavigateToWorkspace = ConnectToLakehouse{[workspaceId = WorkspaceId]}[Data],
    NavigateToLakehouse = NavigateToWorkspace{[lakehouseId = LakehouseId]}[Data],
    NavigateToFilesFolder = NavigateToLakehouse{[Id = "Files", ItemKind = "Folder"]}[Data],
    NavigateToSchoolYearFolder = NavigateToFilesFolder{[Name = SchoolYear]}[Content],
    NavigateToAccreditationFolder = NavigateToSchoolYearFolder{[Name = "Accreditation"]}[Content]
  in
    NavigateToAccreditationFolder;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Accreditation_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared student_drilldown = let
  Source = Lakehouse.Contents(null), // Connect to lakehouse
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data], // Navigate to voa-data-warehouse workspace
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data], // Navigate to CIS lakehouse
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data], // Navigate to Files folder
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content], // Navigate to current school year folder
  #"Navigation 4" = #"Navigation 3"{[Name = "Accreditation"]}[Content], // Navigate to Accreditation folder
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}), // Expand file metadata columns
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true), // Remove hidden files from file list
  #"Filtered rows 1" = Table.SelectRows(#"Filtered hidden files", let latest = List.Max(#"Filtered hidden files"[Date modified]) in each [Date modified] = latest), // Keep only the most recently modified file
  #"Invoke custom function" = Table.AddColumn(#"Filtered rows 1", "Transform file", each #"Transform file"([Content])), // Load Excel data using transform function
  #"Filtered rows" = Table.SelectRows(#"Invoke custom function", let latest = List.Max(#"Invoke custom function"[Date modified]) in each [Date modified] = latest), // Ensure only latest file is processed
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"Transform file"}), // Keep only the Excel data column
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file", Table.ColumnNames(#"Transform file"(#"Sample file"))), // Expand Excel table into columns
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column4", type text}, {"Column6", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column16", type text}, {"Column21", type text}, {"Column22", type text}, {"Column23", type text}, {"Column24", type text}, {"Column31", type text}, {"Column38", type text}, {"Column39", type text}}), // Set initial column types to text
  #"Removed top rows" = Table.Skip(#"Changed column type", 1), // Remove Excel title row
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]), // Use first row as column headers
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"Site ID", Int64.Type}, {"NCES ID", Int64.Type}, {"# of Grading Periods", Int64.Type}, {"Student ID", Int64.Type}, {"Student System ID", Int64.Type}, {"Enrollment Begin Date", type date}, {"Consent Begin Date", type date}, {"Consent End Date", type date}, {"# of Times Support Plan Was Adjusted", Int64.Type}, {"# of Tier II/III Supports", Int64.Type}, {"# of Tier II/III #(lf)Support Hours", type number}, {"# of Basic Needs/Resources entries (Tier II/III)", Int64.Type}, {"# of Formal Check-Ins", Int64.Type}, {"Months Enrolled #(lf)(30-day periods) #(lf)Rounded Down", Int64.Type}, {"# Goal Areas", Int64.Type}, {"# ABCS Metrics Assigned", Int64.Type}, {"# Non-ABCS Metrics Assigned", Int64.Type}, {"# Progress Entries", Int64.Type}, {"Expected Progress Entries for the Year", Int64.Type}, {"# of Metrics with Goal Achievement entered", Int64.Type}}), // Convert columns to proper data types (integers, dates, numbers)
  #"Renamed columns" = Table.RenameColumns(#"Changed column type 1", {{"School", "school_name"}}), // Rename School to school_name
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"Site ID", "Organization", "NCES ID", "# of Grading Periods"}), // Remove unnecessary identification columns
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed columns", {{"Student ID", type text}, {"Student System ID", type text}}), // Convert student IDs back to text to preserve leading zeros
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type 2", {{"Student ID", "sidno"}, {"Student System ID", "client_id"}, {"Student Name", "client_full_name"}, {"Race/Ethnicity", "race"}, {"Sex/Gender", "client_gender"}, {"Ethnicity", "ethnicity"}, {"Case Managed Status", "status"}, {"Case Management Intensity Level", "case_management_intensity"}, {"Grade Level", "grade"}, {"Case Manager", "case)manager"}, {"Current Enrollment Status", "enrollment_status"}, {"Enrollment Begin Date", "entry_start_date"}, {"Enrollment Exit Date", "entry_exit_date"}, {"Consent Begin Date", "consent_begin_date"}, {"Consent End Date", "consent_end_date"}, {"Parental Consent Dates Entered #(lf)and Tier II/III Supports #(lf)Within Consent Dates?", "consent_compliant"}}), // Rename student demographic and enrollment columns to standard names
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"Parental Consent Dates Entered #(lf)and Tier 1/Basic Needs Supports #(lf)Within Consent Dates?"}), // Remove duplicate consent compliance column
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"Student Needs Assessment Entered?", "needs_entered"}, {"Student Support Plan Entered?", "support_plan_entered"}, {"# of Times Support Plan Was Adjusted", "number_adjusted"}, {"# of Tier II/III Supports", "num_II_III_contacts"}, {"# of Tier II/III #(lf)Support Hours", "num_II_III_hours"}, {"# of Basic Needs/Resources entries (Tier II/III)", "num_basic_entries"}, {"# of Formal Check-Ins", "num_formal_checkins"}, {"Months Enrolled #(lf)(30-day periods) #(lf)Rounded Down", "months_enrolled"}, {"On Track for Monthly Check-Ins?", "checkins_on_track"}, {"# Goal Areas", "goal_areas"}, {"# ABCS Metrics Assigned", "abcs_metrics_assigned"}, {"# Non-ABCS Metrics Assigned", "non_abcs_metrics_assigned"}, {"# Progress Entries", "num_progress_entries"}, {"Expected Progress Entries for the Year", "expected_progress_entries"}, {"# of Metrics with Goal Achievement entered", "num_of_metrics_with_goal_achieve"}, {"Goal Achievement Entered for ALL assigned goals?", "goal_for_all"}, {"EOY Status Entered", "eoy_status_entered"}}), // Rename case management metrics to standard field names
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns 2", "-", null, Replacer.ReplaceValue, {"entry_exit_date"}), // Replace dash placeholders with null in exit date
  #"Changed column type 3" = Table.TransformColumnTypes(#"Replaced value", {{"entry_exit_date", type date}}), // Convert exit date to date type
  #"Added suffix" = Table.TransformColumns(#"Changed column type 3", {{"client_id", each _ & "-CIS", type text}}), // Append -CIS suffix to client_id for system identification
  #"Renamed columns 3" = Table.RenameColumns(#"Added suffix", {{"sidno", "sasid"}, {"case)manager", "case_manager"}}), // Fix final column names (typo correction)
  #"Sorted rows" = Table.Sort(#"Renamed columns 3", {{"consent_begin_date", Order.Descending}}) // Sort by consent begin date, newest first
in
  #"Sorted rows";
shared #"Sample file" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Accreditation"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared Parameter = let
  Parameter = #"Sample file" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file"]
in
  Parameter;
shared #"Transform Sample file" = let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Accreditation Student Drilldown", Kind = "Sheet"]}[Data]
in
  Navigation;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file""}"]
shared #"Transform file" = (Parameter as binary) => let
  Source = Excel.Workbook(Parameter, null, true),
  Navigation = Source{[Item = "Accreditation Student Drilldown", Kind = "Sheet"]}[Data]
in
  Navigation;
shared Accreditation_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_accred_drilldown", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "tier1_accred_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared tier1_accred = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Accreditation"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (2)", each #"Transform file (2)"([Content])),
  #"Filtered rows" = Table.SelectRows(#"Invoke custom function", let latest = List.Max(#"Invoke custom function"[Date modified]) in each [Date modified] = latest),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"Transform file (2)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (2)", Table.ColumnNames(#"Transform file (2)"(#"Sample file (2)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 1),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"Site ID", Int64.Type}, {"NCES ID", Int64.Type}, {"Total School Enrollment", Int64.Type}, {"# of Basic Needs #(lf)Entries (whole-school)", Int64.Type}, {"Basic Needs - #(lf)Total Students Served (duplicated)", Int64.Type}, {"Basic Needs - #(lf)Min Served #(lf)(by any one resource)", Int64.Type}, {"Basic Needs - #(lf)Max Served #(lf)(by any one resource)", Int64.Type}, {"# of Tier I #(lf)Supports", Int64.Type}, {"Tier I - #(lf)Total Students Served#(lf) (duplicated)", Int64.Type}, {"Tier I - #(lf)Min Served #(lf)(at any one support)", Int64.Type}, {"Tier I - #(lf)Max Served #(lf)(at any one support)", Int64.Type}, {"# of Tier I and #(lf)Basic Needs/Resources#(lf) combined", Int64.Type}}),
  #"Removed blank rows" = Table.SelectRows(#"Changed column type 1", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
  #"Removed columns" = Table.RemoveColumns(#"Removed blank rows", {"NCES ID", "Site ID", "Organization", "School District"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed columns", {{"School", "school_name"}, {"School Model ", "School Model"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns", {"School Model"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns 1", {{"School Type", "school_type"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns 1", {"School Status"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 2", {{"Total School Enrollment", "school_enrollment"}, {"# of Basic Needs #(lf)Entries (whole-school)", "num_school_basic_needs"}, {"Basic Needs - #(lf)Total Students Served (duplicated)", "total_served_dupe"}, {"Basic Needs- #(lf)Avg Served", "avg_served"}, {"Basic Needs - #(lf)Min Served #(lf)(by any one resource)", "min_served"}, {"Basic Needs - #(lf)Max Served #(lf)(by any one resource)", "max_served"}, {"# of Tier I #(lf)Supports", "num_tier1_supports"}, {"Tier I - #(lf)Total Students Served#(lf) (duplicated)", "total_students_dupe"}, {"Tier I - #(lf)Avg Served", "tier1_avg_served"}, {"Tier I - #(lf)Min Served #(lf)(at any one support)", "tier1_min_served"}, {"Tier I - #(lf)Max Served #(lf)(at any one support)", "tier1_max_served"}, {"# of Tier I and #(lf)Basic Needs/Resources#(lf) combined", "num_tier1_and_needs"}}),
  #"Replaced value" = Table.ReplaceValue(#"Renamed columns 2", "-", 0, Replacer.ReplaceValue, {"avg_served"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "-", 0, Replacer.ReplaceValue, {"tier1_avg_served"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Replaced value 1", {{"avg_served", type number}, {"tier1_avg_served", type number}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type 2", each ([school_enrollment] <> null))
in
  #"Filtered rows 1";
shared #"Sample file (2)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Accreditation"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered hidden files" = Table.SelectRows(#"Expanded Attributes", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (2)" = let
  #"Parameter (2)" = #"Sample file (2)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (2)"]
in
  #"Parameter (2)";
shared #"Transform Sample file (2)" = let
  Source = Excel.Workbook(#"Parameter (2)", null, true),
  Navigation = Source{[Item = "Accreditation Tier I", Kind = "Sheet"]}[Data]
in
  Navigation;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (2)""}"]
shared #"Transform file (2)" = (#"Parameter (2)" as binary) => let
  Source = Excel.Workbook(#"Parameter (2)", null, true),
  Navigation = Source{[Item = "Accreditation Tier I", Kind = "Sheet"]}[Data]
in
  Navigation;
shared tier1_accred_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_tier1_accred", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_cisdm_accred_case_manage_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_cisdm_accred_case_manage = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Accreditation"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Drill down" = #"Filtered rows"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Drill down", null, true),
  #"Drill down 1" = #"Imported Excel workbook"{4}[Data],
  #"Removed top rows" = Table.Skip(#"Drill down 1", 1),
  #"Removed bottom rows" = Table.RemoveLastN(#"Removed top rows", 1),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed bottom rows", [PromoteAllScalars = true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted headers", {{"Organization", "org"}, {"School", "school_name"}, {"Site ID", "site_id"}, {"NCES ID", "nces_id"}, {"School Model", "school_model"}, {"School District", "district"}, {"School Type", "school_type"}, {"School Status", "school_status"}, {"Total School Enrollment", "total_school_enrollment"}, {"# of Grading Periods", "num_of_grading_periods"}, {"Total Case #(lf)Managed Students", "total_case_managed_students"}, {"# of CM Students #(lf)Enrolled With #(lf)Support Plans", "num_of_students_with_support_plan"}, {"% of CM Students #(lf)with Support Plans", "%of_students_w_support_plan"}, {"# of CM Students #(lf)with Tier II/III Supports", "num_of_students_w_tier1_2"}, {"# of CM Students #(lf)with Tier II/III Supports #(lf)and Support Plan", "num_w_tier2&3_w_support_plan"}, {"Saturation Rate ", "saturation_rate"}, {"# of Enrolled Students #(lf)Receiving Tier II/III Supports #(lf)outside of Parent Consent Dates", "num_of_enrolled_with_outdated_consent"}, {"# of Enrolled Students With#(lf) Tier I/Basic Needs  Supports Tracked outside of Parent Consent Dates", "num_tracked_outside_consent_dates"}, {"# of Enrolled Students with #(lf)Basic Needs/Resources #(lf)entered (Tier II/III)", "num_w_basic_needs_entered"}, {"# of CM Students with #(lf)at least one Formal Check-In ", "num_w_>1_formal_checkin"}, {"% of CM Students with #(lf)at least one Formal Check-In ", "%_w_1+_formal_checkin"}, {"% of CM Students with #(lf)On Track Monthly #(lf)Check-Ins", "%_on_track_monthly_checkins"}}),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Renamed columns", {"% of CM Students with Adjusted Support Plans"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed errors", {{"%_w_1+_formal_checkin", Percentage.Type}, {"%_on_track_monthly_checkins", Percentage.Type}, {"% of CM Students with Adjusted Support Plans", Percentage.Type}}),
  #"Renamed columns 1" = Table.RenameColumns(#"Changed column type", {{"# of CM Students with #(lf)at least one Progress Entry #(lf)ABCS goals", "num_w_1+_progress_entry"}, {"% of CM Students with#(lf) at least one Progress Entry #(lf)ABCS goals", "%_w_1+_progress_entry"}, {"# of CM Students with #(lf)Goal Achievement for all #(lf)ABCS goals (metrics)", "num_w_goal_achieve"}, {"% of CM Students with #(lf)Goal Achievement Entered #(lf)for all ABCS goals (metrics)", "#_w_goal_achieve"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 1", {"# of CM Students with #(lf)Goal Achievement Entered for all #(lf)Non-ABCS goals (metrics)", "% of CM Students with #(lf)Goal Achievement Entered for all#(lf) Non-ABCS goals (metrics)"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed columns", {{"% of CM Students with#(lf) EOY Status Entered", Percentage.Type}, {"# of CM Students#(lf) Currently Exited", Int64.Type}}),
  #"Renamed columns 2" = Table.RenameColumns(#"Changed column type 1", {{"#_w_goal_achieve", "%_w_goal_achieve"}}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Renamed columns 2", {{"%_w_goal_achieve", Percentage.Type}, {"# of CM Students with#(lf) EOY Status Entered", Int64.Type}, {"num_w_goal_achieve", Int64.Type}, {"%_w_1+_progress_entry", Percentage.Type}, {"num_w_1+_progress_entry", Int64.Type}, {"num_w_>1_formal_checkin", Int64.Type}, {"num_w_basic_needs_entered", Int64.Type}, {"num_tracked_outside_consent_dates", Int64.Type}, {"num_of_enrolled_with_outdated_consent", Int64.Type}, {"saturation_rate", Percentage.Type}, {"num_w_tier2&3_w_support_plan", Int64.Type}, {"num_of_students_w_tier1_2", Int64.Type}, {"%of_students_w_support_plan", Percentage.Type}, {"num_of_students_with_support_plan", Int64.Type}, {"total_case_managed_students", Int64.Type}, {"num_of_grading_periods", Int64.Type}, {"total_school_enrollment", Int64.Type}, {"school_status", type text}, {"school_type", type text}, {"district", type text}, {"school_model", type text}, {"nces_id", type text}, {"site_id", type text}, {"school_name", type text}, {"org", type text}}),
  #"Renamed columns 3" = Table.RenameColumns(#"Changed column type 2", {{"# of CM Students with#(lf) EOY Status Entered", "num_w_eoy_entered"}, {"% of CM Students with#(lf) EOY Status Entered", "%_w_eoy_entered"}, {"# of CM Students#(lf) Currently Exited", "num_students_exited"}})
in
  #"Renamed columns 3";
shared raw_cisdm_accred_case_manage_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_accred_case_manage", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "raw_cisdm_accred_site_coord_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared raw_cisdm_accred_site_coord = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Accreditation"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Drill down" = #"Filtered rows"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Drill down", null, true),
  #"Drill down 2" = #"Imported Excel workbook"{2}[Data],
  #"Removed top rows" = Table.Skip(#"Drill down 2", 1),
  #"Removed bottom rows" = Table.RemoveLastN(#"Removed top rows", 1),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed bottom rows", [PromoteAllScalars = true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted headers", {{"Organization", "org"}, {"School", "school_name"}, {"Site ID", "site_id"}, {"NCES ID", "nces_id"}, {"School Model", "school_model"}, {"School District", "district"}, {"School Type", "school_type"}, {"School Status", "school_status"}, {"Signed School #(lf)Agreement?", "signed_agreement"}, {"Total School Staff Assigned#(lf) Hours at School", "total_staff_at_school"}, {"School Needs Assessment Complete", "needs_assess_complete"}, {"School Support #(lf)Plan Complete", "support_plan_complete"}, {"Total School Enrollment", "total_enrollment"}, {"# of Grading Periods", "num_of_grading_periods"}, {"# of Times Reporting #(lf)to School Support #(lf)Team Documented", "num_of_times_reporting_school_support"}, {"# of Times Reporting #(lf)to School Leadership#(lf) Documented", "num_of_time_leadership"}, {"# of Times Reporting #(lf)to Organization Documented", "num_of_times_organization"}, {"# of Times Adjustments #(lf)Made to School #(lf)Support Plan", "num_adjustments_to_plan"}, {"# of School #(lf)Goals (Metrics)", "num_school_goals"}, {"# of School #(lf)Progress Entries", "num_progress_entries"}, {"# of Progress Entries #(lf)expected for the year", "num_expected_progress_entries"}, {"# of Goal #(lf)Achievement Entries #(lf)(across all metrics)", "num_goal_achievement"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"num_goal_achievement", Int64.Type}, {"num_expected_progress_entries", Int64.Type}, {"num_progress_entries", Int64.Type}, {"num_school_goals", Int64.Type}, {"num_adjustments_to_plan", Int64.Type}, {"num_of_times_organization", Int64.Type}, {"num_of_time_leadership", Int64.Type}, {"num_of_times_reporting_school_support", Int64.Type}, {"num_of_grading_periods", Int64.Type}, {"total_enrollment", Int64.Type}, {"support_plan_complete", type text}, {"needs_assess_complete", type text}, {"total_staff_at_school", type number}, {"signed_agreement", type text}, {"school_status", type text}, {"school_type", type text}, {"district", type text}, {"school_model", type text}, {"nces_id", type text}, {"site_id", type text}, {"school_name", type text}, {"org", type text}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type", each ([school_status] = "Active")),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows 1", "support_good", each if [num_of_times_reporting_school_support] >= 4 then 1 else ""),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "leadership_good", each if [num_of_time_leadership] >= 4 then 1 else ""),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Inserted conditional column 1", "counter", each 1), {{"counter", Int64.Type}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Added custom", {{"leadership_good", Int64.Type}, {"support_good", Int64.Type}})
in
  #"Changed column type 1";
shared raw_cisdm_accred_site_coord_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_accred_site_coord", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "first_9weeks_tier1_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared first_9weeks_tier1 = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Accreditation"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each ([Name] = "10.27.2025 Accreditation.xlsx")),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Invoke custom function" = Table.AddColumn(#"Filtered hidden files", "Transform file (3)", each #"Transform file (3)"([Content])),
  #"Removed other columns" = Table.SelectColumns(#"Invoke custom function", {"Transform file (3)"}),
  #"Expanded table column" = Table.ExpandTableColumn(#"Removed other columns", "Transform file (3)", Table.ColumnNames(#"Transform file (3)"(#"Sample file (3)"))),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded table column", {{"Column1", type text}, {"Column2", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 1),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Removed bottom rows" = Table.RemoveLastN(#"Promoted headers", 2),
  #"Filtered rows 1" = Table.SelectRows(#"Removed bottom rows", each ([#"School Model "] = "CIS Model")),
  #"Replaced value" = Table.ReplaceValue(#"Filtered rows 1", "-", 0, Replacer.ReplaceValue, {"Tier I - #(lf)Avg Served"}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Replaced value", {{"Basic Needs - #(lf)Total Students Served (duplicated)", Int64.Type}, {"# of Basic Needs #(lf)Entries (whole-school)", Int64.Type}, {"Total School Enrollment", Int64.Type}, {"Basic Needs- #(lf)Avg Served", type number}, {"Basic Needs - #(lf)Min Served #(lf)(by any one resource)", type number}, {"Basic Needs - #(lf)Max Served #(lf)(by any one resource)", type number}, {"# of Tier I #(lf)Supports", type number}, {"Tier I - #(lf)Total Students Served#(lf) (duplicated)", type number}, {"Tier I - #(lf)Avg Served", type number}, {"Tier I - #(lf)Min Served #(lf)(at any one support)", type number}, {"Tier I - #(lf)Max Served #(lf)(at any one support)", type number}, {"# of Tier I and #(lf)Basic Needs/Resources#(lf) combined", type number}, {"Site ID", Int64.Type}})
in
  #"Changed column type 1";
shared #"Sample file (3)" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "Accreditation"]}[Content],
  #"Expanded Attributes" = Table.ExpandRecordColumn(#"Navigation 4", "Attributes", {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}, {"Content Type", "Kind", "Size", "Group", "Owner", "Permissions"}),
  #"Filtered rows" = Table.SelectRows(#"Expanded Attributes", each [Name] = "10.27.2025 Accreditation.xlsx"),
  #"Filtered hidden files" = Table.SelectRows(#"Filtered rows", each [Attributes]?[Hidden]? <> true),
  #"Navigation 5" = #"Filtered hidden files"{0}[Content]
in
  #"Navigation 5";
shared #"Parameter (3)" = let
  #"Parameter (3)" = #"Sample file (3)" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type binary, BinaryIdentifier = #"Sample file (3)"]
in
  #"Parameter (3)";
shared #"Transform Sample file (3)" = let
  Source = Excel.Workbook(#"Parameter (3)", null, true),
  Navigation = Source{[Item = "Accreditation Tier I", Kind = "Sheet"]}[Data]
in
  Navigation;
[FunctionQueryBinding = "{""exemplarFormulaName"":""Transform Sample file (3)""}"]
shared #"Transform file (3)" = (#"Parameter (3)" as binary) => let
  Source = Excel.Workbook(#"Parameter (3)", null, true),
  Navigation = Source{[Item = "Accreditation Tier I", Kind = "Sheet"]}[Data]
in
  Navigation;
shared first_9weeks_tier1_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "first_9weeks_tier1", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
