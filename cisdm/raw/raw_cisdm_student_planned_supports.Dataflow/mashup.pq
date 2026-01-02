[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "planned_supports_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared planned_supports = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "planned_supports"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Drill down" = #"Filtered rows"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Drill down", null, true),
  #"Drill down 1" = #"Imported Excel workbook"{0}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Drill down 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Organization", type text}, {"School", type text}, {"Student ID", type text}, {"Client ID", type text}, {"Student Name", type text}, {"Sex/Gender", type text}, {"Race/Ethnicity", type text}, {"Ethnicity", type text}, {"Grade Level", type text}, {"Enrollment Begin Date", type date}, {"Enrollment End Date", type date}, {"Case Manager", type text}, {"Case Managed Status", type text}, {"CM Intensity Level", type text}, {"Support Category", type text}, {"Support Type", type text}, {"Activity", type text}, {"Tier", type text}, {"Goals Addressed", type text}, {"Frequency of Support", type text}, {"Other Frequency", type text}, {"Support Received? ", type text}, {"Total Supports", Int64.Type}, {"Total Support Hours", type number}, {"Initiatives", type text}, {"Month", type text}, {"Year", Int64.Type}, {"Created By", type text}, {"Created Date", type date}, {"Last Modified By", type text}, {"Last Modified Date", type date}, {"Enrollment ID", type text}, {"School Year", type text}}),
  #"Renamed columns" = Table.RenameColumns(#"Changed column type", {{"Organization", "org"}, {"School", "school_name"}, {"Student ID", "sasid"}, {"Client ID", "client_id"}, {"Student Name", "client_full_name"}, {"Sex/Gender", "gender"}, {"Race/Ethnicity", "race"}, {"Ethnicity", "ethnicity"}, {"Tribal Affiliation", "tribal_affiliation"}, {"Additional Tribal Affiliation(s)", "addtl_tribal_affltn"}, {"Grade Level", "grade"}, {"Enrollment Begin Date", "entry_date"}, {"Enrollment End Date", "exit_date"}, {"Case Manager", "case_manager"}, {"Case Managed Status", "case_managed_status"}, {"CM Intensity Level", "intensity_level"}, {"Support Category", "support_category"}, {"Support Type", "support_type"}, {"Activity", "activity"}, {"Tier", "tier"}, {"Goals Addressed", "goals_addressed"}, {"Frequency of Support", "freq_of_support"}, {"Other Frequency", "other_freq"}, {"Support Received? ", "support_received"}, {"Total Supports", "total_supports"}, {"Total Support Hours", "total_support_hours"}, {"Initiatives", "initiatives"}, {"Month", "month"}, {"Year", "year"}, {"Created By", "created_by"}, {"Created Date", "created_date"}, {"Last Modified By", "last_mod_by"}, {"Last Modified Date", "last_mod_date"}, {"Enrollment ID", "entry_id"}, {"School Year", "school_year"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"tribal_affiliation", type text}, {"addtl_tribal_affltn", type text}})
in
  #"Changed column type 1";
shared planned_supports_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_cisdm_student_planned_supports", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
