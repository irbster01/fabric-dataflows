[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "student_support_detail_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared student_support_detail = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "2526 CIS"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "student_support_detail"]}[Content],
  #"Filtered rows" = Table.SelectRows(#"Navigation 4", let latest = List.Max(#"Navigation 4"[Date modified]) in each [Date modified] = latest),
  #"Drill down" = #"Filtered rows"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(#"Drill down", null, true),
  #"Drill down 1" = #"Imported Excel workbook"{0}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Drill down 1", {{"Column1", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column9", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}, {"Column21", type text}, {"Column22", type text}, {"Column23", type text}, {"Column26", type text}, {"Column29", type text}, {"Column30", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 1),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Renamed columns" = Table.RenameColumns(#"Promoted headers", {{"Home School", "school_name"}, {"Student ID", "sasid"}, {"Student", "client_full_name"}, {"Case Management Intensity Level", "intensity_level"}, {"Grade Level", "grade"}, {"Case Manager", "case_manager"}, {"School Where #(lf)Support Was Provided", "provided_by"}, {"Entry Date", "entry_date"}, {"Entered By", "username"}, {"Unique#(lf)Support ID", "support_id"}, {"Support Date", "service_date"}, {"Service ID", "service_id"}, {"Provider Type 1", "user_type"}, {"Provider Name 1", "provider_name"}}),
  #"Removed columns 2" = Table.RemoveColumns(#"Renamed columns", {"Provider Type 2", "Provider Name 2", "Provider Type 3", "Provider Name 3"}),
  #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 2", {{"Individual or Group", "individual_group"}, {"Student Support Category", "service_category"}, {"Student Support Name", "service_type"}, {"Activity", "service_activity"}, {"Tier", "service_tier"}, {"Hours", "hours"}}),
  #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 3", {"Total Value of Time"}),
  #"Renamed columns 4" = Table.RenameColumns(#"Removed columns 3", {{"Donation Type", "donation_type"}, {"Donation Value", "donation_value"}, {"# Of Volunteers", "num_of_volunteers"}, {"Notes", "note_body"}, {"Initiatives", "initiatives"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns 4", {{"sasid", type text}, {"entry_date", type datetime}, {"support_id", type text}, {"service_date", type date}, {"service_id", type text}, {"hours", type number}, {"donation_value", type number}, {"num_of_volunteers", Int64.Type}}),
  #"Inserted start of month" = Table.AddColumn(#"Changed column type 1", "Start of month", each Date.StartOfMonth([service_date]), type nullable date),
  #"Renamed columns 2" = Table.RenameColumns(#"Inserted start of month", {{"Start of month", "month"}}),
  #"Filtered rows 1" = Table.SelectRows(#"Renamed columns 2", each ([school_name] <> null and [school_name] <> "")),
  #"Inserted conditional column" = Table.AddColumn(#"Filtered rows 1", "school_goal1", each if Text.Contains([school_name], "Atkins") then "Improve Attendance" else if Text.Contains([school_name], "Washington") then "Improve Attendance" else if Text.Contains([school_name], "Clark") then "Improve Attendance" else if Text.Contains([school_name], "Bethune") then "Improve Attendance" else if Text.Contains([school_name], "Broadmoor") then "Improve Attendance" else if Text.Contains([school_name], "Heights") then "Improve Attendance" else if Text.Contains([school_name], "Midway") then "Improve Attendance" else if Text.Contains([school_name], "Caddo Middle") then "Improve Attendance" else if Text.Contains([school_name], "Woodlawn") then "Improve Attendance" else if Text.Contains([school_name], "Westwood") then "Improve Attendance" else if Text.Contains([school_name], "Creswell") then "Improve Attendance" else ""),
  #"Inserted conditional column 1" = Table.AddColumn(#"Inserted conditional column", "school_goals2", each if Text.Contains([school_name], "Atkins") then "Improve Academics" else if Text.Contains([school_name], "Bethune") then "Improve Academics" else if Text.Contains([school_name], "Booker") then "Improve School Behavior" else if Text.Contains([school_name], "Broadmoor") then "Improve School Behavior" else if Text.Contains([school_name], "Heights") then "Improve Academics" else if Text.Contains([school_name], "Caddo Middle") then "Improve School Behavior" else if Text.Contains([school_name], "Creswell") then "Improve Academics" else if Text.Contains([school_name], "Clark") then "Improve Academics" else if Text.Contains([school_name], "Midway") then "Improve Academics" else if Text.Contains([school_name], "Westwood") then "Improve Academics" else if Text.Contains([school_name], "Woodlawn") then "Improve School Behavior" else ""),
  #"Inserted conditional column 2" = Table.AddColumn(#"Inserted conditional column 1", "service_support_category", each if Text.Contains([service_category], "Behavioral") then "Improve School Behavior" else if Text.Contains([service_category], "Academic") then "Improve Academics" else if Text.Contains([service_category], "Case Management") then "Case Management" else if Text.Contains([service_category], "Attendance") then "Improve Attendance" else if Text.Contains([service_category], "Family Engagement") then "Family Engagement" else if Text.Contains([service_category], "Mentoring") then "Improve School Behavior" else if Text.Contains([service_category], "Social Skills") then "Improve School Behavior" else if Text.Contains([service_category], "Enrichment") then "Enrichment" else if Text.Contains([service_category], "Physical") then "Health" else if Text.Contains([service_category], "College") then "Improve Academics" else ""),
  #"Inserted conditional column 3" = Table.AddColumn(#"Inserted conditional column 2", "Custom", each if [school_goal1] = [service_support_category] then 1 else ""),
  #"Inserted conditional column 4" = Table.AddColumn(#"Inserted conditional column 3", "Custom (2)", each if [service_support_category] = [school_goals2] then 1 else ""),
  #"Merged columns" = Table.CombineColumns(Table.TransformColumnTypes(#"Inserted conditional column 4", {{"Custom", type text}, {"Custom (2)", type text}}), {"Custom (2)", "Custom"}, Combiner.CombineTextByDelimiter("", QuoteStyle.None), "goal match"),
  #"Inserted conditional column 5" = Table.AddColumn(#"Merged columns", "matching_school_goal", each if [goal match] = "1" then "Yes" else "No"),
  #"Changed column type 3" = Table.TransformColumnTypes(#"Inserted conditional column 5", {{"matching_school_goal", type text}, {"service_support_category", type text}, {"school_goals2", type text}, {"school_goal1", type text}})
in
  #"Changed column type 3";
shared student_support_detail_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "4241c9c7-5818-4c56-9220-faf632741770"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "1b643108-97a2-432c-a0c0-391df26e0eea"]}[Data],
  TableNavigation = Navigation_2{[Id = "raw_student_support_detail", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
